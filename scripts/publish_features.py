#!/usr/bin/env python3
"""Publish one verified feature version. Feature manifest is uploaded last."""
import argparse
import fcntl
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from publish_processed import ROOT, ACCOUNT, CONTAINER, AzureStore, verify_remote, require, sha256, validate_local
from build_features import FEATURE_COLUMNS, OUTPUT_FILES, write_json

MARKER = "feature_manifest.json"


def validate_features(directory, processed, root):
    processed_manifest, processed_files = validate_local(processed, root)
    manifest = json.loads((directory / MARKER).read_text())
    require(manifest.get("gates_passed") is True, "Feature quality gates did not pass.")
    config_path = root / "config/features_v1.json"
    config = json.loads(config_path.read_text())
    identity = {"processed_manifest_sha256": sha256(processed / "output_manifest.json"),
                "feature_config_sha256": sha256(config_path),
                "feature_code_sha256": sha256(root / "scripts/build_features.py"),
                "validation_helper_sha256": sha256(root / "scripts/publish_processed.py"),
                "duckdb_version": config["required_duckdb_version"]}
    require(manifest["identity"] == identity, "Feature provenance mismatch.")
    require(manifest["config"] == config, "Feature configuration mismatch.")
    require(manifest["processed_identity"] == processed_manifest["identity"]
            and manifest["processed_files"] == processed_files, "Processed lineage mismatch.")
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    require(directory.name == "features-v1-" + digest[:16], "Feature version name mismatch.")
    quality = manifest["quality"]
    expected_rows = processed_manifest["row_accounting"]["electricity_checked"]["accepted_rows"]
    require(quality["input_rows"] == quality["output_rows"] == expected_rows,
            "Feature row accounting mismatch.")
    require(all(quality[k] == 0 for k in ("dropped_rows", "duplicate_key_groups", "invalid_feature_rows")),
            "Feature quality counts are not zero.")
    summaries = quality["split_summary"]
    require(len(summaries) == 3 and {s["split"] for s in summaries} == {"train", "validation", "test"},
            "Feature partitions are incomplete.")
    require(sum(s["rows"] for s in summaries) == expected_rows, "Split row counts do not balance.")
    require(manifest["feature_columns"] == FEATURE_COLUMNS, "Predictor allowlist mismatch.")
    records = manifest["files"]
    require(len(records) == 4 and {r["file_name"] for r in records} == OUTPUT_FILES, "Unexpected feature files.")
    require({p.name for p in directory.iterdir()} == OUTPUT_FILES | {MARKER}, "Unexpected local feature directory contents.")
    for record in records:
        path = directory / record["file_name"]
        require(path.is_file() and not path.is_symlink(), "Invalid feature asset: " + path.name)
        require(path.stat().st_size == record["size_bytes"] and sha256(path) == record["sha256"],
                "Local feature hash mismatch: " + path.name)
    return sorted(records, key=lambda r: r["file_name"]) + [{"file_name": MARKER,
        "size_bytes": (directory / MARKER).stat().st_size, "sha256": sha256(directory / MARKER)}]


def publish(directory, processed, root, store, report):
    records = validate_features(directory, processed, root)
    prefix = "curated/" + directory.name + "/"
    destination = f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/{prefix}"
    report.update(destination=destination, local_validation="passed", files=[])
    baseline_path = root / "evidence" / ("feature_" + directory.name + "_etags.json")
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else None
    if baseline:
        require(baseline["destination"] == destination and baseline["manifest_sha256"] == records[-1]["sha256"],
                "Feature baseline identity mismatch.")
        require(set(baseline["files"]) == OUTPUT_FILES | {MARKER}, "Incomplete feature ETag baseline.")
    committed = store.exists(prefix + MARKER)
    require(not baseline or committed, "Previously published feature manifest is missing; refusing repair.")
    verified = {}
    with tempfile.TemporaryDirectory(prefix="feature-upload-verify-") as temporary:
        temp = Path(temporary)
        # Verify every pre-existing object before any write.
        ordered = [records[-1], *records[:-1]] if committed else records[:-1]
        for record in ordered:
            name = record["file_name"]
            exists = store.exists(prefix + name)
            require(not committed or exists, "Committed feature asset missing: " + name)
            if exists:
                verified[name] = verify_remote(store, prefix + name, record, temp,
                                              baseline["files"][name] if baseline else None)
                report["files"].append({**record, "action": "skipped", "etag": verified[name]["etag"]})
                print("SKIPPED: " + name + " | downloaded SHA-256 verified", flush=True)
        if not committed:
            for record in records[:-1]:
                name = record["file_name"]
                if name in verified:
                    continue
                print("UPLOADING: " + name, flush=True)
                store.upload(prefix + name, directory / name, record["sha256"])
                verified[name] = verify_remote(store, prefix + name, record, temp)
                report["files"].append({**record, "action": "uploaded", "etag": verified[name]["etag"]})
                print("UPLOADED: " + name + " | downloaded SHA-256 verified", flush=True)
            for name, properties in verified.items():
                require(store.show(prefix + name) == properties, "Feature asset changed before commit: " + name)
            require(sha256(directory / MARKER) == records[-1]["sha256"], "Local feature manifest changed.")
            print("UPLOADING completion manifest: " + MARKER, flush=True)
            store.upload(prefix + MARKER, directory / MARKER, records[-1]["sha256"])
            verified[MARKER] = verify_remote(store, prefix + MARKER, records[-1], temp)
            report["files"].append({**records[-1], "action": "uploaded", "etag": verified[MARKER]["etag"]})
        for name, properties in verified.items():
            require(store.show(prefix + name) == properties, "Feature blob changed before final verification: " + name)
    if baseline is None:
        write_json(baseline_path, {"destination": destination, "manifest_sha256": records[-1]["sha256"], "files": verified})
    report.update(status="passed", completion_manifest_verified=True, baseline_path=str(baseline_path),
                  uploaded=sum(f["action"] == "uploaded" for f in report["files"]),
                  skipped=sum(f["action"] == "skipped" for f in report["files"]),
                  downloaded_verification_bytes=sum(r["size_bytes"] for r in records))
    print(f"PASS: uploaded={report['uploaded']}, skipped={report['skipped']}; feature manifest verified.", flush=True)
    print("Destination: " + destination, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-dir", type=Path, required=True)
    parser.add_argument("--etl-dir", type=Path, required=True)
    args = parser.parse_args()
    directory, processed = args.feature_dir.expanduser().resolve(), args.etl_dir.expanduser().resolve()
    report = {"status": "running", "publisher_sha256": sha256(__file__)}
    started = perf_counter()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    code = 1
    try:
        require(directory.is_dir(), "Feature version directory does not exist.")
        with (directory.parent / ".feature-publish.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            publish(directory, processed, ROOT, AzureStore(), report)
        code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print("FAILED: " + str(error), file=sys.stderr, flush=True)
    finally:
        report.update(run_id=run_id, elapsed_seconds=round(perf_counter()-started,3))
        path = evidence / f"publish_features_{run_id}.json"
        write_json(path, report)
        print("Evidence: " + str(path), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
