#!/usr/bin/env python3
"""Offline publication tests: synthetic files and an in-memory Azure substitute."""
import contextlib
import hashlib
import io
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from publish_processed import ASSETS, MARKER, ROOT, publish, require, sha256, write_json


class MemoryStore:
    def __init__(self):
        self.blobs = {}
        self.uploads = []
        self.fail_upload = None
        self.calls = 0

    def exists(self, name):
        self.calls += 1
        return name in self.blobs

    def show(self, name):
        self.calls += 1
        blob = self.blobs[name]
        return {"etag": blob["etag"], "sha256": blob["sha256"], "size_bytes": len(blob["bytes"])}

    def upload(self, name, path, digest):
        self.calls += 1
        require(name != self.fail_upload, "Injected upload interruption")
        require(name not in self.blobs, "Test detected attempted overwrite")
        self.blobs[name] = {"etag": uuid4().hex, "sha256": digest, "bytes": path.read_bytes()}
        self.uploads.append(name)

    def download(self, name, destination, etag):
        self.calls += 1
        require(self.blobs[name]["etag"] == etag, "Test download ETag mismatch")
        destination.write_bytes(self.blobs[name]["bytes"])


def fixture(root):
    for folder in ("evidence", "config", "scripts"):
        (root / folder).mkdir(parents=True)
    source = {"source_version": "ashrae-v1", "files": [{"file_name": "synthetic-only", "sha256": "a" * 64}]}
    config = {"required_duckdb_version": "1.4.3"}
    write_json(root / "evidence/source_manifest_v1.json", source)
    write_json(root / "config/etl_v1.json", config)
    (root / "scripts/run_etl.py").write_text("# Synthetic provenance fixture only\n")
    identity = {"source_manifest_sha256": sha256(root / "evidence/source_manifest_v1.json"),
                "config_sha256": sha256(root / "config/etl_v1.json"),
                "etl_code_sha256": sha256(root / "scripts/run_etl.py"), "duckdb_version": "1.4.3"}
    version = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16]
    directory = root / "processed" / ("etl-v1-" + version)
    directory.mkdir(parents=True)
    for name in ASSETS - {"catalog.json"}:
        (directory / name).write_bytes(("synthetic upload bytes: " + name).encode())
    write_json(directory / "catalog.json", {"tables": {name: {"row_count": 1}
               for name in ("electricity", "building_metadata", "weather_train")}})
    records = [{"file_name": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)}
               for p in sorted(directory.iterdir())]
    counts = {"input_rows": 1, "accepted_rows": 1, "excluded_rows": 0, "rejected_rows": 0}
    write_json(directory / MARKER, {"gates_passed": True, "source_version": "ashrae-v1",
        "identity": identity, "config": config, "raw_sources": source["files"], "files": records,
        "row_accounting": {name: counts for name in ("metadata_checked", "electricity_checked", "weather_checked")}})
    return directory, "processed/" + directory.name + "/"


def run(root, directory, store):
    report = {}
    with contextlib.redirect_stdout(io.StringIO()):
        publish(directory, root, root / "evidence", store, report)
    return report


def expect_failure(root, directory, store, fragment):
    before = len(store.uploads)
    try:
        run(root, directory, store)
    except RuntimeError as error:
        require(fragment in str(error), f"Unexpected failure: {error}")
    else:
        raise AssertionError("Expected failure did not occur")
    require(len(store.uploads) == before, "Failed preflight wrote remote objects")


def main():
    result = {"status": "running", "scope": "offline synthetic files and in-memory Azure substitute; not live Azure",
              "publisher_code_sha256": sha256(ROOT / "scripts/publish_processed.py"),
              "test_code_sha256": sha256(__file__), "checks": []}
    def passed(name):
        result["checks"].append({"test": name, "status": "passed"})
        print("PASS: " + name)
    code = 1
    try:
        with tempfile.TemporaryDirectory(prefix="electricity-publish-tests-") as temporary:
            base = Path(temporary)
            root = base / "valid"
            directory, prefix = fixture(root)
            store = MemoryStore()
            report = run(root, directory, store)
            require(report["uploaded"] == 5 and report["skipped"] == 0, "Wrong first-run counts")
            require(store.uploads[-1] == prefix + MARKER, "Completion manifest was not uploaded last")
            passed("first_upload_and_manifest_last")
            before = {name: value.copy() for name, value in store.blobs.items()}
            report = run(root, directory, store)
            require(report["uploaded"] == 0 and report["skipped"] == 5 and store.blobs == before,
                    "Rerun modified remote data")
            passed("verified_rerun_without_writes")
            original = store.blobs[prefix + "electricity.parquet"].copy()
            store.blobs[prefix + "electricity.parquet"]["bytes"] = b"x" * len(original["bytes"])
            expect_failure(root, directory, store, "Downloaded content hash mismatch")
            store.blobs[prefix + "electricity.parquet"] = original.copy()
            passed("remote_bytes_checked_not_just_metadata")
            store.blobs[prefix + "electricity.parquet"]["etag"] = "changed"
            expect_failure(root, directory, store, "Remote ETag changed")
            store.blobs[prefix + "electricity.parquet"] = original.copy()
            passed("etag_change_blocks_rerun")
            del store.blobs[prefix + "electricity.parquet"]
            expect_failure(root, directory, store, "Committed version is missing")
            store.blobs[prefix + "electricity.parquet"] = original.copy()
            passed("missing_committed_asset_not_repaired")
            path = directory / "electricity.parquet"
            saved = path.read_bytes()
            path.write_bytes(b"changed")
            calls = store.calls
            expect_failure(root, directory, store, "Local output hash/size mismatch")
            require(store.calls == calls, "Invalid local data reached Azure")
            path.write_bytes(saved)
            passed("local_tampering_blocks_all_cloud_calls")
            marker_path = directory / MARKER
            saved_marker = marker_path.read_bytes()
            marker = json.loads(saved_marker)
            marker["gates_passed"] = False
            marker_path.write_text(json.dumps(marker))
            calls = store.calls
            expect_failure(root, directory, store, "passed quality gates")
            require(store.calls == calls, "Failed quality gate reached Azure")
            marker_path.write_bytes(saved_marker)
            passed("failed_quality_gate_blocks_all_cloud_calls")
            (root / "scripts/run_etl.py").write_text("# Changed code\n")
            calls = store.calls
            expect_failure(root, directory, store, "ETL provenance mismatch")
            require(store.calls == calls, "Changed provenance reached Azure")
            passed("provenance_mismatch_blocks_all_cloud_calls")
            root = base / "interrupted"
            directory, prefix = fixture(root)
            store = MemoryStore()
            store.fail_upload = prefix + "electricity.parquet"
            try:
                run(root, directory, store)
            except RuntimeError as error:
                require("Injected upload interruption" in str(error), str(error))
            else:
                raise AssertionError("Injected interruption did not fail")
            require(prefix + MARKER not in store.blobs, "Interrupted upload created completion marker")
            require(len(store.uploads) == 2, "Unexpected interruption stage")
            store.fail_upload = None
            report = run(root, directory, store)
            require(report["uploaded"] == 3 and report["skipped"] == 2, "Partial upload did not resume safely")
            passed("interruption_blocks_marker_and_resume_verifies_existing")
            root = base / "partial_conflict"
            directory, prefix = fixture(root)
            store = MemoryStore()
            path = directory / "electricity.parquet"
            store.upload(prefix + path.name, path, sha256(path))
            store.blobs[prefix + path.name]["bytes"] = b"x" * path.stat().st_size
            expect_failure(root, directory, store, "Downloaded content hash mismatch")
            require(prefix + MARKER not in store.blobs, "Conflicting partial upload was committed")
            passed("partial_conflict_detected_before_any_new_upload")
        result.update(status="passed", temporary_data_removed=True)
        code = 0
    except Exception as error:
        result.update(status="failed", error=str(error))
        print("FAIL: " + str(error), file=sys.stderr)
    finally:
        evidence = ROOT / "evidence"
        evidence.mkdir(exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        path = evidence / f"publish_test_{run_id}.json"
        write_json(path, result)
        print(f"{result['status'].upper()}: {len(result['checks'])} checks; evidence: {path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
