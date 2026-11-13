#!/usr/bin/env python3
"""Publish a validated ETL version to Azure; manifest last, no blob overwrites.

Uses the existing Azure CLI login and Python standard library only. The cloud
completion marker is output_manifest.json. Readers must verify its file hashes.
"""
import argparse
import fcntl
import hashlib
import json
import resource
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
ACCOUNT = "amazonlake60306539"
CONTAINER = "electricity-project"
ASSETS = {"building_metadata.parquet", "catalog.json", "electricity.parquet", "weather_train.parquet"}
MARKER = "output_manifest.json"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def validate_local(directory, root):
    manifest_path = directory / MARKER
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(manifest.get("gates_passed") is True, "ETL manifest does not show passed quality gates.")
    require(manifest.get("source_version") == "ashrae-v1", "Unexpected source version.")
    identity = manifest["identity"]
    version_hash = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    require(directory.name == "etl-v1-" + version_hash[:16], "ETL directory name does not match its identity.")
    references = {
        "source_manifest_sha256": root / "evidence/source_manifest_v1.json",
        "config_sha256": root / "config/etl_v1.json",
        "etl_code_sha256": root / "scripts/run_etl.py",
    }
    for key, path in references.items():
        require(sha256(path) == identity[key], f"ETL provenance mismatch: {path.name}")
    source = json.loads(references["source_manifest_sha256"].read_text())
    require(manifest["raw_sources"] == source["files"], "Raw source lineage differs from the preserved source manifest.")
    config = json.loads(references["config_sha256"].read_text())
    require(manifest["config"] == config, "Manifest configuration differs from the preserved configuration.")
    require(identity["duckdb_version"] == config["required_duckdb_version"], "ETL DuckDB version mismatch.")
    for table in ("metadata_checked", "electricity_checked", "weather_checked"):
        counts = manifest["row_accounting"][table]
        values = [counts[k] for k in ("input_rows", "accepted_rows", "excluded_rows", "rejected_rows")]
        require(all(type(v) is int and v >= 0 for v in values), f"Invalid row counts: {table}")
        require(counts["rejected_rows"] == 0 and counts["accepted_rows"] > 0,
                f"Rejected rows or empty accepted table: {table}")
        require(values[0] == sum(values[1:]), f"Row accounting does not balance: {table}")
    records = manifest["files"]
    require(len(records) == len(ASSETS) and {r["file_name"] for r in records} == ASSETS,
            "ETL manifest must identify exactly the four expected assets.")
    require({p.name for p in directory.iterdir()} == ASSETS | {MARKER}, "Unexpected or missing local output files.")
    for record in records:
        path = directory / record["file_name"]
        require(path.is_file() and not path.is_symlink(), f"Invalid local file: {path.name}")
        require(path.stat().st_size == record["size_bytes"] and sha256(path) == record["sha256"],
                f"Local output hash/size mismatch: {path.name}")
    catalog = json.loads((directory / "catalog.json").read_text())
    for table, check in (("building_metadata", "metadata_checked"),
                         ("electricity", "electricity_checked"), ("weather_train", "weather_checked")):
        require(catalog["tables"][table]["row_count"] == manifest["row_accounting"][check]["accepted_rows"],
                f"Catalog row count mismatch: {table}")
    return manifest, sorted(records, key=lambda r: r["file_name"]) + [{
        "file_name": MARKER, "size_bytes": manifest_path.stat().st_size, "sha256": sha256(manifest_path)}]


class AzureStore:
    """All commands explicitly use Entra login, never account keys or SAS tokens."""
    def call(self, operation, *arguments):
        command = ["az", "storage", "blob", operation,
                   "--account-name", ACCOUNT, "--container-name", CONTAINER,
                   "--auth-mode", "login", "--only-show-errors", "--output", "json", *arguments]
        completed = subprocess.run(command, capture_output=True, text=True)
        if completed.returncode:
            raise RuntimeError(f"Azure blob {operation} failed: {completed.stderr.strip()}")
        return json.loads(completed.stdout) if completed.stdout.strip() else None

    def exists(self, name):
        return self.call("exists", "--name", name)["exists"]

    def show(self, name):
        blob = self.call("show", "--name", name)
        return {"etag": blob["properties"]["etag"], "size_bytes": blob["properties"]["contentLength"],
                "sha256": (blob.get("metadata") or {}).get("sha256")}

    def upload(self, name, path, digest):
        self.call("upload", "--name", name, "--file", str(path), "--type", "block",
                  "--overwrite", "false", "--if-none-match", "*", "--validate-content",
                  "--metadata", "sha256=" + digest, "--no-progress")

    def download(self, name, destination, etag):
        self.call("download", "--name", name, "--file", str(destination), "--if-match", etag,
                  "--validate-content", "--no-progress")


def verify_remote(store, name, record, temp, baseline=None):
    properties = store.show(name)
    require(properties["size_bytes"] == record["size_bytes"] and properties["sha256"] == record["sha256"],
            f"Remote metadata/size mismatch: {name}")
    if baseline is not None:
        require(properties["etag"] == baseline["etag"], f"Remote ETag changed: {name}")
    # Metadata alone is not proof of remote content: download and hash actual bytes.
    destination = temp / "verify.bin"
    if destination.exists():
        destination.unlink()
    store.download(name, destination, properties["etag"])
    require(destination.stat().st_size == record["size_bytes"] and sha256(destination) == record["sha256"],
            f"Downloaded content hash mismatch: {name}")
    require(store.show(name) == properties, f"Remote blob changed during verification: {name}")
    destination.unlink()
    return properties


def publish(directory, root, evidence, store, report):
    manifest, records = validate_local(directory, root)
    report.update(local_validation="passed", etl_identity=manifest["identity"],
                  etl_directory=str(directory), files=[])
    prefix = "processed/" + directory.name + "/"
    report["destination"] = f"https://{ACCOUNT}.blob.core.windows.net/{CONTAINER}/{prefix}"
    marker_digest = records[-1]["sha256"]
    baseline_path = evidence / f"processed_{directory.name}_etags.json"
    baseline = json.loads(baseline_path.read_text()) if baseline_path.exists() else None
    if baseline:
        require(baseline["destination"] == report["destination"] and baseline["manifest_sha256"] == marker_digest,
                "Preserved Azure baseline does not match this ETL version.")
        require(set(baseline["files"]) == ASSETS | {MARKER}, "Preserved baseline has unexpected files.")
    committed = store.exists(prefix + MARKER)
    require(not baseline or committed, "Previously published completion manifest is missing; refusing repair.")
    verified = {}
    with tempfile.TemporaryDirectory(prefix="electricity-azure-verify-") as temporary:
        temp = Path(temporary)
        # Preflight ALL existing blobs before writing any missing ones.
        # An existing marker commits the set: missing files then indicate damage.
        ordered = [records[-1], *records[:-1]] if committed else records[:-1]
        for record in ordered:
            name = record["file_name"]
            exists = store.exists(prefix + name)
            require(not committed or exists, f"Committed version is missing {name}; refusing repair.")
            if exists:
                verified[name] = verify_remote(store, prefix + name, record, temp,
                                              baseline["files"][name] if baseline else None)
                report["files"].append({**record, "action": "skipped", "etag": verified[name]["etag"]})
                print(f"SKIPPED: {name} | downloaded SHA-256 verified", flush=True)
        if not committed:
            for record in records[:-1]:
                name = record["file_name"]
                if name in verified:
                    continue
                print(f"UPLOADING: {name}", flush=True)
                store.upload(prefix + name, directory / name, record["sha256"])
                verified[name] = verify_remote(store, prefix + name, record, temp)
                report["files"].append({**record, "action": "uploaded", "etag": verified[name]["etag"]})
                print(f"UPLOADED: {name} | downloaded SHA-256 verified", flush=True)
            for name, properties in verified.items():
                require(store.show(prefix + name) == properties, f"Remote blob changed before commit: {name}")
            require(sha256(directory / MARKER) == marker_digest, "Local manifest changed before publication.")
            print(f"UPLOADING completion manifest: {MARKER}", flush=True)
            store.upload(prefix + MARKER, directory / MARKER, marker_digest)
            verified[MARKER] = verify_remote(store, prefix + MARKER, records[-1], temp)
            report["files"].append({**records[-1], "action": "uploaded", "etag": verified[MARKER]["etag"]})
        for name, properties in verified.items():
            require(store.show(prefix + name) == properties, f"Remote blob changed before final check: {name}")
    if baseline is None:
        write_json(baseline_path, {"destination": report["destination"], "manifest_sha256": marker_digest,
                                   "files": verified})
    report.update(status="passed", baseline_path=str(baseline_path),
                  uploaded=sum(f["action"] == "uploaded" for f in report["files"]),
                  skipped=sum(f["action"] == "skipped" for f in report["files"]),
                  completion_manifest_verified=True,
                  downloaded_verification_bytes=sum(r["size_bytes"] for r in records))
    print(f"PASS: uploaded={report['uploaded']}, skipped={report['skipped']}; completion manifest verified.", flush=True)
    print(f"Destination: {report['destination']}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--etl-dir", type=Path, required=True,
                        help="Exact successful local version directory, not the processed parent.")
    args = parser.parse_args()
    directory = args.etl_dir.expanduser().resolve()
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report = {"run_id": run_id, "status": "running", "files": [],
              "publisher_code_sha256": sha256(__file__)}
    report_path = evidence / f"publish_processed_{run_id}.json"
    started = perf_counter()
    lock = None
    code = 1
    try:
        require(directory.is_dir(), "ETL directory does not exist.")
        lock = (directory.parent / ".processed-publish.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        publish(directory, ROOT, evidence, AzureStore(), report)
        code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print(f"FAILED: {error}", file=sys.stderr, flush=True)
    finally:
        if lock:
            lock.close()
        report["elapsed_seconds"] = round(perf_counter() - started, 3)
        report["peak_process_rss_mib"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        report["peak_child_rss_mib"] = round(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024, 2)
        write_json(report_path, report)
        print(f"Evidence: {report_path}", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
