import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SOURCE = Path.home() / "electricity-data/incoming/ashrae-v1"
ACCOUNT = "amazonlake60306539"
CONTAINER = "electricity-project"
PREFIX = "raw/ashrae-v1/"

manifest_path = ROOT / "evidence/source_manifest_v1.json"
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
baseline_path = ROOT / "evidence/raw_v1_etags.json"
baseline = (
    json.loads(baseline_path.read_text(encoding="utf-8"))
    if baseline_path.exists() else None
)

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def blob(action, *args):
    command = [
        "az", "storage", "blob", action,
        "--account-name", ACCOUNT,
        "--container-name", CONTAINER,
        "--auth-mode", "login",
        "--only-show-errors", "--output", "json",
        *args,
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return json.loads(result.stdout or "{}")

started = perf_counter()
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
run_id += "-" + uuid4().hex[:8]
report = {
    "run_id": run_id,
    "account": ACCOUNT,
    "container": CONTAINER,
    "raw_prefix": PREFIX,
    "status": "running",
    "files": [],
}

try:
    items = []

    # Check every local source file before uploading anything.
    for record in manifest["files"]:
        path = SOURCE / record["file_name"]
        if path.stat().st_size != record["size_bytes"]:
            raise RuntimeError(f"Local size changed: {path.name}")
        if sha256(path) != record["sha256"]:
            raise RuntimeError(f"Local hash changed: {path.name}")
        items.append((path, record["sha256"]))

    # Publish the manifest after its source files.
    items.append((manifest_path, sha256(manifest_path)))
    current_etags = {}

    for path, digest in items:
        name = PREFIX + path.name
        exists = blob("exists", "--name", name)["exists"]

        if not exists:
            if baseline is not None:
                raise RuntimeError(f"Previously preserved blob is missing: {name}")

            print(f"UPLOADING: {path.name}", flush=True)
            blob(
                "upload",
                "--name", name,
                "--file", str(path),
                "--type", "block",
                "--overwrite", "false",
                "--if-none-match", "*",
                "--validate-content",
                "--metadata", f"sha256={digest}",
                "--no-progress",
            )
            action = "uploaded"
        else:
            action = "skipped"

        remote = blob("show", "--name", name)
        properties = remote["properties"]

        if properties["contentLength"] != path.stat().st_size:
            raise RuntimeError(f"Remote size mismatch: {name}")
        if (remote.get("metadata") or {}).get("sha256") != digest:
            raise RuntimeError(f"Remote SHA-256 metadata mismatch: {name}")

        etag = properties["etag"]
        if baseline is not None and baseline.get(name) != etag:
            raise RuntimeError(f"Preserved blob ETag changed: {name}")

        current_etags[name] = etag
        report["files"].append({
            "blob": name,
            "action": action,
            "size_bytes": path.stat().st_size,
            "sha256": digest,
            "etag": etag,
        })
        print(f"{action.upper()}: {path.name} | verified", flush=True)

    if baseline is None:
        with baseline_path.open("x", encoding="utf-8") as stream:
            json.dump(current_etags, stream, indent=2)

    report["status"] = "passed"

except Exception as error:
    report["status"] = "failed"
    report["error"] = str(error)
    raise SystemExit(f"FAILED: {error}")

finally:
    report["duration_seconds"] = round(perf_counter() - started, 2)
    report_path = ROOT / "evidence" / f"ingestion_{run_id}.json"
    report_path.write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(f"Evidence saved: {report_path}", flush=True)

uploaded = sum(item["action"] == "uploaded" for item in report["files"])
skipped = sum(item["action"] == "skipped" for item in report["files"])
print(f"PASS: uploaded={uploaded}, skipped={skipped}")
