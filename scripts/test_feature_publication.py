#!/usr/bin/env python3
"""Feature publication tests use real synthetic ETL/features and simulated Azure."""
import contextlib
import io
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from test_features import fixture, command
from test_publish_processed import MemoryStore
from publish_features import publish, MARKER, require, sha256, write_json

ROOT = Path(__file__).resolve().parents[1]


def main():
    report = {"status": "running", "scope": "synthetic inputs and in-memory Azure; no cloud calls",
              "publisher_sha256": sha256(ROOT / "scripts/publish_features.py"),
              "test_sha256": sha256(__file__), "checks": []}
    def passed(name):
        report["checks"].append({"test": name, "status": "passed"})
        print("PASS: " + name)
    code = 1
    try:
        with tempfile.TemporaryDirectory(prefix="feature-publication-tests-") as temp:
            root = Path(temp)
            fixture(root)
            for script, args in [("run_etl.py", ["--source-dir", root / "source", "--output-root", root / "processed"])]:
                result = command(root, script, *args)
                require(result.returncode == 0, result.stdout + result.stderr)
            processed = next((root / "processed").glob("etl-v1-*"))
            result = command(root, "build_features.py", "--etl-dir", processed, "--output-root", root / "features")
            require(result.returncode == 0, result.stdout + result.stderr)
            directory = next((root / "features").glob("features-v1-*"))
            prefix = "curated/" + directory.name + "/"
            store = MemoryStore()
            def run():
                result = {}
                with contextlib.redirect_stdout(io.StringIO()):
                    publish(directory, processed, root, store, result)
                return result
            def fails(fragment):
                before = len(store.uploads)
                try:
                    run()
                except RuntimeError as error:
                    require(fragment in str(error), "Unexpected error: " + str(error))
                else:
                    raise AssertionError("Expected publication failure")
                require(len(store.uploads) == before, "Failed preflight wrote objects")
            result = run()
            require(result["uploaded"] == 5 and store.uploads[-1] == prefix + MARKER, "Wrong upload count/order")
            passed("five_assets_and_manifest_last")
            result = run()
            require(result["uploaded"] == 0 and result["skipped"] == 5, "Rerun wrote blobs")
            passed("safe_verified_rerun")
            original = store.blobs[prefix + "train.parquet"].copy()
            store.blobs[prefix + "train.parquet"]["bytes"] = b"x" * len(original["bytes"])
            fails("Downloaded content hash mismatch")
            store.blobs[prefix + "train.parquet"] = original.copy()
            passed("changed_remote_bytes_blocked")
            store.blobs[prefix + "train.parquet"]["etag"] = "changed"
            fails("Remote ETag changed")
            store.blobs[prefix + "train.parquet"] = original.copy()
            passed("changed_remote_etag_blocked")
            del store.blobs[prefix + "train.parquet"]
            fails("Committed feature asset missing")
            store.blobs[prefix + "train.parquet"] = original.copy()
            passed("missing_committed_asset_not_repaired")
            local = directory / "train.parquet"
            saved = local.read_bytes()
            local.write_bytes(b"changed")
            calls = store.calls
            fails("Local feature hash mismatch")
            require(store.calls == calls, "Invalid local data reached Azure")
            local.write_bytes(saved)
            passed("changed_local_feature_blocks_cloud_calls")
            path = directory / MARKER
            saved = path.read_bytes()
            value = json.loads(saved)
            value["gates_passed"] = False
            path.write_text(json.dumps(value))
            calls = store.calls
            fails("Feature quality gates did not pass")
            require(store.calls == calls, "Failed gates reached Azure")
            path.write_bytes(saved)
            passed("failed_gates_block_cloud_calls")
            baseline = next((root / "evidence").glob("feature_*_etags.json"))
            baseline.unlink()  # Isolated synthetic baseline only; simulate first partial upload.
            store = MemoryStore()
            store.fail_upload = prefix + "train.parquet"
            try:
                run()
            except RuntimeError as error:
                require("Injected upload interruption" in str(error), str(error))
            else:
                raise AssertionError("Injected upload interruption did not occur")
            require(prefix + MARKER not in store.blobs, "Interrupted publication has completion manifest")
            store.fail_upload = None
            result = run()
            require(result["uploaded"] == 3 and result["skipped"] == 2, "Partial upload failed to resume")
            passed("partial_upload_resumes_without_premature_manifest")
        report.update(status="passed", temporary_data_removed=True)
        code = 0
    except Exception as error:
        report.update(status="failed", error=str(error))
        print("FAIL: " + str(error))
    finally:
        evidence = ROOT / "evidence"
        evidence.mkdir(exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        path = evidence / f"feature_publish_test_{run_id}.json"
        write_json(path, report)
        print(f"{report['status'].upper()}: {len(report['checks'])} checks; evidence: {path}")
    return code


if __name__ == "__main__":
    raise SystemExit(main())
