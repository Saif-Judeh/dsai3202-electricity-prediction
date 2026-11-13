#!/usr/bin/env python3
"""Run and record the complete Phase-1 workflow. Stop at the first failed stage."""
import argparse
import fcntl
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def execute_stage(root, run_id, label, script, arguments=(), pattern=None):
    evidence = root / "evidence"
    evidence.mkdir(exist_ok=True)
    before = set(evidence.glob(pattern)) if pattern else set()
    command = [sys.executable, str(root / "scripts" / script), *map(str, arguments)]
    log = evidence / f"pipeline_{run_id}_{label}.log"
    entry = {"stage": label, "command": command, "console_log": str(log), "status": "running"}
    started = perf_counter()
    print(f"\nSTART: {label}", flush=True)
    with log.open("x", encoding="utf-8") as stream:
        process = subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   text=True, bufsize=1)
        for line in process.stdout:
            stream.write(line)
            stream.flush()
            print(line, end="", flush=True)
        entry["exit_code"] = process.wait()
    entry["elapsed_seconds"] = round(perf_counter()-started,3)
    data = None
    if entry["exit_code"] != 0:
        entry.update(status="failed", error="Child process returned nonzero exit code")
    elif pattern:
        added = set(evidence.glob(pattern)) - before
        if len(added) != 1:
            entry.update(status="failed", error=f"Expected exactly one new {pattern} report; found {len(added)}")
        else:
            path = added.pop()
            entry["evidence_report"] = str(path)
            try:
                data = json.loads(path.read_text())
                if str(data.get("status", "")).lower() in {"failed", "failure", "error"}:
                    entry.update(status="failed", error="Stage report records failure despite zero exit code")
                else:
                    entry["status"] = "passed"
            except (OSError, ValueError) as error:
                entry.update(status="failed", error="Unreadable stage report: " + str(error))
    else:
        entry["status"] = "passed"
    print(f"STAGE {entry['status'].upper()}: {label}", flush=True)
    return entry, data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-tests", action="store_true", help="For a repeat run after the same code passed the full test run.")
    args = parser.parse_args()
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report = {"run_id": run_id, "status": "running", "tests_skipped": args.skip_tests, "stages": [],
              "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    started = perf_counter()
    code = 1
    def stage(label, script, arguments=(), pattern=None):
        entry, data = execute_stage(ROOT, run_id, label, script, arguments, pattern)
        report["stages"].append(entry)
        if entry["status"] != "passed":
            raise RuntimeError(f"Stage {label} failed; subsequent stages were not run.")
        return data
    try:
        # This project intentionally uses Linux/Azure VM execution.
        lock_directory = Path.home() / "electricity-data"
        lock_directory.mkdir(exist_ok=True)
        with (lock_directory / ".pipeline.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
            report["git_commit_at_start"] = git.stdout.strip() if git.returncode == 0 else None
            status = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True)
            report["git_dirty_at_start"] = bool(status.stdout.strip()) if status.returncode == 0 else None
            if not args.skip_tests:
                for label, script, pattern in [
                    ("runner_tests", "test_pipeline.py", "pipeline_test_*.json"),
                    ("etl_tests", "test_etl_quality.py", "quality_test_*.json"),
                    ("processed_publication_tests", "test_publish_processed.py", "publish_test_*.json"),
                    ("feature_tests", "test_features.py", "feature_test_*.json"),
                    ("feature_publication_tests", "test_feature_publication.py", "feature_publish_test_*.json")]:
                    stage(label, script, pattern=pattern)
            stage("source_manifest", "create_manifest.py")
            stage("raw_ingestion", "ingest_raw.py", pattern="ingestion_*.json")
            stage("full_profile", "profile_data.py", pattern="profile_*.json")
            etl = stage("etl", "run_etl.py", pattern="etl_*.json")
            processed = etl["output_directory"]
            stage("processed_publication", "publish_processed.py", ["--etl-dir", processed], "publish_processed_*.json")
            stage("training_eda", "summarize_eda.py", ["--etl-dir", processed], "eda_summary_*.json")
            features = stage("features", "build_features.py", ["--etl-dir", processed], "features_*.json")
            stage("feature_publication", "publish_features.py",
                  ["--feature-dir", features["output_directory"], "--etl-dir", processed], "publish_features_*.json")
            report.update(status="passed", processed_directory=processed, feature_directory=features["output_directory"])
        code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print("FAILED: " + str(error), file=sys.stderr, flush=True)
    finally:
        report["elapsed_seconds"] = round(perf_counter()-started,3)
        path = evidence / f"pipeline_{run_id}.json"
        with path.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
        print(f"\nPIPELINE {report['status'].upper()}: {len(report['stages'])} recorded stages", flush=True)
        print("Pipeline evidence: " + str(path), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
