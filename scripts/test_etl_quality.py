#!/usr/bin/env python3
"""Exercise ETL safety using tiny isolated fixtures; never use real raw inputs."""
import csv
import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb
from run_etl import HEADERS, ROOT, sha256, write_json


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def fixture(directory, variant="good"):
    source = directory / "source"
    source.mkdir(parents=True)
    train = [[0, 0, "2016-01-01 00:00:00", 100],
             [0, 0, "2016-01-01 01:00:00", 0],
             [1, 0, "2016-01-01 00:00:00", 10],
             [0, 1, "2016-01-01 00:00:00", 50]]
    if variant == "negative":
        train.append([1, 0, "2016-01-01 01:00:00", -5])
    if variant == "duplicate":
        train.append([0, 0, "2016-01-01 00:00:00", 101])
    if variant == "fractional_id":
        train.append([0.5, 0, "2016-01-01 02:00:00", 5])
    metadata = [[0, 0, "Education", 1000, "", ""], [1, 1, "Office", 2000, 2000, 3]]
    weather = [[0, "2016-01-01 00:00:00", 20, "", 10, -1, 1010, 90, 2],
               [1, "2016-01-01 00:00:00", "", "", 9, "", 1011, 80, 3]]
    if variant == "invalid_weather":
        weather[0][2] = "NaN"
    if variant == "duplicate_metadata":
        metadata.append(metadata[0])
    rows = {
        "building_metadata.csv": metadata,
        "train.csv": train,
        "test.csv": [[0, 0, 0, "2017-01-01 00:00:00"]],
        "sample_submission.csv": [[0, 0]],
        "weather_train.csv": weather,
        "weather_test.csv": [[0, "2017-01-01 00:00:00", 20, "", 10, "", 1010, 90, 2]],
    }
    records = []
    for name, header in HEADERS.items():
        path = source / name
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(header.split(","))
            writer.writerows(rows[name])
        records.append({"file_name": name, "size_bytes": path.stat().st_size,
                        "sha256": sha256(path), "columns": header.split(",")})
    # The manifest intentionally matches each fixture, including bad-quality data.
    # This ensures controlled failures reach QUALITY gates, not just hash checks.
    manifest = directory / "fixture_manifest.json"
    write_json(manifest, {"source_version": "ashrae-v1", "files": records,
                          "purpose": "synthetic isolated quality test; not production source"})
    return source, manifest


def snapshot(directory):
    return {str(p.relative_to(directory)): (sha256(p), p.stat().st_mtime_ns)
            for p in directory.rglob("*") if p.is_file()}


def run(source, manifest, output, evidence):
    completed = subprocess.run([
        sys.executable, str(ROOT / "scripts/run_etl.py"), "--source-dir", str(source),
        "--manifest", str(manifest), "--config", str(ROOT / "config/etl_v1.json"),
        "--output-root", str(output), "--evidence-dir", str(evidence)
    ], capture_output=True, text=True)
    reports = sorted(evidence.glob("etl_*.json"), key=lambda p: p.stat().st_mtime_ns)
    require(bool(reports), "ETL did not write an evidence report: " + completed.stderr)
    report = json.loads(reports[-1].read_text())
    return completed, report


def main():
    started = perf_counter()
    result = {"status": "running", "scope": "isolated synthetic fixtures only",
              "etl_code_sha256": sha256(ROOT / "scripts/run_etl.py"),
              "test_code_sha256": sha256(__file__),
              "config_sha256": sha256(ROOT / "config/etl_v1.json"), "checks": []}
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    evidence_root = ROOT / "evidence"
    evidence_root.mkdir(exist_ok=True)
    report_path = evidence_root / f"quality_test_{run_id}.json"
    code = 1
    try:
        with tempfile.TemporaryDirectory(prefix="electricity-etl-quality-") as temporary:
            base = Path(temporary)
            output, evidence = base / "processed", base / "run_evidence"
            source, manifest = fixture(base / "good")
            raw_before = snapshot(source)
            completed, report = run(source, manifest, output, evidence)
            require(completed.returncode == 0 and report["action"] == "published_local",
                    "Valid fixture failed: " + completed.stdout + completed.stderr)
            final = Path(report["output_directory"])
            stats = report["row_accounting"]["electricity_checked"]
            require((stats["input_rows"], stats["excluded_rows"], stats["rejected_rows"], stats["accepted_rows"]) == (4, 1, 0, 3), "Incorrect row accounting")
            db = duckdb.connect()
            values = db.execute("SELECT building_id, meter_reading_original, meter_reading_kwh, original_unit, is_zero FROM read_parquet(?) ORDER BY building_id, timestamp_reported",
                                [str(final / "electricity.parquet")]).fetchall()
            require(abs(values[0][2] - 29.307107) < 1e-8 and values[0][3] == "kBTU", "Site 0 unit conversion incorrect")
            require(values[1][2] == 0 and values[1][4], "Zero reading was not retained and flagged")
            require(values[2][2] == 10 and values[2][3] == "kWh", "Other-site units changed")
            require(db.execute("SELECT year_built IS NULL AND floor_count IS NULL FROM read_parquet(?) WHERE building_id=0", [str(final / "building_metadata.parquet")]).fetchone()[0], "Optional missing metadata was not retained")
            require(db.execute("SELECT air_temperature IS NULL FROM read_parquet(?) WHERE site_id=1", [str(final / "weather_train.parquet")]).fetchone()[0], "Missing weather was not retained")
            db.close()
            published_before = snapshot(final)
            result["checks"].append({"test": "valid_fixture_conversion_zeros_nulls_accounting", "status": "passed", "etl_report": report})
            completed, report = run(source, manifest, output, evidence)
            require(completed.returncode == 0 and report["action"] == "skipped_verified", "Rerun was not verified and skipped")
            require(snapshot(final) == published_before, "Rerun changed published bytes or timestamps")
            require(snapshot(source) == raw_before, "Raw source changed")
            result["checks"].append({"test": "safe_rerun_and_raw_preservation", "status": "passed", "etl_report": report})
            for variant, table, reason in [
                ("negative", "electricity_checked", "invalid_reading"),
                ("duplicate", "electricity_checked", "duplicate_electricity_key"),
                ("fractional_id", "electricity_checked", "invalid_building_id"),
                ("invalid_weather", "weather_checked", "invalid_weather_number"),
                ("duplicate_metadata", "metadata_checked", "duplicate_building_id"),
            ]:
                bad_source, bad_manifest = fixture(base / variant, variant)
                before = snapshot(bad_source)
                completed, report = run(bad_source, bad_manifest, output, evidence)
                require(completed.returncode != 0 and report["error_type"] == "QualityFailure", f"{variant} did not fail quality gate")
                require(report["row_accounting"][table]["rejection_reasons"].get(reason, 0) > 0,
                        f"{variant} failed for the wrong reason")
                require(not Path(report["output_directory"]).exists(), "Failed run published a final directory")
                require(snapshot(final) == published_before, "Failed run altered successful version")
                require(snapshot(bad_source) == before, "Failed run altered its source")
                require(not list(output.glob(".staging-*")), "Failed run left staged data")
                result["checks"].append({"test": variant + "_blocks_publication", "status": "passed", "etl_report": report})
            # Changed inputs must be caught even when a successful version exists.
            changed_source, changed_manifest = fixture(base / "changed")
            with (changed_source / "train.csv").open("a") as stream:
                stream.write("1,0,2016-01-01 03:00:00,5\n")
            completed, report = run(changed_source, changed_manifest, output, evidence)
            require(completed.returncode != 0 and "Source identity mismatch" in report["error"], "Source tampering was not blocked")
            require(snapshot(final) == published_before, "Source failure changed previous output")
            result["checks"].append({"test": "changed_source_blocked", "status": "passed", "etl_report": report})
            # Changed outputs must not be silently reused or overwritten.
            with (final / "electricity.parquet").open("ab") as stream:
                stream.write(b"tampered-for-test")
            tampered = snapshot(final)
            completed, report = run(source, manifest, output, evidence)
            require(completed.returncode != 0 and "Existing output verification failed" in report["error"], "Changed output was not detected")
            require(snapshot(final) == tampered, "Changed output was silently overwritten")
            result["checks"].append({"test": "changed_output_blocked", "status": "passed", "etl_report": report})
        result.update(status="passed", temporary_test_data_removed=True)
        code = 0
    except Exception as error:
        result.update(status="failed", error=str(error))
        print(f"FAIL: {error}", file=sys.stderr)
    finally:
        result["elapsed_seconds"] = round(perf_counter() - started, 3)
        write_json(report_path, result)
        for check in result["checks"]:
            print("PASS: " + check["test"])
        print(f"{result['status'].upper()}: {len(result['checks'])} checks; evidence: {report_path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
