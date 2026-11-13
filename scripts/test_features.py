#!/usr/bin/env python3
"""Test feature timing, gaps, boundary behavior and immutable reruns on isolated fixtures."""
import csv
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import publish_processed
import run_etl
from build_features import FEATURE_COLUMNS, make_features, sha256, write_json, require

PROJECT = Path(__file__).resolve().parents[1]


def fixture(root):
    for folder in ("scripts", "config", "evidence", "source"):
        (root / folder).mkdir(parents=True)
    for path in (PROJECT / "scripts/build_features.py", Path(publish_processed.__file__), Path(run_etl.__file__)):
        shutil.copy2(path, root / "scripts" / path.name)
    shutil.copy2(PROJECT / "config/features_v1.json", root / "config/features_v1.json")
    shutil.copy2(Path(run_etl.__file__).resolve().parents[1] / "config/etl_v1.json", root / "config/etl_v1.json")
    train = [[0,0,"2016-01-01 00:00:00",0], [0,0,"2016-01-01 01:00:00",10],
             [0,0,"2016-01-01 03:00:00",30], [0,0,"2016-01-02 00:00:00",40],
             [0,0,"2016-01-08 00:00:00",80], [0,0,"2016-08-31 23:00:00",50],
             [0,0,"2016-09-01 00:00:00",60], [0,0,"2016-10-31 23:00:00",70],
             [0,0,"2016-11-01 00:00:00",80],
             [1,0,"2016-01-01 00:00:00",1000], [1,0,"2016-01-01 01:00:00",1001]]
    content = {"building_metadata.csv": [[1,0,"Office",1000,"",""],[1,1,"Education",2000,2000,3]],
               "train.csv": train, "test.csv": [[0,0,0,"2017-01-01 00:00:00"]],
               "sample_submission.csv": [[0,0]],
               "weather_train.csv": [[1,"2016-01-01 00:00:00",20,"",10,-1,1010,90,2]],
               "weather_test.csv": [[1,"2017-01-01 00:00:00",20,"",10,-1,1010,90,2]]}
    records = []
    for name, header in run_etl.HEADERS.items():
        path = root / "source" / name
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(header.split(","))
            writer.writerows(content[name])
        records.append({"file_name": name, "size_bytes": path.stat().st_size,
                        "sha256": sha256(path), "columns": header.split(",")})
    write_json(root / "evidence/source_manifest_v1.json", {"source_version": "ashrae-v1", "files": records})


def command(root, script, *arguments):
    result = subprocess.run([sys.executable, str(root / "scripts" / script), *map(str, arguments)],
                            capture_output=True, text=True)
    return result


def snapshot(root):
    return {str(p.relative_to(root)): (sha256(p), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()}


def main():
    report = {"status": "running", "scope": "synthetic isolated CSV -> real ETL -> real feature Parquet",
              "feature_code_sha256": sha256(PROJECT / "scripts/build_features.py"),
              "test_code_sha256": sha256(__file__), "checks": []}
    def passed(name):
        report["checks"].append({"test": name, "status": "passed"})
        print("PASS: " + name)
    code = 1
    try:
        with tempfile.TemporaryDirectory(prefix="electricity-feature-tests-") as temporary:
            root = Path(temporary)
            fixture(root)
            result = command(root, "run_etl.py", "--source-dir", root / "source", "--output-root", root / "processed")
            require(result.returncode == 0, result.stdout + result.stderr)
            source = next((root / "processed").glob("etl-v1-*"))
            source_before = snapshot(source)
            result = command(root, "build_features.py", "--etl-dir", source, "--output-root", root / "features")
            require(result.returncode == 0, result.stdout + result.stderr)
            final = next((root / "features").glob("features-v1-*"))
            db = duckdb.connect()
            db.read_parquet([str(final / (s + ".parquet")) for s in ("train", "validation", "test")]).create_view("features")
            require(db.execute("SELECT count(*) FROM features").fetchone()[0] == 11, "Target rows were lost")
            require(dict(db.execute("SELECT split,count(*) FROM features GROUP BY split").fetchall()) == {"train":8,"validation":2,"test":1}, "Split counts incorrect")
            passed("preserves_all_rows_and_chronological_splits")
            def get(timestamp, fields, building=0):
                return db.execute(f"SELECT {fields} FROM features WHERE building_id=? AND target_time_reported=?::TIMESTAMP", [building,timestamp]).fetchone()
            require(get("2016-01-01 01:00:00", "lag_1h_kwh,lag_1h_missing,lag_1h_is_zero") == (0,False,True), "Zero history misclassified as missing")
            require(get("2016-01-01 03:00:00", "lag_1h_kwh,lag_1h_missing,lag_1h_is_zero") == (None,True,None), "Gap was forward-filled or treated as previous row")
            passed("exact_hour_gaps_and_zero_history")
            require(get("2016-01-02 00:00:00", "lag_24h_kwh") == (0,), "Daily offset incorrect")
            require(get("2016-01-08 00:00:00", "lag_168h_kwh,baseline_available") == (0,True), "Weekly zero baseline unavailable")
            require(get("2016-01-01 01:00:00", "lag_1h_kwh", 1) == (1000,), "History mixed buildings")
            passed("daily_weekly_offsets_and_building_isolation")
            require(get("2016-09-01 00:00:00", "split,lag_1h_kwh") == ("validation",50), "Training-to-validation history incorrect")
            require(get("2016-11-01 00:00:00", "split,lag_1h_kwh") == ("test",70), "Validation-to-test rolling history incorrect")
            require(get("2016-01-02 00:00:00", "target_hour,target_weekday,target_month,target_is_weekend") == (0,6,1,True), "Calendar fields incorrect")
            passed("boundary_history_and_calendar_fields")
            require(db.execute("SELECT count(*) FROM features WHERE lag_1h_source_time>prediction_time_reported OR lag_24h_source_time>prediction_time_reported OR lag_168h_source_time>prediction_time_reported").fetchone()[0] == 0, "Future source timestamp detected")
            feature_catalog = json.loads((final / "feature_catalog.json").read_text())
            require(feature_catalog["model_feature_allowlist"] == FEATURE_COLUMNS, "Predictor allowlist changed")
            require(not {"target_kwh", "split", "baseline_available", "is_zero"} & set(FEATURE_COLUMNS), "Label/evaluation leakage in predictors")
            passed("availability_audits_and_predictor_allowlist")
            # Changing the current/future labels must not change current predictors.
            db.read_parquet(str(source / "electricity.parquet")).create_view("electricity_input")
            db.execute("CREATE TABLE electricity AS SELECT * FROM electricity_input")
            db.read_parquet(str(source / "building_metadata.parquet")).create_view("building_metadata")
            config = json.loads((root / "config/features_v1.json").read_text())
            make_features(db, config)
            selection = "SELECT " + ",".join(FEATURE_COLUMNS) + " FROM feature_rows WHERE building_id=0 AND target_time_reported=TIMESTAMP '2016-01-02 00:00:00'"
            before = db.execute(selection).fetchone()
            db.execute("UPDATE electricity SET meter_reading_kwh=meter_reading_kwh+9999 WHERE timestamp_reported>=TIMESTAMP '2016-01-02 00:00:00'")
            make_features(db, config)
            require(db.execute(selection).fetchone() == before, "Current/future target values changed current predictors")
            require(db.execute("SELECT target_kwh FROM feature_rows WHERE building_id=0 AND target_time_reported=TIMESTAMP '2016-01-02 00:00:00'").fetchone()[0] == 10039, "Future perturbation was not applied")
            db.close()
            passed("current_and_future_label_changes_do_not_change_predictors")
            before = snapshot(final)
            result = command(root, "build_features.py", "--etl-dir", source, "--output-root", root / "features")
            require(result.returncode == 0 and "no files rewritten" in result.stdout, "Safe rerun failed")
            require(snapshot(final) == before and snapshot(source) == source_before, "Rerun modified output or processed inputs")
            passed("safe_rerun_and_processed_input_preservation")
            with (final / "train.parquet").open("ab") as stream:
                stream.write(b"tampered-test")
            changed = snapshot(final)
            result = command(root, "build_features.py", "--etl-dir", source, "--output-root", root / "features")
            require(result.returncode != 0 and "Existing feature output changed" in result.stderr, "Changed feature bytes were not blocked")
            require(snapshot(final) == changed, "Changed feature version was overwritten")
            passed("changed_feature_output_blocked")
            with (source / "electricity.parquet").open("ab") as stream:
                stream.write(b"tampered-input")
            result = command(root, "build_features.py", "--etl-dir", source, "--output-root", root / "new_features")
            require(result.returncode != 0 and "Local output hash/size mismatch" in result.stderr, "Changed processed input was not blocked")
            require(not list((root / "new_features").glob("features-v1-*")), "Invalid input published features")
            passed("changed_processed_input_blocks_publication")
        report.update(status="passed", temporary_data_removed=True)
        code = 0
    except Exception as error:
        report.update(status="failed", error=str(error))
        print("FAIL: " + str(error), file=sys.stderr)
    finally:
        evidence = PROJECT / "evidence"
        evidence.mkdir(exist_ok=True)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
        path = evidence / f"feature_test_{run_id}.json"
        write_json(path, report)
        print(f"{report['status'].upper()}: {len(report['checks'])} checks; evidence: {path}")
    return code


if __name__ == "__main__":
    sys.exit(main())
