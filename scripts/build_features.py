#!/usr/bin/env python3
"""Build immutable, exact-time electricity features; preserve every target row."""
import argparse
import fcntl
import hashlib
import json
import math
import resource
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb
from publish_processed import ROOT, require, sha256, validate_local

FEATURE_COLUMNS = ["building_id", "site_id", "primary_use", "square_feet", "target_hour",
                   "target_weekday", "target_month", "target_is_weekend",
                   "lag_1h_kwh", "lag_24h_kwh", "lag_168h_kwh",
                   "lag_1h_missing", "lag_24h_missing", "lag_168h_missing", "lag_1h_is_zero"]
OUTPUT_FILES = {"train.parquet", "validation.parquet", "test.parquet", "feature_catalog.json"}


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def clean(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return value


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(clean(value), stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def rows(db, sql):
    cursor = db.execute(sql)
    names = [c[0] for c in cursor.description]
    return [clean(dict(zip(names, r))) for r in cursor.fetchall()]


def make_features(db, config):
    require(config["forecast_horizon_hours"] == 1 and config["history_offsets_hours"] == [1, 24, 168],
            "This implementation requires a one-hour horizon and offsets 1, 24, 168.")
    boundaries = [datetime.fromisoformat(config[k]) for k in
                  ("train_start", "validation_start", "test_start", "end_exclusive")]
    require(all(a < b for a, b in zip(boundaries, boundaries[1:])), "Invalid chronological split boundaries.")
    start, validation, test, end = map(q, boundaries)
    require(db.execute(f"SELECT count(*) FROM electricity WHERE timestamp_reported < TIMESTAMP {start} "
                       f"OR timestamp_reported >= TIMESTAMP {end}").fetchone()[0] == 0,
            "Electricity timestamps fall outside the split plan.")
    db.execute(f"""CREATE OR REPLACE TABLE feature_rows AS SELECT
        t.building_id, t.site_id, t.timestamp_reported AS target_time_reported,
        t.timestamp_reported - INTERVAL '1 hour' AS prediction_time_reported,
        t.meter_reading_kwh AS target_kwh,
        b.primary_use, b.square_feet,
        hour(t.timestamp_reported)::SMALLINT AS target_hour,
        isodow(t.timestamp_reported)::SMALLINT AS target_weekday,
        month(t.timestamp_reported)::SMALLINT AS target_month,
        isodow(t.timestamp_reported) IN (6,7) AS target_is_weekend,
        h1.meter_reading_kwh AS lag_1h_kwh,
        h24.meter_reading_kwh AS lag_24h_kwh,
        h168.meter_reading_kwh AS lag_168h_kwh,
        h1.building_id IS NULL AS lag_1h_missing,
        h24.building_id IS NULL AS lag_24h_missing,
        h168.building_id IS NULL AS lag_168h_missing,
        h1.meter_reading_kwh = 0 AS lag_1h_is_zero,
        h1.timestamp_reported AS lag_1h_source_time,
        h24.timestamp_reported AS lag_24h_source_time,
        h168.timestamp_reported AS lag_168h_source_time,
        h168.building_id IS NOT NULL AS baseline_available,
        CASE WHEN t.timestamp_reported < TIMESTAMP {validation} THEN 'train'
             WHEN t.timestamp_reported < TIMESTAMP {test} THEN 'validation'
             ELSE 'test' END AS split
        FROM electricity t
        LEFT JOIN building_metadata b ON t.building_id=b.building_id AND t.site_id=b.site_id
        LEFT JOIN electricity h1 ON t.building_id=h1.building_id
            AND h1.timestamp_reported=t.timestamp_reported-INTERVAL '1 hour'
        LEFT JOIN electricity h24 ON t.building_id=h24.building_id
            AND h24.timestamp_reported=t.timestamp_reported-INTERVAL '24 hours'
        LEFT JOIN electricity h168 ON t.building_id=h168.building_id
            AND h168.timestamp_reported=t.timestamp_reported-INTERVAL '168 hours'
    """)


def check_features(db):
    input_rows = db.execute("SELECT count(*) FROM electricity").fetchone()[0]
    output_rows = db.execute("SELECT count(*) FROM feature_rows").fetchone()[0]
    require(input_rows == output_rows and input_rows > 0, "Feature joins changed the target row count.")
    duplicates = db.execute("SELECT count(*) FROM (SELECT building_id,target_time_reported "
                            "FROM feature_rows GROUP BY 1,2 HAVING count(*)>1)").fetchone()[0]
    require(duplicates == 0, "Duplicate feature keys detected.")
    invalid = db.execute("""SELECT count(*) FROM feature_rows WHERE
        primary_use IS NULL OR square_feet IS NULL OR square_feet<=0
        OR target_kwh IS NULL OR NOT isfinite(target_kwh) OR target_kwh<0
        OR prediction_time_reported<>target_time_reported-INTERVAL '1 hour'
        OR lag_1h_source_time>prediction_time_reported
        OR lag_24h_source_time>prediction_time_reported
        OR lag_168h_source_time>prediction_time_reported
        OR lag_1h_source_time<>target_time_reported-INTERVAL '1 hour'
        OR lag_24h_source_time<>target_time_reported-INTERVAL '24 hours'
        OR lag_168h_source_time<>target_time_reported-INTERVAL '168 hours'
        OR lag_1h_missing<>(lag_1h_kwh IS NULL)
        OR lag_24h_missing<>(lag_24h_kwh IS NULL)
        OR lag_168h_missing<>(lag_168h_kwh IS NULL)
        OR baseline_available<>(lag_168h_kwh IS NOT NULL)""").fetchone()[0]
    require(invalid == 0, "Invalid feature values, missing flags, or future history detected.")
    summaries = rows(db, """SELECT split, count(*) AS rows, count(DISTINCT building_id) AS buildings,
        min(target_time_reported) AS first_target, max(target_time_reported) AS last_target,
        count(*) FILTER (WHERE lag_1h_missing) AS missing_lag_1h,
        count(*) FILTER (WHERE lag_24h_missing) AS missing_lag_24h,
        count(*) FILTER (WHERE lag_168h_missing) AS missing_lag_168h,
        count(*) FILTER (WHERE baseline_available) AS baseline_eligible_rows
        FROM feature_rows GROUP BY split ORDER BY first_target""")
    require({s["split"] for s in summaries} == {"train", "validation", "test"}, "A planned partition is empty.")
    return {"input_rows": input_rows, "output_rows": output_rows, "dropped_rows": 0,
            "duplicate_key_groups": duplicates, "invalid_feature_rows": invalid, "split_summary": summaries}


def catalog(db, config):
    definitions = {
        "building_id": "Building identifier; categorical predictor, not an ordinal quantity.",
        "site_id": "Site identifier; categorical predictor, not an ordinal quantity.",
        "primary_use": "Building use category from metadata.",
        "square_feet": "Building floor area from metadata, square feet.",
        "target_hour": "Reported target hour, 0-23; calendar known at forecast origin.",
        "target_weekday": "Reported target ISO weekday, Monday=1 through Sunday=7.",
        "target_month": "Reported target month, 1-12.",
        "target_is_weekend": "ISO weekday 6 or 7. Generic Saturday/Sunday flag, not a verified local work calendar.",
        "lag_1h_is_zero": "Whether the previous-hour observed reading is zero; NULL if unavailable.",
        "target_kwh": "Future target label; NEVER a predictor.",
        "target_time_reported": "Label timestamp as reported, timezone not asserted.",
        "prediction_time_reported": "Forecast origin: reported target timestamp minus one hour.",
        "split": "Chronological partition assignment; NEVER a predictor.",
        "baseline_available": "Evaluation mask: exact same-hour-last-week reading exists; NEVER a predictor.",
    }
    for h in (1, 24, 168):
        definitions[f"lag_{h}h_kwh"] = f"Same-building reading exactly {h} hours before target; kWh, NULL if absent."
        definitions[f"lag_{h}h_missing"] = f"True when the exact {h}-hour historical reading is absent."
        definitions[f"lag_{h}h_source_time"] = "Historical timestamp retained only for availability auditing; not a predictor."
    columns = []
    for name, dtype, *_ in db.execute("DESCRIBE feature_rows").fetchall():
        source = ("building_metadata.parquet" if name in {"primary_use", "square_feet"}
                  else "electricity.parquet / reported timestamp / split configuration")
        role = "predictor" if name in FEATURE_COLUMNS else "label" if name == "target_kwh" else "audit_or_evaluation"
        availability = ("Unavailable at forecast origin; label only." if role == "label"
                        else "Static metadata assumed known before forecast." if name in {"primary_use", "square_feet", "building_id", "site_id"}
                        else "Known calendar/configuration, or historical observation no later than forecast origin, subject to the configured delivery assumption.")
        columns.append({"name": name, "type": dtype, "role": role, "definition": definitions[name],
                        "source": source, "availability": availability})
    return {"columns": columns, "model_feature_allowlist": FEATURE_COLUMNS,
            "key": ["building_id", "target_time_reported"], "label": "target_kwh",
            "units": "kWh for target and lag readings", "config": config,
            "missing_policy": "NULL lags plus explicit indicators; no imputation or forward filling.",
            "zero_policy": "Zeros retained; historical zero flag only. No future zero-run flags.",
            "weather_policy": "Deferred pending timestamp alignment and availability validation.",
            "baseline": "lag_168h_kwh; compare both models on identical baseline_available rows.",
            "lineage": "Verified processed manifest -> exact-time historical joins + metadata + calendar -> versioned split Parquet"}


def build(directory, root, destination, evidence, report):
    config_path = root / "config/features_v1.json"
    config = json.loads(config_path.read_text())
    require(duckdb.__version__ == config["required_duckdb_version"], "DuckDB version mismatch.")
    manifest, source_records = validate_local(directory, root)
    identity = {"processed_manifest_sha256": sha256(directory / "output_manifest.json"),
                "feature_config_sha256": sha256(config_path), "feature_code_sha256": sha256(__file__),
                "validation_helper_sha256": sha256(root / "scripts/publish_processed.py"),
                "duckdb_version": duckdb.__version__}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    final = destination / ("features-v1-" + digest[:16])
    report.update(identity=identity, config=config, processed_directory=str(directory), output_directory=str(final))
    if final.exists():
        existing = json.loads((final / "feature_manifest.json").read_text())
        require(existing["identity"] == identity and existing["gates_passed"] is True, "Existing feature identity/gates mismatch.")
        require(len(existing["files"]) == len(OUTPUT_FILES)
                and {f["file_name"] for f in existing["files"]} == OUTPUT_FILES, "Unexpected feature manifest files.")
        require({p.name for p in final.iterdir()} == OUTPUT_FILES | {"feature_manifest.json"}, "Unexpected existing feature directory contents.")
        for item in existing["files"]:
            path = final / item["file_name"]
            require(path.stat().st_size == item["size_bytes"] and sha256(path) == item["sha256"], "Existing feature output changed: " + path.name)
        report.update(status="passed", action="skipped_verified", quality=existing["quality"], files=existing["files"])
        print("PASS: existing feature version verified; no files rewritten.", flush=True)
        return
    stage = destination / (".staging-" + uuid4().hex)
    stage.mkdir()
    db = None
    try:
        db = duckdb.connect(str(stage / "work.duckdb"))
        db.execute(f"SET threads={int(config['threads'])}")
        db.execute(f"SET memory_limit={q(config['memory_limit'])}")
        db.execute(f"SET temp_directory={q(stage / 'spill')}")
        db.execute("SET preserve_insertion_order=false")
        for table in ("electricity", "building_metadata"):
            db.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet({q(directory / (table + '.parquet'))})")
        print("Building calendar, metadata and exact-time historical features ...", flush=True)
        make_features(db, config)
        quality = check_features(db)
        report["quality"] = quality
        for summary in quality["split_summary"]:
            print(json.dumps(summary), flush=True)
        # Descriptive associations are training-only, not validation/test scoring.
        report["training_correlations"] = rows(db, """SELECT
            corr(target_kwh,lag_1h_kwh) AS target_vs_lag_1h,
            count(lag_1h_kwh) AS pairs_1h,
            corr(target_kwh,lag_24h_kwh) AS target_vs_lag_24h,
            count(lag_24h_kwh) AS pairs_24h,
            corr(target_kwh,lag_168h_kwh) AS target_vs_lag_168h,
            count(lag_168h_kwh) AS pairs_168h
            FROM feature_rows WHERE split='train'""")[0]
        for summary in quality["split_summary"]:
            split = summary["split"]
            path = stage / (split + ".parquet")
            print(f"Writing {path.name} ...", flush=True)
            db.execute(f"COPY (SELECT * FROM feature_rows WHERE split={q(split)} "
                       f"ORDER BY building_id,target_time_reported) TO {q(path)} "
                       "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
            require(db.execute(f"SELECT count(*) FROM read_parquet({q(path)})").fetchone()[0] == summary["rows"],
                    "Written Parquet row count mismatch.")
        write_json(stage / "feature_catalog.json", catalog(db, config))
        db.close()
        db = None
        (stage / "work.duckdb").unlink()
        shutil.rmtree(stage / "spill", ignore_errors=True)
        for item in source_records:
            require(sha256(directory / item["file_name"]) == item["sha256"], "Processed input changed during feature build.")
        files = [{"file_name": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)} for p in sorted(stage.iterdir())]
        output_manifest = {"identity": identity, "gates_passed": True, "quality": quality, "files": files,
            "config": config, "processed_identity": manifest["identity"], "processed_files": source_records,
            "feature_columns": FEATURE_COLUMNS, "training_correlations": report["training_correlations"],
            "created_utc": datetime.now(timezone.utc).isoformat()}
        write_json(stage / "feature_manifest.json", output_manifest)
        stage.rename(final)
        report.update(status="passed", action="published_local", files=files,
                      output_bytes=sum(f["size_bytes"] for f in files))
        print("Training correlations: " + json.dumps(report["training_correlations"]), flush=True)
        print("PASS: feature checks passed; versioned outputs published locally on VM.", flush=True)
    finally:
        if db is not None:
            db.close()
        if stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--etl-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path.home() / "electricity-data/features")
    args = parser.parse_args()
    directory, destination = args.etl_dir.expanduser().resolve(), args.output_root.expanduser().resolve()
    require(destination != directory and directory not in destination.parents, "Feature outputs must be outside processed input version.")
    destination.mkdir(parents=True, exist_ok=True)
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report = {"run_id": run_id, "status": "running"}
    started = perf_counter()
    code = 1
    try:
        with (destination / ".features.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            build(directory, ROOT, destination, evidence, report)
        print("Output: " + report["output_directory"], flush=True)
        code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print(f"FAILED: {error}", file=sys.stderr, flush=True)
    finally:
        report["elapsed_seconds"] = round(perf_counter()-started,3)
        report["peak_process_rss_mib"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,2)
        path = evidence / f"features_{run_id}.json"
        write_json(path, report)
        print("Evidence: " + str(path), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
