#!/usr/bin/env python3
"""Manifest-verified ASHRAE ETL. Publish local Parquet only after quality gates.

No credentials, network calls, raw writes, imputation, or model fitting.
Linux VM execution; DuckDB pinned by the configuration and requirements.txt.
"""
import argparse
import csv
import fcntl
import hashlib
import json
import platform
import resource
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

ROOT = Path(__file__).resolve().parents[1]
HEADERS = {
    "building_metadata.csv": "site_id,building_id,primary_use,square_feet,year_built,floor_count",
    "train.csv": "building_id,meter,timestamp,meter_reading",
    "test.csv": "row_id,building_id,meter,timestamp",
    "sample_submission.csv": "row_id,meter_reading",
    "weather_train.csv": "site_id,timestamp,air_temperature,cloud_coverage,dew_temperature,precip_depth_1_hr,sea_level_pressure,wind_direction,wind_speed",
    "weather_test.csv": "site_id,timestamp,air_temperature,cloud_coverage,dew_temperature,precip_depth_1_hr,sea_level_pressure,wind_direction,wind_speed",
}
WEATHER = HEADERS["weather_train.csv"].split(",")[2:]


class QualityFailure(RuntimeError):
    pass


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False, default=str)
        stream.write("\n")


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def blank(column):
    return f"nullif(trim({column}), '')"


def integer(column):
    # A cast to INTEGER alone can round fractional IDs: require integer text.
    value = blank(column)
    return (f"CASE WHEN regexp_full_match({value}, '[+-]?[0-9]+') "
            f"THEN try_cast({value} AS BIGINT) END")


def scalar(db, sql):
    return db.execute(sql).fetchone()[0]


def verify_source(source, manifest):
    records = manifest["files"]
    if len(records) != len(HEADERS) or {r["file_name"] for r in records} != set(HEADERS):
        raise RuntimeError("Manifest must identify exactly the six ASHRAE CSV files.")
    if {p.name for p in source.glob("*.csv")} != set(HEADERS):
        raise RuntimeError("Source CSV collection differs from the manifest.")
    for record in records:
        path = source / record["file_name"]
        if path.stat().st_size != record["size_bytes"] or sha256(path) != record["sha256"]:
            raise RuntimeError(f"Source identity mismatch: {path.name}")
        with path.open(newline="", encoding="utf-8-sig") as stream:
            columns = next(csv.reader(stream))
        if columns != HEADERS[path.name].split(",") or columns != record["columns"]:
            raise RuntimeError(f"Source header mismatch: {path.name}")


def check_table(db, table, report, output_root, run_id, limit):
    total = scalar(db, f"SELECT count(*) FROM {table}")
    rejected = scalar(db, f"SELECT count(*) FROM {table} WHERE reject_reason IS NOT NULL")
    excluded = (scalar(db, f"SELECT count(*) FROM {table} WHERE excluded")
                if table == "electricity_checked" else 0)
    stats = {"input_rows": total, "excluded_rows": excluded,
             "rejected_rows": rejected, "accepted_rows": total - excluded - rejected}
    stats["rejection_reasons"] = dict(db.execute(
        f"SELECT reject_reason, count(*) FROM {table} WHERE reject_reason IS NOT NULL GROUP BY 1"
    ).fetchall())
    report["row_accounting"][table] = stats
    print(f"{table}: {json.dumps(stats)}", flush=True)
    if rejected:
        directory = output_root / "_rejected" / run_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (table + ".parquet")
        db.execute(f"COPY (SELECT * FROM {table} WHERE reject_reason IS NOT NULL) "
                   f"TO {q(path)} (FORMAT PARQUET, COMPRESSION ZSTD)")
        stats["quarantine_path"] = str(path)
    if rejected > limit:
        raise QualityFailure(f"{table}: {rejected} rejected rows exceed limit {limit}.")
    return stats


def build(db, source, config, report, stage, output_root, run_id):
    for name in ("building_metadata", "train", "weather_train"):
        db.execute(f"CREATE VIEW raw_{name} AS SELECT * FROM read_csv("
                   f"{q(source / (name + '.csv'))}, header=true, all_varchar=true, "
                   "strict_mode=true, ignore_errors=false)")
    start, end = q(config["timestamp_start"]), q(config["timestamp_end_exclusive"])
    time_bad = (f"timestamp_reported IS NULL OR timestamp_reported < TIMESTAMP {start} "
                f"OR timestamp_reported >= TIMESTAMP {end} "
                "OR timestamp_reported <> date_trunc('hour', timestamp_reported)")
    limit = config["maximum_rejected_rows_per_table"]
    # Fail closed for this immutable snapshot, whose profile found zero key/type errors.
    if limit != 0:
        raise RuntimeError("This ETL version requires a zero-rejection quality gate.")
    print("Checking building metadata ...", flush=True)
    db.execute(f"""CREATE TABLE metadata_typed AS SELECT
        {integer('building_id')} AS building_id, {integer('site_id')} AS site_id,
        {blank('primary_use')} AS primary_use,
        try_cast(square_feet AS DOUBLE) AS square_feet,
        {integer('year_built')} AS year_built, {integer('floor_count')} AS floor_count,
        {blank('year_built')} AS year_built_raw, {blank('floor_count')} AS floor_count_raw,
        building_id AS building_id_raw, site_id AS site_id_raw,
        square_feet AS square_feet_raw
        FROM raw_building_metadata""")
    db.execute("""CREATE TABLE metadata_checked AS SELECT *, CASE
        WHEN building_id IS NULL OR building_id < 0 OR site_id IS NULL OR site_id < 0
            THEN 'invalid_identifier'
        WHEN count(*) OVER (PARTITION BY building_id) > 1 THEN 'duplicate_building_id'
        WHEN primary_use IS NULL THEN 'missing_primary_use'
        WHEN square_feet IS NULL OR NOT isfinite(square_feet) OR square_feet <= 0
            THEN 'invalid_square_feet'
        WHEN year_built_raw IS NOT NULL AND year_built IS NULL THEN 'invalid_year_built'
        WHEN floor_count_raw IS NOT NULL AND (floor_count IS NULL OR floor_count < 0)
            THEN 'invalid_floor_count'
        END AS reject_reason FROM metadata_typed""")
    check_table(db, "metadata_checked", report, output_root, run_id, limit)
    db.execute("""CREATE TABLE buildings AS SELECT building_id, site_id, primary_use,
        square_feet, year_built, floor_count FROM metadata_checked WHERE reject_reason IS NULL""")
    print("Checking electricity readings (all training rows) ...", flush=True)
    db.execute(f"""CREATE TABLE train_typed AS SELECT
        {integer('building_id')} AS building_id, {integer('meter')} AS meter,
        try_strptime({blank('timestamp')}, '%Y-%m-%d %H:%M:%S') AS timestamp_reported,
        try_cast(meter_reading AS DOUBLE) AS meter_reading_original,
        building_id AS building_id_raw, meter AS meter_raw,
        timestamp AS timestamp_raw, meter_reading AS meter_reading_raw
        FROM raw_train""")
    db.execute("""CREATE TABLE duplicate_electricity AS
        SELECT building_id, timestamp_reported FROM train_typed WHERE meter = 0
        GROUP BY building_id, timestamp_reported HAVING count(*) > 1""")
    db.execute(f"""CREATE TABLE electricity_checked AS SELECT t.*,
        coalesce(t.meter IN (1, 2, 3), false) AS excluded, CASE
        WHEN t.meter IS NULL OR t.meter NOT IN (0, 1, 2, 3) THEN 'invalid_meter'
        WHEN t.meter <> 0 THEN NULL
        WHEN t.building_id IS NULL OR t.building_id < 0 THEN 'invalid_building_id'
        WHEN {time_bad.replace('timestamp_reported', 't.timestamp_reported')} THEN 'invalid_timestamp'
        WHEN meter_reading_original IS NULL OR NOT isfinite(meter_reading_original)
            OR meter_reading_original < 0 THEN 'invalid_reading'
        WHEN d.building_id IS NOT NULL THEN 'duplicate_electricity_key'
        WHEN b.building_id IS NULL THEN 'missing_building_metadata'
        END AS reject_reason
        FROM train_typed t LEFT JOIN buildings b ON t.building_id = b.building_id
        LEFT JOIN duplicate_electricity d ON t.building_id = d.building_id
            AND t.timestamp_reported = d.timestamp_reported""")
    stats = check_table(db, "electricity_checked", report, output_root, run_id, limit)
    if stats["accepted_rows"] < config["minimum_electricity_rows"]:
        raise QualityFailure("Too few accepted electricity rows.")
    factor = float(config["site_0_kbtu_to_kwh"])
    if not 0 < factor < 1:
        raise RuntimeError("Invalid kBTU conversion factor.")
    db.execute(f"""CREATE VIEW electricity AS SELECT t.building_id, b.site_id,
        t.timestamp_reported, t.meter_reading_original,
        CASE WHEN b.site_id = 0 THEN t.meter_reading_original * {factor}
             ELSE t.meter_reading_original END AS meter_reading_kwh,
        CASE WHEN b.site_id = 0 THEN 'kBTU' ELSE 'kWh' END AS original_unit,
        b.site_id = 0 AS unit_converted, t.meter_reading_original = 0 AS is_zero
        FROM electricity_checked t JOIN buildings b ON t.building_id = b.building_id
        WHERE NOT excluded AND reject_reason IS NULL""")
    print("Checking training weather (kept separate from electricity) ...", flush=True)
    typed_weather = ", ".join(f"try_cast({c} AS DOUBLE) AS {c}, {blank(c)} AS {c}_raw"
                              for c in WEATHER)
    db.execute(f"""CREATE TABLE weather_typed AS SELECT {integer('site_id')} AS site_id,
        try_strptime({blank('timestamp')}, '%Y-%m-%d %H:%M:%S') AS timestamp_reported,
        site_id AS site_id_raw, timestamp AS timestamp_raw, {typed_weather}
        FROM raw_weather_train""")
    invalid_weather = " OR ".join(
        f"({c}_raw IS NOT NULL AND ({c} IS NULL OR NOT isfinite({c})))" for c in WEATHER)
    db.execute(f"""CREATE TABLE weather_checked AS SELECT *, CASE
        WHEN site_id IS NULL OR site_id < 0 THEN 'invalid_site_id'
        WHEN {time_bad} THEN 'invalid_timestamp'
        WHEN count(*) OVER (PARTITION BY site_id, timestamp_reported) > 1
            THEN 'duplicate_weather_key'
        WHEN {invalid_weather} THEN 'invalid_weather_number'
        END AS reject_reason FROM weather_typed""")
    weather_stats = check_table(db, "weather_checked", report, output_root, run_id, limit)
    if not weather_stats["accepted_rows"]:
        raise QualityFailure("No accepted weather rows.")
    db.execute(f"CREATE VIEW weather AS SELECT site_id, timestamp_reported, {', '.join(WEATHER)} "
               "FROM weather_checked WHERE reject_reason IS NULL")
    report["gates_passed"] = True
    report["electricity_summary"] = dict(zip(
        ["rows", "buildings", "zero_rows", "converted_site_0_rows", "first_timestamp", "last_timestamp"],
        db.execute("""SELECT count(*), count(DISTINCT building_id), count(*) FILTER (WHERE is_zero),
            count(*) FILTER (WHERE unit_converted), min(timestamp_reported), max(timestamp_reported)
            FROM electricity""").fetchone()))
    tables = {
        "electricity": ("electricity", "building_id, timestamp_reported", ["train.csv", "building_metadata.csv"]),
        "building_metadata": ("buildings", "building_id", ["building_metadata.csv"]),
        "weather_train": ("weather", "site_id, timestamp_reported", ["weather_train.csv"]),
    }
    catalog = {"tables": {}, "timestamp_policy": "Source timestamps preserved without timezone claims. Weather alignment is unresolved; no weather-to-meter join performed.",
               "missing_policy": "Optional metadata/weather remain NULL; no imputation.",
               "zero_policy": "Retained and flagged; not presumed to be bad sensor data.",
               "weather_sentinel_policy": "Numeric values including precip_depth_1_hr=-1 retained as reported; not yet model features.",
               "unit_policy": f"Site 0 electricity converted from kBTU using {factor}; other sites retained as kWh. Original reading and unit retained.",
               "source_references": ["https://www.kaggle.com/c/ashrae-energy-prediction/discussion/119261",
                   "https://www.nist.gov/pml/special-publication-811/nist-guide-si-appendix-b-conversion-factors/nist-guide-si-appendix-b9"]}
    for name, (table, keys, inputs) in tables.items():
        path = stage / (name + ".parquet")
        print(f"Writing {path.name} ...", flush=True)
        db.execute(f"COPY (SELECT * FROM {table} ORDER BY {keys}) TO {q(path)} "
                   "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
        rows = scalar(db, f"SELECT count(*) FROM {table}")
        if scalar(db, f"SELECT count(*) FROM read_parquet({q(path)})") != rows:
            raise QualityFailure(f"Output row count mismatch: {name}")
        catalog["tables"][name] = {"file_name": path.name, "row_count": rows,
            "key": keys.split(", "), "raw_sources": inputs,
            "columns": [{"name": c[0], "type": c[1]} for c in db.execute(f"DESCRIBE {table}").fetchall()]}
    write_json(stage / "catalog.json", catalog)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path.home() / "electricity-data/incoming/ashrae-v1")
    parser.add_argument("--manifest", type=Path, default=ROOT / "evidence/source_manifest_v1.json")
    parser.add_argument("--config", type=Path, default=ROOT / "config/etl_v1.json")
    parser.add_argument("--output-root", type=Path, default=Path.home() / "electricity-data/processed")
    parser.add_argument("--evidence-dir", type=Path, default=ROOT / "evidence")
    args = parser.parse_args()
    source, output_root = args.source_dir.resolve(), args.output_root.resolve()
    for destination in (output_root, args.evidence_dir.resolve()):
        if destination == source or source in destination.parents:
            parser.error("Output/evidence locations must be outside the raw source directory.")
    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report = {"run_id": run_id, "status": "running", "gates_passed": False, "row_accounting": {},
              "source_directory": str(source), "output_root": str(output_root),
              "python_version": platform.python_version(), "duckdb_version": duckdb.__version__}
    report_path = args.evidence_dir / f"etl_{run_id}.json"
    started = perf_counter()
    stage = output_root / (".staging-" + run_id)
    db = lock = None
    exit_code = 1
    try:
        config = json.loads(args.config.read_text())
        manifest = json.loads(args.manifest.read_text())
        if duckdb.__version__ != config["required_duckdb_version"]:
            raise RuntimeError("DuckDB version differs from the pinned ETL configuration.")
        if manifest["source_version"] != config["source_version"]:
            raise RuntimeError("Source version differs from the configuration.")
        print("Verifying all six source files against the preserved manifest ...", flush=True)
        verify_source(source, manifest)
        identity = {"source_manifest_sha256": sha256(args.manifest), "config_sha256": sha256(args.config),
                    "etl_code_sha256": sha256(__file__), "duckdb_version": duckdb.__version__}
        version_hash = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        final = output_root / ("etl-v1-" + version_hash[:16])
        report.update(identity=identity, output_directory=str(final), config=config)
        lock = (output_root / ".etl.lock").open("a")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if final.exists():
            existing = json.loads((final / "output_manifest.json").read_text())
            if existing["identity"] != identity or not existing["gates_passed"]:
                raise RuntimeError("Existing output has an incompatible identity or failed quality gates.")
            expected = {"electricity.parquet", "building_metadata.parquet", "weather_train.parquet", "catalog.json"}
            files = existing["files"]
            if len(files) != len(expected) or {f["file_name"] for f in files} != expected:
                raise RuntimeError("Existing output manifest has unexpected files.")
            if {p.name for p in final.iterdir()} != expected | {"output_manifest.json"}:
                raise RuntimeError("Existing output directory has unexpected files.")
            for record in files:
                path = final / record["file_name"]
                if path.stat().st_size != record["size_bytes"] or sha256(path) != record["sha256"]:
                    raise RuntimeError(f"Existing output verification failed: {path.name}")
            report.update(status="passed", action="skipped_verified", gates_passed=True,
                          row_accounting=existing["row_accounting"], files=files)
            print(f"PASS: existing ETL output verified; no files rewritten.\nOutput: {final}", flush=True)
        else:
            stage.mkdir()
            db = duckdb.connect(str(stage / "work.duckdb"))
            db.execute(f"SET threads = {int(config['threads'])}")
            db.execute(f"SET memory_limit = {q(config['memory_limit'])}")
            db.execute(f"SET temp_directory = {q(stage / 'spill')}")
            db.execute("SET preserve_insertion_order = false")
            build(db, source, config, report, stage, output_root, run_id)
            db.close()
            db = None
            (stage / "work.duckdb").unlink()
            shutil.rmtree(stage / "spill", ignore_errors=True)
            files = [{"file_name": p.name, "size_bytes": p.stat().st_size, "sha256": sha256(p)}
                     for p in sorted(stage.iterdir())]
            output_manifest = {"identity": identity, "source_version": manifest["source_version"],
                "created_utc": datetime.now(timezone.utc).isoformat(), "gates_passed": True,
                "row_accounting": report["row_accounting"], "files": files,
                "raw_sources": manifest["files"], "config": config}
            write_json(stage / "output_manifest.json", output_manifest)
            stage.rename(final)
            report.update(status="passed", action="published_local", files=files,
                          output_bytes=sum(f["size_bytes"] for f in files))
            print(f"PASS: quality gates passed; versioned output published locally on VM.\nOutput: {final}", flush=True)
        exit_code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print(f"FAILED: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
    finally:
        if db is not None:
            db.close()
        if stage.exists():
            shutil.rmtree(stage)
        if lock is not None:
            lock.close()
        report["elapsed_seconds"] = round(perf_counter() - started, 3)
        report["peak_process_rss_mib"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        write_json(report_path, report)
        print(f"Evidence: {report_path}", flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
