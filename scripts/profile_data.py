#!/usr/bin/env python3
"""Profile every row in the manifest-backed ASHRAE CSV collection.

Run on the project Linux VM with DuckDB 1.4.3. Raw files are read only.
Outputs are uniquely named JSON reports; no cleaning or publication occurs.
"""
import argparse
import hashlib
import json
import platform
import resource
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

KEYS = {
    "building_metadata.csv": [["building_id"]],
    "train.csv": [["building_id", "meter", "timestamp"]],
    "test.csv": [["building_id", "meter", "timestamp"], ["row_id"]],
    "weather_train.csv": [["site_id", "timestamp"]],
    "weather_test.csv": [["site_id", "timestamp"]],
    "sample_submission.csv": [["row_id"]],
}
INTEGER_COLUMNS = {
    "building_id", "site_id", "meter", "row_id", "year_built", "floor_count"
}


def quote_id(value):
    return '"' + value.replace('"', '""') + '"'


def quote_text(value):
    return "'" + str(value).replace("'", "''") + "'"


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def query_one(connection, sql):
    result = connection.execute(sql)
    names = [column[0] for column in result.description]
    return dict(zip(names, result.fetchone()))


def profile_file(connection, name, columns):
    table = quote_id("src_" + Path(name).stem)
    expressions = ["count(*) AS row_count"]
    types = {}
    for index, column in enumerate(columns):
        value = f"nullif(trim({quote_id(column)}), '')"
        kind = ("VARCHAR" if column == "primary_use" else
                "TIMESTAMP" if column == "timestamp" else
                "BIGINT" if column in INTEGER_COLUMNS else "DOUBLE")
        types[column] = kind
        expressions.append(f"count(*) FILTER (WHERE {value} IS NULL) AS c{index}_missing")
        if kind == "VARCHAR":
            continue
        cast = f"try_cast({value} AS {kind})"
        expressions.append(
            f"count(*) FILTER (WHERE {value} IS NOT NULL AND {cast} IS NULL) AS c{index}_cast_failures"
        )
        valid = f"{cast} IS NOT NULL"
        if kind == "DOUBLE":
            expressions.append(
                f"count(*) FILTER (WHERE {cast} IS NOT NULL AND NOT isfinite({cast})) AS c{index}_nonfinite"
            )
            valid += f" AND isfinite({cast})"
        if kind == "BIGINT":
            # BIGINT conversion can round decimals; detect fractional identifiers too.
            number = f"try_cast({value} AS DOUBLE)"
            expressions.append(
                f"count(*) FILTER (WHERE isfinite({number}) AND {number} <> trunc({number})) AS c{index}_fractional"
            )
        if kind == "TIMESTAMP":
            expressions.append(
                f"count(*) FILTER (WHERE {cast} <> date_trunc('hour', {cast})) AS c{index}_off_hour"
            )
        expressions.extend([
            f"min({cast}) FILTER (WHERE {valid}) AS c{index}_min",
            f"max({cast}) FILTER (WHERE {valid}) AS c{index}_max",
        ])

    values = query_one(connection, f"SELECT {', '.join(expressions)} FROM {table}")
    result = {"row_count": values["row_count"], "columns": {}, "key_checks": []}
    print(f"  Rows: {result['row_count']:,}", flush=True)
    for index, column in enumerate(columns):
        prefix = f"c{index}_"
        stats = {key[len(prefix):]: value for key, value in values.items() if key.startswith(prefix)}
        stats["intended_type"] = types[column]
        result["columns"][column] = stats
        problems = {key: value for key, value in stats.items()
                    if key in {"missing", "cast_failures", "nonfinite", "fractional", "off_hour"} and value}
        if problems:
            print(f"  {column}: {problems}", flush=True)

    for keys in KEYS[name]:
        print(f"  Checking duplicate keys: {', '.join(keys)} ...", flush=True)
        group = ", ".join(f"nullif(trim({quote_id(key)}), '')" for key in keys)
        duplicates = query_one(connection, f"""
            SELECT count(*) AS duplicate_key_groups,
                   coalesce(sum(n - 1), 0)::BIGINT AS excess_rows
            FROM (SELECT count(*) AS n FROM {table}
                  GROUP BY {group} HAVING count(*) > 1)
        """)
        result["key_checks"].append({"columns": keys, **duplicates})
        print(f"  Duplicate groups: {duplicates['duplicate_key_groups']:,}; "
              f"excess rows: {duplicates['excess_rows']:,}", flush=True)
    return result


def electricity_profile(connection):
    connection.execute("""
        CREATE VIEW electricity AS
        SELECT try_cast(building_id AS BIGINT) AS building_id,
               try_cast(timestamp AS TIMESTAMP) AS timestamp,
               try_cast(meter_reading AS DOUBLE) AS reading
        FROM src_train WHERE try_cast(meter AS DOUBLE) = 0
    """)
    stats = query_one(connection, """
        SELECT count(*) AS row_count,
               count(DISTINCT building_id) AS building_count,
               min(timestamp) AS first_timestamp,
               max(timestamp) AS last_timestamp,
               count(*) FILTER (WHERE reading IS NULL) AS missing_or_unparseable_readings,
               count(*) FILTER (WHERE reading IS NOT NULL AND NOT isfinite(reading)) AS nonfinite_readings,
               count(*) FILTER (WHERE isfinite(reading) AND reading < 0) AS negative_readings,
               count(*) FILTER (WHERE reading = 0) AS zero_readings,
               count(*) FILTER (WHERE isfinite(reading) AND reading >= 0) AS finite_nonnegative_readings,
               min(reading) FILTER (WHERE isfinite(reading) AND reading >= 0) AS min_valid_reading,
               max(reading) FILTER (WHERE isfinite(reading) AND reading >= 0) AS max_valid_reading,
               quantile_cont(reading, [0.25, 0.5, 0.75, 0.95, 0.99])
                   FILTER (WHERE isfinite(reading) AND reading >= 0) AS p25_p50_p75_p95_p99
        FROM electricity
    """)
    stats["units"] = "Original recorded units; not standardized. Do not interpret as uniformly kWh."
    stats["quantile_population"] = "All finite nonnegative electricity readings; exact quantiles; no sample."
    stats["zero_policy"] = "Reported for investigation; zero values are not automatically rejected."
    stats["duplicate_policy"] = "Key duplicates are reported, not removed; conflicting readings require investigation."
    return stats


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path.home() / "electricity-data/incoming/ashrae-v1")
    parser.add_argument("--manifest", type=Path, default=root / "evidence/source_manifest_v1.json")
    parser.add_argument("--output-dir", type=Path, default=root / "evidence")
    parser.add_argument("--memory-limit", default="6GB")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report_path = args.output_dir / f"profile_{run_id}.json"
    report = {
        "run_id": run_id, "status": "running", "population": "All rows in all six manifest-listed CSVs",
        "sampling": "None", "script_sha256": checksum(Path(__file__)),
        "python_version": platform.python_version(), "duckdb_version": duckdb.__version__,
        "memory_limit": args.memory_limit, "threads": args.threads, "files": {},
        "notes": [
            "Missing values include empty strings and whitespace-only fields.",
            "Duplicate keys are compared as trimmed source text, with blanks treated as null.",
            "Raw data is not modified. Report generation is not ETL publication.",
            "Competition test rows have no actual target; submission-template values are not labels.",
            "Timestamp timezone alignment and electricity unit conversion need explicit ETL rules."
        ],
    }
    started = perf_counter()
    try:
        if duckdb.__version__ != "1.4.3":
            raise RuntimeError("Install requirements.txt: this script is tested with DuckDB 1.4.3.")
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        report["source_manifest_sha256"] = checksum(args.manifest)
        report["source_version"] = manifest["source_version"]
        records = manifest["files"]
        if len(records) != 6 or {r["file_name"] for r in records} != set(KEYS):
            raise RuntimeError("Manifest must contain exactly the six ASHRAE CSV files.")
        if {p.name for p in args.source_dir.glob("*.csv")} != set(KEYS):
            raise RuntimeError("Unexpected or missing CSV files in the source folder.")
        print("Verifying source files against the preserved manifest ...", flush=True)
        for record in records:
            path = args.source_dir / record["file_name"]
            if path.stat().st_size != record["size_bytes"] or checksum(path) != record["sha256"]:
                raise RuntimeError(f"Source changed: {path.name}")

        # Temp files remain outside the repository and raw source directory.
        with tempfile.TemporaryDirectory(prefix="profile-", dir=args.source_dir.parent) as temp_dir:
            connection = duckdb.connect(config={
                "memory_limit": args.memory_limit, "threads": str(args.threads),
                "temp_directory": temp_dir, "preserve_insertion_order": "false",
            })
            try:
                for record in records:
                    name = record["file_name"]
                    table = quote_id("src_" + Path(name).stem)
                    path = args.source_dir / name
                    connection.execute(f"""
                        CREATE VIEW {table} AS SELECT * FROM read_csv(
                            {quote_text(path)}, header=true, all_varchar=true,
                            delim=',', strict_mode=true, ignore_errors=false)
                    """)
                    actual_columns = [r[0] for r in connection.execute(f"DESCRIBE {table}").fetchall()]
                    if actual_columns != record["columns"]:
                        raise RuntimeError(f"Column mismatch: {name}")
                    print(f"\nPROFILING: {name}", flush=True)
                    file_start = perf_counter()
                    report["files"][name] = profile_file(connection, name, actual_columns)
                    report["files"][name]["role"] = record["role"]
                    report["files"][name]["duration_seconds"] = round(perf_counter() - file_start, 2)

                report["electricity"] = electricity_profile(connection)
                report["meter_row_counts"] = [dict(zip(["meter", "rows"], row)) for row in
                    connection.execute("SELECT meter, count(*) FROM src_train GROUP BY meter ORDER BY meter").fetchall()]
                report["metadata_coverage"] = query_one(connection, """
                    SELECT count(*) AS training_rows_without_building_metadata
                    FROM src_train t WHERE NOT EXISTS (
                        SELECT 1 FROM src_building_metadata b
                        WHERE try_cast(t.building_id AS BIGINT) = try_cast(b.building_id AS BIGINT))
                """)
                print("\nELECTRICITY SUMMARY (original units):", flush=True)
                print(json.dumps(report["electricity"], indent=2, default=str), flush=True)
                print("\nMETADATA COVERAGE:", report["metadata_coverage"], flush=True)
                report["status"] = "passed"
            finally:
                connection.close()
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        print(f"FAILED: {error}", file=sys.stderr, flush=True)
    finally:
        report["duration_seconds"] = round(perf_counter() - started, 2)
        report["peak_process_rss_mib"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        with report_path.open("x", encoding="utf-8") as stream:
            json.dump(report, stream, indent=2, default=str, allow_nan=False)
            stream.write("\n")
        print(f"\nReport saved: {report_path}", flush=True)
        print(f"Elapsed: {report['duration_seconds']} seconds; peak process RSS: {report['peak_process_rss_mib']} MiB", flush=True)
    if report["status"] != "passed":
        return 1
    print("PASS: full collection profiled; raw files unchanged by this script.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
