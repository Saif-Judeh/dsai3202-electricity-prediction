#!/usr/bin/env python3
"""Reproducible training-only EDA; preserves inputs and saves exact query outputs."""
import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb
from publish_processed import ROOT, validate_local, sha256, require
from build_features import rows, write_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--etl-dir", type=Path, required=True)
    args = parser.parse_args()
    source = args.etl_dir.expanduser().resolve()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    report = {"status": "running", "run_id": run_id, "script_sha256": sha256(__file__),
              "scope": "All training-period electricity observations; no sample"}
    started = perf_counter()
    code = 1
    try:
        manifest, inputs = validate_local(source, ROOT)
        config = json.loads((ROOT / "config/features_v1.json").read_text())
        require(duckdb.__version__ == config["required_duckdb_version"], "DuckDB version mismatch")
        report.update(processed_identity=manifest["identity"],
                      source_manifest_sha256=sha256(source / "output_manifest.json"),
                      split_config_sha256=sha256(ROOT / "config/features_v1.json"))
        with tempfile.TemporaryDirectory(prefix="electricity-eda-", dir=source.parent) as temp:
            db = duckdb.connect(str(Path(temp) / "work.duckdb"))
            try:
                db.execute("SET threads=2")
                db.execute("SET memory_limit='6GB'")
                db.read_parquet(str(source / "electricity.parquet")).create_view("electricity")
                db.read_parquet(str(source / "building_metadata.parquet")).create_view("metadata")
                db.execute("CREATE TABLE training AS SELECT building_id,site_id,timestamp_reported,meter_reading_kwh "
                           "FROM electricity WHERE timestamp_reported>=?::TIMESTAMP AND timestamp_reported<?::TIMESTAMP",
                           [config["train_start"], config["validation_start"]])
                queries = {
                    "distribution": """SELECT count(*) AS rows,count(DISTINCT building_id) AS buildings,
                        count(*) FILTER (WHERE meter_reading_kwh=0) AS zero_rows,
                        min(meter_reading_kwh) AS min_kwh,max(meter_reading_kwh) AS max_kwh,
                        quantile_cont(meter_reading_kwh,[0.25,0.5,0.75,0.95,0.99]) AS p25_p50_p75_p95_p99 FROM training""",
                    "monthly": """SELECT strftime(timestamp_reported,'%Y-%m') AS month,count(*) AS readings,
                        count(DISTINCT building_id) AS buildings,round(avg(meter_reading_kwh),2) AS mean_kwh,
                        round(median(meter_reading_kwh),2) AS median_kwh,
                        round(100.0*count(*) FILTER (WHERE meter_reading_kwh=0)/count(*),2) AS zero_percent
                        FROM training GROUP BY 1 ORDER BY 1""",
                    "zeros_by_site": """SELECT site_id,count(*) AS readings,
                        count(*) FILTER (WHERE meter_reading_kwh=0) AS zero_readings,
                        round(100.0*count(*) FILTER (WHERE meter_reading_kwh=0)/count(*),2) AS zero_percent
                        FROM training GROUP BY 1 ORDER BY zero_readings DESC,site_id""",
                    "time_gaps": """WITH ordered AS (SELECT building_id,timestamp_reported,
                        lag(timestamp_reported) OVER(PARTITION BY building_id ORDER BY timestamp_reported) AS previous_time
                        FROM training), gaps AS (SELECT building_id,date_diff('hour',previous_time,timestamp_reported)-1 AS hours
                        FROM ordered WHERE timestamp_reported>previous_time+INTERVAL '1 hour')
                        SELECT count(*) AS gap_events,count(DISTINCT building_id) AS buildings_with_gaps,
                        coalesce(sum(hours),0) AS missing_hours_inside_observed_spans,
                        coalesce(max(hours),0) AS longest_gap_hours FROM gaps""",
                    "largest_readings": """SELECT building_id,site_id,timestamp_reported,meter_reading_kwh
                        FROM training ORDER BY meter_reading_kwh DESC,building_id,timestamp_reported LIMIT 20""",
                    "building_area_correlation": """WITH means AS (SELECT building_id,avg(meter_reading_kwh) AS mean_kwh
                        FROM training GROUP BY 1) SELECT count(*) AS buildings,
                        corr(means.mean_kwh,m.square_feet) AS mean_kwh_vs_area,
                        corr(ln(1+means.mean_kwh),ln(m.square_feet)) AS log1p_mean_kwh_vs_log_area
                        FROM means JOIN metadata m USING(building_id)"""
                }
                results = {}
                for name, query in queries.items():
                    print("EDA: " + name, flush=True)
                    results[name] = rows(db, query)
                db.execute("""CREATE TABLE zero_runs AS WITH numbered AS (
                    SELECT building_id,site_id,timestamp_reported,
                    timestamp_reported-row_number() OVER(PARTITION BY building_id ORDER BY timestamp_reported)*INTERVAL '1 hour' AS grp
                    FROM training WHERE meter_reading_kwh=0)
                    SELECT building_id,site_id,min(timestamp_reported) AS first_timestamp,max(timestamp_reported) AS last_timestamp,
                    count(*) AS zero_hours FROM numbered GROUP BY building_id,site_id,grp""")
                results["zero_run_summary"] = rows(db, """SELECT count(*) AS runs,
                    count(*) FILTER(WHERE zero_hours>=24) AS runs_at_least_24_hours,
                    count(DISTINCT building_id) FILTER(WHERE zero_hours>=24) AS buildings_with_long_runs,
                    coalesce(max(zero_hours),0) AS longest_run_hours FROM zero_runs""")
                results["longest_zero_runs"] = rows(db, "SELECT * FROM zero_runs ORDER BY zero_hours DESC,building_id,first_timestamp LIMIT 10")
                report["results"] = results
            finally:
                db.close()
        for item in inputs:
            require(sha256(source / item["file_name"]) == item["sha256"], "EDA input changed during execution")
        report["status"] = "passed"
        print("Building-level correlations: " + json.dumps(results["building_area_correlation"]), flush=True)
        print("PASS: training-only EDA saved; processed inputs unchanged.", flush=True)
        code = 0
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, error=str(error))
        print("FAILED: " + str(error), flush=True)
    finally:
        report["elapsed_seconds"] = round(perf_counter()-started,3)
        path = ROOT / "evidence" / f"eda_summary_{run_id}.json"
        write_json(path, report)
        print("Evidence: " + str(path), flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
