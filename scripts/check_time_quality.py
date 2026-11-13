import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
from publish_processed import ROOT, validate_local, sha256

source = Path.home() / "electricity-data/processed/etl-v1-c4c56f89d676b969"
manifest, _ = validate_local(source, ROOT)

con = duckdb.connect()
con.execute("SET threads = 2")
con.execute("SET memory_limit = '6GB'")

con.read_parquet(str(source / "electricity.parquet")).create_view("electricity")
con.execute("""
CREATE TABLE training AS
SELECT building_id, site_id, timestamp_reported, meter_reading_kwh
FROM electricity
WHERE timestamp_reported >= TIMESTAMP '2016-01-01'
  AND timestamp_reported < TIMESTAMP '2016-09-01'
""")

queries = {
    "zeros_by_site": """
        SELECT site_id,
               count(*) AS readings,
               count(DISTINCT building_id) AS buildings,
               count(*) FILTER (WHERE meter_reading_kwh=0) AS zero_readings,
               round(100.0 * count(*) FILTER (WHERE meter_reading_kwh=0)
                     / count(*), 2) AS zero_percent
        FROM training
        GROUP BY site_id
        ORDER BY zero_readings DESC, site_id
    """,
    "time_gaps": """
        WITH ordered AS (
            SELECT building_id, timestamp_reported,
                   lag(timestamp_reported) OVER (
                       PARTITION BY building_id ORDER BY timestamp_reported
                   ) AS previous_time
            FROM training
        ), gaps AS (
            SELECT building_id,
                   date_diff('hour', previous_time, timestamp_reported)-1
                       AS missing_hours
            FROM ordered
            WHERE timestamp_reported > previous_time + INTERVAL '1 hour'
        )
        SELECT count(*) AS gap_events,
               count(DISTINCT building_id) AS buildings_with_gaps,
               coalesce(sum(missing_hours),0) AS missing_hours_inside_observed_spans,
               coalesce(max(missing_hours),0) AS longest_gap_hours
        FROM gaps
    """
}

report = {
    "scope": "All electricity observations January-August 2016; no sample",
    "etl_identity": manifest["identity"],
    "output_manifest_sha256": sha256(source / "output_manifest.json"),
    "script_sha256": sha256(__file__),
    "results": {}
}

for name, sql in queries.items():
    result = con.execute(sql)
    columns = [column[0] for column in result.description]
    report["results"][name] = [
        dict(zip(columns, row)) for row in result.fetchall()
    ]

con.execute("""
CREATE TABLE zero_runs AS
WITH numbered AS (
    SELECT building_id, site_id, timestamp_reported,
           timestamp_reported
             - row_number() OVER (
                 PARTITION BY building_id ORDER BY timestamp_reported
               ) * INTERVAL '1 hour' AS run_group
    FROM training
    WHERE meter_reading_kwh=0
)
SELECT building_id, site_id,
       min(timestamp_reported) AS first_timestamp,
       max(timestamp_reported) AS last_timestamp,
       count(*) AS consecutive_zero_hours
FROM numbered
GROUP BY building_id, site_id, run_group
""")

for name, sql in {
    "zero_run_summary": """
        SELECT count(*) AS zero_runs,
               count(*) FILTER (WHERE consecutive_zero_hours >= 24)
                   AS runs_at_least_24_hours,
               count(DISTINCT building_id)
                   FILTER (WHERE consecutive_zero_hours >= 24)
                   AS buildings_with_long_zero_runs,
               coalesce(max(consecutive_zero_hours),0) AS longest_zero_run_hours
        FROM zero_runs
    """,
    "longest_zero_runs": """
        SELECT * FROM zero_runs
        ORDER BY consecutive_zero_hours DESC, building_id, first_timestamp
        LIMIT 10
    """
}.items():
    result = con.execute(sql)
    columns = [column[0] for column in result.description]
    report["results"][name] = [
        dict(zip(columns, row)) for row in result.fetchall()
    ]

con.close()

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
output = ROOT / "evidence" / f"time_quality_{run_id}-{uuid4().hex[:8]}.json"
with output.open("x", encoding="utf-8") as stream:
    json.dump(report, stream, indent=2, default=str)

for name, records in report["results"].items():
    print(f"\n{name.upper()}")
    for record in records:
        print(record)

print(f"\nEvidence saved: {output}")
