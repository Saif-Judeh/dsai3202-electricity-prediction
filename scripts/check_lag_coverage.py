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
SELECT building_id, timestamp_reported, meter_reading_kwh
FROM electricity
WHERE timestamp_reported >= TIMESTAMP '2016-01-01'
  AND timestamp_reported < TIMESTAMP '2016-09-01'
""")

results = []

for hours in (1, 24, 168):
    cursor = con.execute(f"""
        SELECT
            count(*) AS target_rows,
            count(p.building_id) AS available_history_rows,
            count(*) - count(p.building_id) AS missing_history_rows,
            round(100.0 * count(p.building_id) / count(*), 2)
                AS available_percent,
            count(*) FILTER (WHERE p.meter_reading_kwh = 0)
                AS available_history_that_is_zero
        FROM training t
        LEFT JOIN training p
          ON p.building_id = t.building_id
         AND p.timestamp_reported =
             t.timestamp_reported - INTERVAL '{hours} hours'
    """)
    columns = [column[0] for column in cursor.description]
    result = {"hours_before_target": hours}
    result.update(dict(zip(columns, cursor.fetchone())))
    results.append(result)
    print(result, flush=True)

con.close()

report = {
    "scope": "January-August 2016; all training readings; no sample",
    "definition": "Same building, exact timestamp offset; zeros count as available",
    "etl_identity": manifest["identity"],
    "output_manifest_sha256": sha256(source / "output_manifest.json"),
    "script_sha256": sha256(__file__),
    "results": results
}

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
path = ROOT / "evidence" / f"lag_coverage_{run_id}-{uuid4().hex[:8]}.json"
with path.open("x", encoding="utf-8") as stream:
    json.dump(report, stream, indent=2)

print(f"Evidence saved: {path}")
