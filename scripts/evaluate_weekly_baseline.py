import json
import sys
from pathlib import Path

import duckdb

source = Path(sys.argv[1])
with duckdb.connect() as con:
    con.read_parquet(str(source)).create_view("validation")

    total, eligible, mae = con.execute("""
        SELECT count(*),
               count(*) FILTER (WHERE baseline_available),
               avg(abs(target_kwh - lag_168h_kwh))
                   FILTER (WHERE baseline_available)
        FROM validation
    """).fetchone()

    if (total, eligible) != (2045185, 2038958):
        raise SystemExit(f"Unexpected validation counts: {total=}, {eligible=}")

    sites = con.execute("""
        SELECT site_id, count(*) AS readings,
               avg(abs(target_kwh - lag_168h_kwh)) AS mae_kwh
        FROM validation
        WHERE baseline_available
        GROUP BY site_id
        ORDER BY mae_kwh DESC, site_id
    """).fetchall()

print(json.dumps({
    "feature_version": "features-v1-4416c69c061b464e",
    "split": "September–October 2016 validation",
    "total_rows": total,
    "evaluated_rows": eligible,
    "missing_weekly_history": total - eligible,
    "baseline_mae_kwh": mae,
    "mae_for_10_percent_improvement_kwh": 0.9 * mae,
    "by_site": [
        {"site_id": site, "evaluated_rows": rows, "mae_kwh": error}
        for site, rows, error in sites
    ]
}, indent=2))
