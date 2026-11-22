from pathlib import Path
import duckdb

home = Path.home()
db = duckdb.connect()
db.read_parquet(str(home / "validation.parquet")).create_view("actual")
db.read_parquet(str(home / "validation_predictions.parquet")).create_view("pred")

for table in ("actual", "pred"):
    rows, keys = db.execute(f"""
        SELECT count(*), count(DISTINCT (building_id, target_time_reported))
        FROM {table}
    """).fetchone()
    if rows != 2045185 or keys != rows:
        raise SystemExit(f"Check {table}: {rows} rows, {keys} distinct keys")

db.execute("""
    CREATE TEMP TABLE comparison AS
    SELECT a.building_id, a.site_id, a.target_time_reported, a.target_kwh,
           a.lag_168h_kwh, p.prediction_kwh, a.baseline_available,
           abs(a.target_kwh - a.lag_168h_kwh) AS weekly_error,
           abs(a.target_kwh - p.prediction_kwh) AS model_error
    FROM actual a
    JOIN pred p USING (building_id, target_time_reported)
""")

matched, eligible, valid = db.execute("""
    SELECT count(*), count(*) FILTER (WHERE baseline_available),
           count(*) FILTER (WHERE isfinite(prediction_kwh))
    FROM comparison
""").fetchone()
if (matched, eligible, valid) != (2045185, 2038958, 2045185):
    raise SystemExit(f"Check join or predictions: {matched=}, {eligible=}, {valid=}")

print("Validation comparison — errors in kWh")
db.sql("""
    SELECT CASE WHEN grouping(site_id) = 1 THEN 'ALL'
                ELSE CAST(site_id AS VARCHAR) END AS site,
           count(*) AS readings,
           round(avg(weekly_error), 3) AS weekly_mae,
           round(avg(model_error), 3) AS catboost_mae,
           round(100 * (1 - avg(model_error) / nullif(avg(weekly_error), 0)), 2)
               AS improvement_pct
    FROM comparison
    WHERE baseline_available
    GROUP BY GROUPING SETS ((), (site_id))
    ORDER BY grouping(site_id) DESC, site_id
""").show(max_rows=20)
