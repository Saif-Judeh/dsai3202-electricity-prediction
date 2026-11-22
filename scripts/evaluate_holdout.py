import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
from catboost import CatBoostRegressor

root = Path(__file__).resolve().parents[1]
data = Path.home() / "electricity-data"
run_id = "candidate-20261122T164221Z"
training = json.loads((root / "evidence" / f"{run_id}.json").read_text())
model_path = data / "models" / run_id / "model.cbm"
test_path = data / "features" / training["feature_version"] / "test.parquet"

model = CatBoostRegressor()
model.load_model(str(model_path))
features = training["predictors"]
categories = training["categorical_features"]
if model.feature_names_ != features:
    raise ValueError("Saved model feature names differ from the training report")

with duckdb.connect() as db:
    frame = db.read_parquet(str(test_path)).df()

times = frame["target_time_reported"]
if not ((times >= "2016-11-01") & (times < "2017-01-01")).all():
    raise ValueError("Unexpected date outside the November–December holdout")
if frame.duplicated(["building_id", "target_time_reported"]).any():
    raise ValueError("Duplicate building and target timestamp")

eligible = frame["baseline_available"]
if not eligible.equals(frame["lag_168h_kwh"].notna()):
    raise ValueError("Weekly baseline flag disagrees with the lag column")
if (len(frame), int(eligible.sum())) != (2023232, 1989186):
    raise ValueError("Holdout row counts differ from the Phase 1 report")

# Apply the same input conversions used in train_candidate.py.
x = frame[features].copy()
for col in categories:
    x[col] = x[col].astype("string").fillna("unknown").astype(str)
for col in features:
    if col not in categories:
        x[col] = x[col].astype("float64")

frame["prediction_kwh"] = model.predict(x, thread_count=4)
if not np.isfinite(frame["prediction_kwh"]).all():
    raise ValueError("Nonfinite prediction detected")

paired = frame.loc[eligible].copy()
paired["weekly_error"] = (paired["target_kwh"] - paired["lag_168h_kwh"]).abs()
paired["model_error"] = (paired["target_kwh"] - paired["prediction_kwh"]).abs()
weekly = float(paired["weekly_error"].mean())
candidate = float(paired["model_error"].mean())
improvement = 100 * (1 - candidate / weekly)

by_site = []
for site, group in paired.groupby("site_id", sort=True):
    by_site.append({
        "site_id": int(site),
        "rows": len(group),
        "weekly_mae_kwh": float(group["weekly_error"].mean()),
        "candidate_mae_kwh": float(group["model_error"].mean()),
    })

report = {
    "training_run": run_id,
    "training_commit": training["code_commit"],
    "feature_version": training["feature_version"],
    "model_sha256": hashlib.sha256(model_path.read_bytes()).hexdigest(),
    "holdout_sha256": hashlib.sha256(test_path.read_bytes()).hexdigest(),
    "period": "November–December 2016",
    "total_rows": len(frame),
    "evaluated_rows": len(paired),
    "rows_without_weekly_history": len(frame) - len(paired),
    "weekly_mae_kwh": weekly,
    "candidate_mae_kwh": candidate,
    "improvement_percent": improvement,
    "meets_10_percent_target": improvement >= 10,
    "by_site": by_site,
}

output = root / "evidence" / f"holdout_{run_id}.json"
output.write_text(json.dumps(report, indent=2) + "\n")
print(f"Evaluated rows:   {len(paired):,}")
print(f"Weekly baseline:  {weekly:.4f} kWh MAE")
print(f"CatBoost:         {candidate:.4f} kWh MAE")
print(f"Error reduction:  {improvement:.2f}%")
print(f"Meets 10% target: {improvement >= 10}")
print(f"Evidence:         {output}")
