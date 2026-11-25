import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
from catboost import CatBoostRegressor

parser = argparse.ArgumentParser()
parser.add_argument("feature_dir", type=Path)
args = parser.parse_args()

root = Path(__file__).resolve().parents[1]
run_id = datetime.now(timezone.utc).strftime("candidate-%Y%m%dT%H%M%SZ")
out = Path.home() / "electricity-data" / "models" / run_id
out.mkdir(parents=True)

categories = ["building_id", "site_id", "primary_use"]
features = categories + [
    "square_feet", "target_hour", "target_weekday", "target_month",
    "target_is_weekend", "lag_1h_kwh", "lag_24h_kwh", "lag_168h_kwh",
    "lag_1h_missing", "lag_24h_missing", "lag_168h_missing", "lag_1h_is_zero",
]

def predictors(frame):
    result = frame[features].copy()
    for col in categories:
        result[col] = result[col].astype("string").fillna("unknown").astype(str)
    for col in features[len(categories):]:
        result[col] = result[col].astype("float64")
    return result

db = duckdb.connect()
db.execute("SET threads=2")
sample_rule = "hash(building_id, target_time_reported) % 8 = 0"
columns = ", ".join(features)
train = db.execute(f"""
    SELECT {columns}, target_kwh FROM read_parquet(?)
    WHERE {sample_rule}
    ORDER BY building_id, target_time_reported
""", [str(args.feature_dir / "train.parquet")]).df()

params = dict(iterations=300, depth=7, learning_rate=0.12, loss_function="RMSE",
              random_seed=42, thread_count=4, verbose=50, allow_writing_files=False)
model = CatBoostRegressor(**params)
n_train = len(train)
print(f"Training on {n_train:,} January–August readings", flush=True)
model.fit(predictors(train), train["target_kwh"], cat_features=categories)
model.save_model(str(out / "model.cbm"))
del train

val = db.execute(f"""
    SELECT {columns}, target_time_reported, target_kwh, baseline_available
    FROM read_parquet(?)
""", [str(args.feature_dir / "validation.parquet")]).df()
eligible = val["baseline_available"]
baseline = json.loads((root / "evidence/phase2_validation_weekly_baseline.json").read_text())
if len(val) != baseline["total_rows"] or int(eligible.sum()) != baseline["evaluated_rows"]:
    raise ValueError("Validation row counts differ from Adam's benchmark")

baseline_mae = (val.loc[eligible, "target_kwh"] - val.loc[eligible, "lag_168h_kwh"]).abs().mean()
if not np.isclose(baseline_mae, baseline["baseline_mae_kwh"]):
    raise ValueError("Weekly baseline MAE differs from Adam's benchmark")

pred = model.predict(predictors(val), thread_count=4)
if not np.isfinite(pred).all():
    raise ValueError("Model produced a nonfinite prediction")
mae = float(np.abs(val.loc[eligible, "target_kwh"] - pred[eligible]).mean())

handoff = val[["building_id", "target_time_reported"]].copy()
handoff["prediction_kwh"] = pred
db.from_df(handoff).write_parquet(str(out / "validation_predictions.parquet"))

report = dict(
    run_id=run_id, code_commit=subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
    feature_version=args.feature_dir.name, predictors=features,
    categorical_features=categories, parameters=params,
    training_sample_rule=sample_rule, training_rows=n_train,
    validation_rows=len(val), evaluated_rows=int(eligible.sum()),
    weekly_baseline_mae_kwh=float(baseline_mae), candidate_mae_kwh=mae,
    improvement_percent=100 * (1 - mae / float(baseline_mae)),
    output_directory=str(out),
)
summary = json.dumps(report, indent=2)
(root / "evidence" / f"{run_id}.json").write_text(summary + "\n")
print(summary)
