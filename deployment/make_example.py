import json
import math
from pathlib import Path

import duckdb
import pandas as pd
from catboost import CatBoostRegressor

root = Path(__file__).resolve().parents[1]
data = Path.home() / "electricity-data"
run = "candidate-20261122T164221Z"
training = json.loads((root / "evidence" / f"{run}.json").read_text())
features = training["predictors"]
categories = training["categorical_features"]
model_dir = data / "models" / run
validation = data / "features" / training["feature_version"] / "validation.parquet"

with duckdb.connect() as db:
    db.read_parquet(str(validation)).create_view("v")
    db.read_parquet(str(model_dir / "validation_predictions.parquet")).create_view("p")
    row = db.execute(f"""
        SELECT {", ".join("v." + col for col in features)},
               v.target_time_reported, p.prediction_kwh
        FROM v JOIN p USING (building_id, target_time_reported)
        WHERE v.site_id = 7 AND v.baseline_available
          AND v.target_time_reported < TIMESTAMP '2016-10-01'
          AND v.lag_1h_kwh IS NOT NULL AND v.lag_24h_kwh IS NOT NULL
        ORDER BY v.target_time_reported, v.building_id
        LIMIT 1
    """).fetchone()

if row is None:
    raise SystemExit("No matching validation example found.")

record = dict(zip(features, row[:-2]))
timestamp, original_prediction = row[-2:]
frame = pd.DataFrame([record], columns=features)
for col in categories:
    frame[col] = frame[col].astype("string").fillna("unknown").astype(str)
for col in features:
    if col not in categories:
        frame[col] = frame[col].astype("float64")

model = CatBoostRegressor()
model.load_model(str(model_dir / "model.cbm"))
prediction = float(model.predict(frame)[0])
if not math.isclose(prediction, original_prediction, rel_tol=1e-9, abs_tol=1e-8):
    raise SystemExit("Reloaded model disagrees with the saved validation prediction.")

examples = root / "deployment" / "examples"
request = {"records": [record]}
reference = {
    "model": "electricity-catboost:1",
    "validation_timestamp": str(timestamp),
    "predictions_kwh": [prediction],
    "relative_tolerance": 1e-9,
    "absolute_tolerance_kwh": 1e-8,
}
for filename, content in [("valid_request.json", request),
                          ("offline_reference.json", reference)]:
    (examples / filename).write_text(json.dumps(content, indent=2, allow_nan=False) + "\n")

print("PASS: saved model reproduced the original validation prediction")
print(json.dumps(reference, indent=2))
