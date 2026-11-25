import json
import math
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostRegressor

CATEGORIES = {"building_id", "site_id", "primary_use"}
FLAGS = {
    "target_is_weekend", "lag_1h_missing", "lag_24h_missing",
    "lag_168h_missing", "lag_1h_is_zero",
}
NULLABLE = {"lag_1h_kwh", "lag_24h_kwh", "lag_168h_kwh", "lag_1h_is_zero"}


def init():
    global model, features
    location = Path(os.environ["AZUREML_MODEL_DIR"])
    paths = [location] if location.is_file() else list(location.rglob("model.cbm"))
    if len(paths) != 1:
        raise RuntimeError(f"Expected one model.cbm under {location}, found {len(paths)}")
    model = CatBoostRegressor()
    model.load_model(str(paths[0]))
    features = model.feature_names_


def validate(record):
    if not isinstance(record, dict) or set(record) != set(features):
        raise ValueError("Each record must contain exactly the model's 15 feature fields.")

    for name in features:
        value = record[name]
        if value is None and name in NULLABLE:
            continue
        if name == "primary_use":
            valid = isinstance(value, str) and bool(value.strip())
        elif name in FLAGS:
            valid = type(value) is bool
        elif name in {"building_id", "site_id", "target_hour", "target_weekday", "target_month"}:
            valid = type(value) is int
        else:
            valid = type(value) in (int, float) and math.isfinite(value)
        if not valid:
            raise ValueError(f"Invalid value for {name}.")

    for hours in (1, 24, 168):
        if record[f"lag_{hours}h_missing"] != (record[f"lag_{hours}h_kwh"] is None):
            raise ValueError(f"The {hours}-hour lag and its missing flag disagree.")
    lag = record["lag_1h_kwh"]
    if record["lag_1h_is_zero"] != (None if lag is None else lag == 0):
        raise ValueError("lag_1h_is_zero disagrees with lag_1h_kwh.")


def run(raw_data):
    payload = json.loads(raw_data)
    if not isinstance(payload, dict) or set(payload) != {"records"}:
        raise ValueError("Expected a JSON object with a records field.")
    records = payload["records"]
    if not isinstance(records, list) or not records:
        raise ValueError("records must be a nonempty list.")
    for record in records:
        validate(record)

    frame = pd.DataFrame(records, columns=features)
    for name in features:
        if name in CATEGORIES:
            frame[name] = frame[name].astype("string").fillna("unknown").astype(str)
        else:
            frame[name] = frame[name].astype("float64")
    return {"predictions_kwh": model.predict(frame, thread_count=1).tolist()}
