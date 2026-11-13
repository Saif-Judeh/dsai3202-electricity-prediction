import csv
import hashlib
import json
from pathlib import Path

source_dir = Path.home() / "electricity-data/incoming/ashrae-v1"
manifest_path = Path("evidence/source_manifest_v1.json")

expected_sizes = {
    "building_metadata.csv": 45527,
    "sample_submission.csv": 447562511,
    "test.csv": 1462461085,
    "train.csv": 678616640,
    "weather_test.csv": 14787908,
    "weather_train.csv": 7450075,
}

actual_names = {p.name for p in source_dir.glob("*.csv")}
if actual_names != set(expected_sizes):
    raise SystemExit("FAILED: unexpected or missing CSV files.")

records = []

for name, expected_size in sorted(expected_sizes.items()):
    path = source_dir / name
    size = path.stat().st_size

    if size != expected_size:
        raise SystemExit(
            f"FAILED: {name} has {size} bytes; expected {expected_size}."
        )

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        columns = next(csv.reader(stream))

    records.append({
        "file_name": name,
        "size_bytes": size,
        "sha256": digest.hexdigest(),
        "columns": columns,
        "role": (
            "submission_template"
            if name == "sample_submission.csv"
            else "source_data"
        ),
    })

    print(f"CHECKED: {name} | {size:,} bytes")

manifest = {
    "dataset": "ASHRAE Great Energy Predictor III",
    "source_url": (
        "https://www.kaggle.com/competitions/"
        "ashrae-energy-prediction/data"
    ),
    "source_version": "ashrae-v1",
    "format": "CSV",
    "ingestion_mode": "batch",
    "refresh_strategy": "Manual refresh into a new source version.",
    "hash_algorithm": "SHA-256",
    "total_collection_bytes": sum(r["size_bytes"] for r in records),
    "source_data_bytes": sum(
        r["size_bytes"] for r in records if r["role"] == "source_data"
    ),
    "files": records,
}

manifest_path.parent.mkdir(parents=True, exist_ok=True)

if manifest_path.exists():
    previous = json.loads(manifest_path.read_text(encoding="utf-8"))
    if previous != manifest:
        raise SystemExit(
            "FAILED: files differ from the existing v1 manifest. "
            "Investigate before creating a new version."
        )
    print("PASS: existing manifest matches; no changes made.")
else:
    with manifest_path.open("x", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    print(f"CREATED: {manifest_path}")

print(f"Collection bytes: {manifest['total_collection_bytes']:,}")
print(f"Source bytes excluding template: {manifest['source_data_bytes']:,}")
