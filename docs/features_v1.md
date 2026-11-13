# Initial electricity features v1

This step builds a small initial feature set for Phase 1. It does not train a model,
evaluate the hypothesis, or upload feature files to Azure Blob Storage. It runs on
the existing Azure VM and needs only the installed DuckDB 1.4.3 environment and the
previous ETL/publication scripts. No managed feature store is required for this step.

## Prediction timing and splits

For a row with target timestamp T, the prediction origin is T minus one hour. The
target is the electricity reading in kWh at T. We assume a reading is completed and
available at its reported timestamp. Thus the T-1 hour observation is available at
the origin. This is an explicit offline assumption; the source has not established
sensor delivery latency or whether timestamps label the start or end of intervals.
An operational model must validate that convention and shift history if needed.
Timestamp timezone interpretation is not changed or asserted by this pipeline.

Splits are based on TARGET timestamps, with exclusive upper boundaries:

| Partition | Target period | Purpose |
|---|---|---|
| Train | January 1 through August 31, 2016 | Fit models and learned preprocessing |
| Validation | September 1 through October 31, 2016 | Model/parameter choices |
| Test | November 1 through December 31, 2016 | Final held-out evaluation |

The intended evaluation is rolling one-hour-ahead prediction. Earlier validation or
test readings may be used as observed history for later forecasts, but they are not
used to fit preprocessing or model parameters. This is not a single forecast made
in August for every remaining hour of the year. Future observations are never used
for an earlier forecast. This split evaluates later seasons in one year and does
not establish generalization to a new year or to unseen buildings.

## Predictor definitions

The saved feature_catalog.json records each column's SQL type, definition, source,
availability and role. Its model_feature_allowlist is the definitive predictor list.

| Predictor | Definition and source | Availability / missing policy |
|---|---|---|
| building_id, site_id | Building/site identifiers; categorical inputs | Known IDs; do not interpret as ordered measurements |
| primary_use | Use category from building metadata | Static metadata assumed known at origin |
| square_feet | Building floor area from metadata | Static metadata assumed known at origin |
| target_hour | Reported hour, 0–23 | Calendar known in advance |
| target_weekday | ISO weekday, Monday=1 to Sunday=7 | Calendar known in advance |
| target_month | Month, 1–12 | Calendar known in advance |
| target_is_weekend | Saturday/Sunday flag | Generic calendar indicator, not a verified local work schedule |
| lag_1h_kwh | Same building's exact T-1 hour electricity | Available at origin under the timing assumption; NULL if absent |
| lag_24h_kwh | Same building's exact T-24 hour electricity | Historical; NULL if absent |
| lag_168h_kwh | Same building's exact T-168 hour electricity | Historical; also the planned weekly baseline; NULL if absent |
| lag_1h_missing, lag_24h_missing, lag_168h_missing | Whether the corresponding exact-time reading is absent | Computed from historical coverage |
| lag_1h_is_zero | Whether the available previous-hour reading equals zero | NULL when the previous hour is missing; uses no target/future value |

Rationale: the calendar represents daily/weekly/seasonal patterns, metadata represents
building differences, and exact historical readings capture recent and recurring
consumption. Year built and floor count are omitted initially because the full profile
found substantial missingness. Temperature is deferred until timestamp alignment and
forecast-time availability are justified. This initial set does not claim the full
temperature-inclusive hypothesis has already been tested.

## Leakage and quality rules

The target_kwh label, split, baseline_available, and timestamp audit fields are NOT
predictors. In particular, no target-is-zero flag or future-derived zero-run duration
is included. Long zero runs identified retrospectively in EDA are diagnostic findings,
not features available at the beginning of those runs.

All history uses exact timestamp equality within the same building. No row-offset lag,
forward fill, interpolation, imputation, scaling or encoding is fitted here. NULLs stay
NULL with missing indicators. Any later learned preprocessing must fit on training
data only and be reused unchanged for validation/test. Model inputs must select the
explicit allowlist, not all numeric columns. Unknown categories/buildings need a
documented policy in the model stage.

All 12,060,910 observed electricity targets should be preserved in the real build;
rows with missing history are not dropped. A unique building/target-time key, unchanged
row count, metadata coverage, valid target and past-only history checks are required
before any final feature version is published. Staging is atomically renamed after
the three Parquet outputs are written and their row counts verified. An unchanged
rerun verifies hashes and skips; conflicting existing bytes are refused.

The weekly baseline is lag_168h_kwh. Missing baseline predictions remain missing,
not zero. Later headline MAE comparison must use the identical baseline_available
rows for both models and report how many rows were excluded. Any all-row model metric
must be reported separately. No MAE or success of the 10% hypothesis is claimed here.

Zeros remain observed values, not automatically missing values or sensor faults.
The known concentration and long runs are unresolved quality risks. Any future
exclusion policy requires a documented, training-derived rationale and an immutable
new version; it must not be chosen to improve held-out scores. Report sensitivity
to suspicious observations before making a scientific claim about the hypothesis.

## Outputs and evidence

Default directory: ~/electricity-data/features/features-v1-<identity hash>/.
Contains train.parquet, validation.parquet, test.parquet, feature_catalog.json and
feature_manifest.json. The manifest binds the processed input, feature code/config,
validation helper and DuckDB version to output hashes, schema/catalog and row counts.
Reports are written to evidence/features_<run_id>.json. The builder also reports
training-only pairwise Pearson correlations between the target and the three lags,
with pair counts. These are descriptive autocorrelations, not independent-sample
significance tests, causal claims, or a substitute for held-out MAE.

## Run on the VM

```bash
cd ~/electricity-project
.venv/bin/python scripts/test_features.py
.venv/bin/python scripts/build_features.py --etl-dir ~/electricity-data/processed/etl-v1-c4c56f89d676b969
.venv/bin/python scripts/build_features.py --etl-dir ~/electricity-data/processed/etl-v1-c4c56f89d676b969
```

Run the next command only after the previous one passes. The nine synthetic tests
exercise real CSV-to-ETL-to-feature Parquet, exact hour/day/week offsets, missing gaps,
zero preservation, building isolation, chronological boundaries, availability audits,
predictor allowlisting, invariance to current/future target changes, safe reruns, and
changed-input/output rejection. They use temporary local fixtures, not production
data or Azure API calls. Their outcome is saved in evidence/feature_test_<run_id>.json.

## Resource use and attribution

The job uses two DuckDB threads and a 6 GB database memory budget, with a disk-backed
work database and spill space on the existing VM. Process RSS may exceed that budget.
ZSTD Parquet outputs are outside Git; temporary work files are cleaned after the job.
Record actual real-data time, memory and output size from the run report. These scripts
create no new Azure service. Code and documentation were generated with ChatGPT
assistance; review, execute and explain them, and disclose AI use in the final README.
