# Electricity ETL v1

This step creates validated, versioned Parquet tables on the existing Azure VM.
It does not yet upload derived tables to Azure Blob Storage, create model features,
fit a model, or complete Phase 1. Raw Azure blobs remain under raw/ashrae-v1/.

## Inputs and scope

All six local CSV files are checked against the preserved source manifest on every
run, including reruns. The full six-file collection has already been ingested and
profiled. This ETL transforms all training rows, building metadata, and training
weather. The unlabeled test files and submission template remain preserved raw;
they are not treated as observed training labels. No production data is sampled.

## Quality rules and accounting

Each table reports input, accepted, rejected, and excluded counts. For training:
input = accepted electricity + rejected rows + excluded non-electricity rows.
Valid meter codes 1, 2, and 3 are excluded as outside the electricity task. Missing,
fractional, or unrecognized meter codes are rejected. IDs require integer text;
fractional IDs are not rounded. Electricity must have a nonnegative building ID,
a valid whole-hour timestamp within 2016, a finite nonnegative reading, and exactly
one matching metadata row. All members of duplicate electricity keys are rejected;
the script never arbitrarily selects one reading. A row with several issues receives
the first applicable rejection reason, so reasons do not double-count rows.

Building metadata requires unique nonnegative building IDs, nonnegative site IDs,
nonblank primary use, and finite positive square footage. Year built and floor count
may be missing; present values must parse as integers, and floor count cannot be
negative. Weather requires unique site/hour keys, nonnegative site IDs, whole-hour
2016 timestamps, and numeric finite values when optional measurements are present.
Missing weather remains NULL. Physical range/sensor-quality investigations are
still required; passing these gates is not proof every measurement is plausible.

The measured source had no key/type/electricity-label failures. This immutable
version therefore allows zero rejected rows. New failures stop publication;
adjusting that policy requires a reviewed code/configuration version. Gates are
fail-fast: a metadata failure prevents later joins, so subsequent tables are not
reported as checked. Rejected rows from the failing table are saved under
processed/_rejected/<run_id>/ for inspection, separate from published versions.

## Units, missing values, and time

Site 0 electricity is recorded in kBTU, as confirmed by the competition organizer:
https://www.kaggle.com/c/ashrae-energy-prediction/discussion/119261

Only site 0 electricity is multiplied by 0.29307107 to obtain kWh. This approximates
the International Table Btu conversion (1000 * 1055.056 / 3,600,000); the dataset
announcement supplies a rounded factor of 0.2931. Original readings and original
units are preserved alongside the converted target. Reference:
https://www.nist.gov/pml/special-publication-811/nist-guide-si-appendix-b-conversion-factors/nist-guide-si-appendix-b9

Zeros are retained and flagged, not automatically declared errors. Positive outliers
are retained for later investigation. No imputation, scaling, clipping, or statistical
parameters are fitted. Optional year/floor/weather NULLs remain NULL. Weather values
including precipitation sentinel -1 remain as reported and are not yet features.

Timestamps are named timestamp_reported and preserved without timezone assertions.
Weather is deliberately separate: site-specific weather/meter alignment is unresolved.
No unverified timezone offsets or contemporaneous weather joins are introduced.
Later temperature features must use information available at forecast time; fitting
imputation or any learned transformations must use training data only. Historical
reading features must use exact time offsets and account for gaps, not assume that
the previous row always means the previous hour. Sensor outages, unusually long zero
runs, time gaps, and site 0 early-period anomalies remain EDA questions.

## Outputs, reruns, and lineage

Default destination: ~/electricity-data/processed/etl-v1-<identity hash>/.
Files: electricity.parquet, building_metadata.parquet, weather_train.parquet,
catalog.json, output_manifest.json. The catalog lists schema, keys, source tables,
row counts, and missing/unit/time policies. The manifest binds each output SHA-256
and size to source hashes, configuration, ETL code, and DuckDB version.

The build uses a staging directory on the same filesystem. Quality gates precede
cleaned output generation. Parquet row counts are rechecked before the manifest is
written and the completed directory is atomically renamed. A local lock prevents
concurrent writers. Existing identical versions are hash-verified and skipped;
changed or incomplete published versions are refused, never silently overwritten.
An interrupted process can leave .staging-* data; it is not a published version.
No external service should consume staging or _rejected directories. Outputs become
eligible for the later Azure upload step only when their manifest and hashes pass.

Reports in evidence/etl_*.json record success/failure, source/code/config identity,
quality results, action, runtime, and peak process RSS. Evidence JSON and code can
be committed; Parquet and large source files remain outside Git. File hashes detect
accidental changes; they are not a signed adversarial-tampering protection scheme.

## Resource choice

DuckDB 1.4.3 uses two threads and a 6 GB database memory budget on the existing
4-vCPU, 16-GiB VM. The budget is not an absolute process RSS cap. A disk-backed work
database and spill space are removed after execution. ZSTD Parquet reduces derived
storage size relative to CSV. Measure actual duration, RSS, output size, and free
disk space from the VM run; no cost or performance savings are claimed in advance.
No new Azure service or paid resource is created by these scripts.

## Run and validation

From ~/electricity-project, with the existing virtual environment:

```bash
.venv/bin/python scripts/test_etl_quality.py
.venv/bin/python scripts/run_etl.py
.venv/bin/python scripts/run_etl.py
```

The test runner uses tiny synthetic six-file fixtures under a temporary directory.
It checks valid conversion, zeros and NULLs, row accounting, unchanged reruns, negative
readings, conflicting duplicate keys, fractional identifiers, malformed weather,
duplicate metadata, changed source files, and changed published files. Bad-quality
fixtures have matching manifests so their failure demonstrates quality gates, not
only checksum checks. It verifies publication is blocked and the earlier successful
version is unchanged. Temporary test data is removed; all nine checks and their ETL
reports are retained in evidence/quality_test_*.json. Paths embedded in those reports
refer to intentionally deleted synthetic fixtures, not production assets.

Synthetic tests do not substitute for the real VM run and safe rerun. Review their
evidence before marking this project step complete.

## Attribution

Generated with ChatGPT assistance for the electricity project. Students must review,
execute, explain, and adapt the code. Record AI assistance and each team member's
actual contribution in the final project README; do not assign unperformed work.
