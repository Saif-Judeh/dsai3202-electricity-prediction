# Combined pipeline and feature publication

Run `scripts/run_pipeline.py` from the existing virtual environment on the Azure VM.
It preserves the existing component scripts so their code/config hashes and published
ETL/feature versions do not change. This bundle adds files; it does not replace
publish_processed.py, build_features.py, run_etl.py or their pinned configuration.

Order: synthetic tests, source manifest validation, safe raw ingestion, full source
profiling, ETL, processed publication, training-only EDA, feature build, feature
publication. The runner stops at the first failed stage and records the child exit
status, console log and new evidence path. Missing/ambiguous reports also block the
workflow. `--skip-tests` is intended only for the second data-stage rerun after the
same code has passed tests; it is recorded in the pipeline report.

The existing manifest/ETL/features jobs verify or reuse immutable versions. Profiling
and EDA recompute summaries and create fresh evidence. The synthetic test data is
isolated and removed. Runner test output deliberately includes `STAGE FAILED` for
simulated bad children; the runner test suite itself must end with `PASSED: 4 runner
checks`. Those expected failures are not failures of the real pipeline.

Feature publication uses curated/<feature-version>/ in the same private container.
It checks the processed provenance, feature code/config identity, zero-failure quality
counts, all local hashes and expected split rows. Azure calls use the existing login,
never account keys. Existing blobs are downloaded and SHA-256 checked before any new
writes. New blobs use conditional non-overwriting uploads, and the feature manifest is
uploaded last. Readers must require that manifest and verify every referenced hash;
this is a completion protocol, not a multi-object transaction or signed tamper defense.
Completed versions with missing/changed blobs fail instead of being silently repaired.

Standalone feature-publication commands, if needed:

```bash
.venv/bin/python scripts/test_feature_publication.py
.venv/bin/python scripts/publish_features.py \
  --feature-dir ~/electricity-data/features/features-v1-4416c69c061b464e \
  --etl-dir ~/electricity-data/processed/etl-v1-c4c56f89d676b969
```

The first real publication should upload five assets; its safe rerun should upload
zero and skip five. A resumed partial upload may have mixed counts. Both publication
paths verify actual downloaded bytes. Each complete pipeline run therefore downloads
about 206 MiB of processed/features data for verification, plus small manifests; it
also performs raw metadata/ETag checks and reads local raw files for profiling.

Evidence patterns: pipeline_<run-id>.json and per-stage logs, publish_features_*.json,
feature_features-v1-<hash>_etags.json, eda_summary_*.json, feature_publish_test_*.json,
pipeline_test_*.json, and the established component reports. Large data stays outside
Git. A pipeline report records the starting Git commit and dirty-state flag; new files
should be committed after validation, and the final submitted commit must be recorded.

The complete real pipeline passed 13 stages, followed by a safe data-stage rerun
that passed 8 stages. Feature publication uploaded 5 objects on the first run and
verified/skipped all 5 on the rerun. See the repository-root README.md for the exact
evidence links, shared team contributions and submission instructions. These live VM
results are separate from the offline synthetic tests.
