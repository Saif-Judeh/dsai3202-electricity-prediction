# Publishing validated processed data to Azure

The publisher uses the existing storage account amazonlake60306539 and private
container electricity-project. It creates no Azure resources and uses the existing
Azure CLI login with --auth-mode login on every request. No account keys, SAS tokens,
or credentials are stored in the code. Raw blobs under raw/ashrae-v1/ are not touched.

## Version and completion contract

Destination: processed/<exact ETL version directory name>/.
The three Parquet files and catalog.json are uploaded and verified first.
output_manifest.json is uploaded last and serves as the completion marker.
Consumers must require that marker and verify all named file sizes and SHA-256
hashes before reading a version. Directory listing alone does not prove completion.
This is a manifest-based publication protocol, not a multi-blob atomic transaction.

Before any Azure call, the publisher checks local ETL gates, row accounting,
accepted table counts, catalog counts, all output hashes, and the preserved
source-manifest/configuration/ETL-code identity. Publish an old version with its
matching code and configuration checkout; changed provenance is intentionally
refused. The publisher does not rerun ETL or reparse Parquet: successful ETL plus
unchanged hashes establishes that those validated bytes are being published.

Azure uploads use --overwrite false and --if-none-match '*' to prevent overwrites.
For each existing or new blob, the publisher checks size and SHA-256 metadata,
downloads the actual content with an ETag condition, computes SHA-256 locally, and
rechecks the remote ETag. Metadata alone is not accepted as proof of content.
Temporary verification downloads are deleted. Full byte verification adds reads
and transfer; each run downloads roughly one output version (about 63 MiB for the
current data), rather than the entire raw CSV collection.

All existing blobs are checked before any missing blob is uploaded. Interrupted
uploads without a completion manifest can resume: matching objects are skipped.
If a completion manifest already exists, every asset must exist and match;
missing or changed assets stop the run instead of being silently repaired.
After success, an evidence baseline records ETags. Later reruns require these
ETags to remain unchanged. A race with another writer fails closed via conditional
uploads, downloads, and final property checks; rerun after investigating the cause.
The local lock prevents simultaneous publishers from this VM. This script does not
prevent a privileged external actor from changing/deleting blobs later, so readers
must still validate the manifest and hashes. No signed-tampering guarantee is claimed.

## Evidence and measurements

- evidence/publish_processed_<run_id>.json: passed/failed, actions, output hashes,
  ETags, code identity, destination, duration, and process/child memory measures.
- evidence/processed_<version>_etags.json: immutable baseline created on first success.
- evidence/publish_test_<run_id>.json: offline test outcomes and test/code hashes.

The reported peak child RSS is the largest CLI child measurement, not cumulative
memory or a guaranteed total peak across parent and child processes. Publication
reports measure upload/verification work, separately from the ETL run.

Observed full ETL checkpoint supplied by the student on 2026-11-13:
52.881 seconds; peak process RSS 1543.76 MiB; derived data plus catalog 63.25 MiB;
12,060,910 accepted electricity rows; 8,155,190 excluded non-electricity rows;
zero rejected training rows. The output_manifest.json size is additional.
Do not describe the 2.43-GiB source-to-63.25-MiB difference as pure compression:
the ETL also selects electricity training data and excludes other source tables.

## Execution on the VM

```bash
cd ~/electricity-project
.venv/bin/python scripts/test_publish_processed.py
.venv/bin/python scripts/publish_processed.py --etl-dir ~/electricity-data/processed/etl-v1-c4c56f89d676b969
.venv/bin/python scripts/publish_processed.py --etl-dir ~/electricity-data/processed/etl-v1-c4c56f89d676b969
```

Run the next command only after the previous command passes. Expected first run:
uploaded=5, skipped=0. Expected complete rerun: uploaded=0, skipped=5. An interrupted
first run may produce a later successful run with mixed upload/skip counts.

The ten offline tests use synthetic byte fixtures and an in-memory Azure substitute;
they require no Azure login, call no Azure APIs, and do not touch production data.
They test protocol safety, not the live service or Parquet encoding. The real VM
publication and rerun provide the Azure integration evidence. Keep those reports.

Commit code, documentation, and JSON evidence; do not commit Parquet files or keys.
These files were generated with ChatGPT assistance. Students should review and
explain them, execute the real Azure validation, and disclose AI use in the README.

## Official CLI references

- https://learn.microsoft.com/en-us/cli/azure/storage/blob
- https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli
