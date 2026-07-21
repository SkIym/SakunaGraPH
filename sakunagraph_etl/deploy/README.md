# Stage 6/7 deployment and operations

The same `sakuna-etl` commands run in a Python virtual environment or in the
two pinned images. The core image handles spreadsheet sources, alignment,
artifact operations, validation, and GraphDB publication. The optional
documents image adds Chromium, LibreOffice, PDF tooling, Docling, and GLiNER2
for DROMIC/NDRRMC collection and parsing.

## Local Python

From `sakunagraph_etl/`:

```bash
python -m venv .venv
.venv/bin/python -m pip install --editable . --constraint constraints.txt
.venv/bin/sakuna-etl emdat --input ../data/raw/emdat/report.xlsx
```

On Windows use `.venv\Scripts\python.exe` and
`.venv\Scripts\sakuna-etl.exe`. Add `.[documents]` for local DROMIC/NDRRMC
document work. The default `local` profile writes immutable runs below
`data/artifacts`.

## Docker Compose / on-premise

Create the host directories `data/artifacts` and `logs`, then build the core
image from `sakunagraph_etl/`:

```bash
docker compose build core
docker compose run --rm core emdat --input /data/raw/emdat/report.xlsx
docker compose run --rm core artifacts verify \
  --manifest-key runs/emdat/EMDAT_RUN_ID/manifest.json --profile onprem
```

Build and invoke the larger worker only when document/browser dependencies are
needed:

```bash
docker compose --profile documents build documents
docker compose --profile documents run --rm documents parse-dromic \
  --year 2025 --input-dir /data/raw/dromic/2025 \
  --output-dir /data/parsed/dromic/2025
```

The Compose profile mounts `/artifacts` as the shared network-filesystem
boundary. Its adapter uses cross-process lock files and atomic replacement for
mutable compatibility state, while run artifacts themselves are write-once.
Mount the same paths from NFS/SMB in a multi-host deployment and keep clocks
synchronized for stale-lock recovery.

## Cloud object storage

Install the pinned cloud extra and configure an S3-compatible bucket:

```bash
python -m pip install --editable '.[cloud]' --constraint constraints.txt
export SAKUNA_ETL_PROFILE=cloud
export SAKUNA_OBJECT_BUCKET=my-etl-artifacts
export SAKUNA_OBJECT_PREFIX=sakunagraph/prod
sakuna-etl emdat --input /work/report.xlsx
```

`SAKUNA_OBJECT_ENDPOINT` selects an S3-compatible endpoint; otherwise the AWS
endpoint for `AWS_REGION` is used. Credentials follow the normal boto3 provider
chain and are never stored in run manifests.

## Reproducing a run

Every manifest records immutable input/output URIs, SHA-256 checksums,
validation status, graph context, profile, parameters, and package version.
Verify or materialize the exact inputs before rerunning:

```bash
sakuna-etl artifacts verify --manifest-key RUN_MANIFEST_KEY --profile local
sakuna-etl artifacts materialize --manifest-key RUN_MANIFEST_KEY \
  --destination ./reproduced-inputs --profile local
sakuna-etl emdat --input ./reproduced-inputs/report.xlsx
```

Failed validation is written under `quarantine/` and is not eligible for
manifest-based GraphDB publication. An identical retry reuses the existing run;
a retry that produces different bytes for the same content-derived run ID fails
instead of overwriting it.

## Production workflows

The package-owned runner uses the same CLI boundaries as manual execution and
checkpoints verified immutable manifests:

```bash
sakuna-etl workflow list
sakuna-etl workflow run source-emdat --profile onprem \
  --param input=/data/raw/emdat/report.xlsx \
  --param output_dir=/data/rdf/events/emdat
sakuna-etl workflow backfill source-dromic --start 2026-01-01 --end 2026-01-31 \
  --params-file /etc/sakunagraph/workflows/source-dromic.json --profile onprem
```

AWS Step Functions Standard, ECS/Fargate, and EventBridge Scheduler are the
selected managed path under `deploy/terraform/`; hardened systemd units under
`deploy/systemd/` are the on-premise scheduler option. Operational procedures,
alert/metric setup, and tested recovery controls are documented in
`deploy/runbooks/`.

After installing the service/timer files and source-specific parameter JSON,
enable the desired non-manual schedules:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now \
  sakunagraph-etl-emdat.timer sakunagraph-etl-psgc.timer \
  sakunagraph-etl-ndrrmc.timer sakunagraph-etl-dromic.timer \
  sakunagraph-etl-integration.timer
systemctl list-timers 'sakunagraph-etl-*'
```
