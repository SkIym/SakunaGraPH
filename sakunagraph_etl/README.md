# ETL Pipeline

Extract-Transform-Load pipeline for converting Philippine disaster data from multiple sources into RDF triples for the SakunaGraPH knowledge graph.

## Data Sources

| Source | Description | Raw Format |
|--------|-------------|------------|
| **NDRRMC** | National Disaster Risk Reduction and Management Council situation reports | PDF |
| **GDA** | Geography Disaster Archive (historical records) | XLSX |
| **EM-DAT** | Emergency Events Database (international disaster data) | XLSX |
| **PSGC** | Philippine Standard Geographic Code (geographic hierarchy) | XLSX |
| **DROMIC** | DSWD Disaster Response Operations Monitoring and Information Center | PDF (scraped) |

## Repository layout

```text
SakunaGraPH/
|-- sakunagraph_etl/         # standalone packaged application (this project)
|   |-- src/sakunagraph_etl/
|   |-- tests/
|   `-- deploy/
`-- etl/                     # preserved legacy commands and compatibility modules
```

The production implementation now lives under `src/sakunagraph_etl/`:
source-owned jobs are in `sources/`, shared matching in `enrichment/`,
cross-source resolution in `resolution/`, and GraphDB access in `io/` behind
the `rdf.publication` interface. The sibling `../etl/` directory retains the
historical module paths for existing automation but is not included in the
standalone wheel or container.

## Pipeline Overview

```
Raw Data (PDF, XLSX)
        |
        v
 sources/{source}/fetch or parse     Collect structured source data
        |
        v
 sources/{source}/transform          Normalize into typed dataclasses
        |
        v
 enrichment/ + transform/            Resolve shared semantics and impacts
        |
        v
 sources/{source}/rdf + quality/     Map RDF and enforce validation gates
        |
        v
 source jobs + orchestration/        Record immutable artifacts and workflows
        |
        v
 resolution/ + io/graphdb.py         Align events and publish named graphs
```

## Usage

Install and develop the production application from `sakunagraph_etl/`. Paths
resolve from the shared repository root, and every production runner accepts
explicit input/output arguments.

Install the constrained Python 3.12 core environment and editable package:

```bash
python -m pip install --editable . --constraint constraints.txt
```

Install the document/browser extra for DROMIC/NDRRMC collection and parsing,
or `all` for both document and cloud adapters:

```bash
python -m pip install --editable '.[documents]' --constraint constraints.txt
python -m pip install --editable '.[all]' --constraint constraints.txt
```

Select a typed deployment profile with `--profile local|onprem|cloud` or
`SAKUNA_ETL_PROFILE`. Profiles accept `SAKUNA_DATA_ROOT`,
`SAKUNA_LOGS_ROOT`, `SAKUNA_ONTOLOGY_ROOT`, `SAKUNA_CONSTANTS_ROOT`,
`SAKUNA_ARTIFACT_ROOT`, and the optional `SAKUNA_DEBUG_ROOT`. Local files are
the reference artifact store, `onprem` adds shared-filesystem locking, and
`cloud` uses the S3-compatible adapter configured by `SAKUNA_OBJECT_BUCKET`,
`SAKUNA_OBJECT_PREFIX`, and optional `SAKUNA_OBJECT_ENDPOINT`.

### Stage 8 parsed-data quality gates

All source jobs now create a structured parsed-data quality report before
transformation. `onprem` and `cloud` profiles stop on failed schema or
rejection-threshold checks; `local` records the report without breaking legacy
exploration. The default policy requires a nonempty input with no rejected
records. Configure reviewed tolerances with
`SAKUNA_QUALITY_MINIMUM_RECORDS`,
`SAKUNA_QUALITY_MAXIMUM_REJECTED_RECORDS`,
`SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO`, and
`SAKUNA_QUALITY_FAIL_ON_UNEXPECTED_COLUMNS`.

```bash
sakuna-etl quality source --source emdat --input PATH/export.xlsx \
  --profile onprem --report PATH/emdat-quality.json
sakuna-etl quality table --schema PATH/schema.json --input PATH/rows.jsonl \
  --format jsonl --report PATH/table-quality.json
```

See `docs/stage8-data-quality.md` for contract structure, reason codes, and
threshold semantics. Parsed-data validation complements, and does not replace,
the RDF SHACL gate.

### Reproducible Stage 6 artifacts

Source jobs snapshot inputs and RDF outputs below a content-derived run ID.
The immutable run manifest records SHA-256 checksums, validation status, code
version, storage URI, and named-graph context. Failed validation is written to
`quarantine/` instead of the publishable `runs/` namespace. Modification-time
selection has been removed; use an explicit `--input` or `--input-manifest`.

```bash
sakuna-etl artifacts verify --manifest-key runs/SOURCE/RUN_ID/manifest.json
sakuna-etl artifacts materialize \
  --manifest-key runs/SOURCE/RUN_ID/manifest.json \
  --destination ./reproduced-inputs
sakuna-etl load-graphdb --input-manifest /path/to/manifest.json --replace
```

### Stage 7 workflows and recovery

Run or resume package-owned local/on-premise workflows without changing the
underlying source, alignment, or publication commands:

```bash
sakuna-etl workflow list
sakuna-etl workflow run source-emdat --param input=PATH --param output_dir=PATH
sakuna-etl workflow run integration --params-file integration.json
sakuna-etl workflow backfill source-dromic --start 2026-01-01 --end 2026-01-31 \
  --params-file source-dromic.json
```

Task state, Prometheus text metrics, durable alerts, and OpenLineage-compatible
events are stored below `logs/`. Failed validation is a hard dependency gate:
alignment and GraphDB publication are not invoked. AWS Step Functions with
ECS/Fargate and EventBridge Scheduler is the managed deployment; systemd timers
provide the on-premise scheduler. See `deploy/README.md` and
`deploy/runbooks/` for provisioning, operations, and GraphDB recovery.

DROMIC event state is authoritative in each year's `_event_manifest.json`.
Producer-specific reason codes and timestamps are merged under a shared lock;
`_parsed.txt` and `_needs_rerun.txt` are regenerated compatibility views.
Container and on-premise instructions are in `deploy/README.md`.

### Unified command shell

The installed `sakuna-etl` command now targets source-owned packages for all
five data sources. Legacy modules remain thin delegates, so the unified,
packaged, and compatibility forms execute the same implementation:

```bash
sakuna-etl emdat --out emdat.ttl
python -m sakunagraph_etl.sources.emdat.job --out emdat.ttl
python -m pipeline.run_emdat --out emdat.ttl

sakuna-etl dromic --data-dir ../data/parsed/dromic --year 2026
python -m sakunagraph_etl.sources.dromic.job --data-dir ../data/parsed/dromic --year 2026
python -m pipeline.run_dromic --year 2026

sakuna-etl align --sources ../data/rdf/events --stats
python -m sakunagraph_etl.resolution.job --sources ../data/rdf/events --stats
python -m pipeline.build_alignment --sources ../data/rdf/events --stats

sakuna-etl load-graphdb --scope events --replace
python -m sakunagraph_etl.io.graphdb --scope events --replace
python -m pipeline.load_graphdb --scope events --replace
```

GraphDB replacement uses one RDF4J transaction per named graph. The `onprem`
and `cloud` profiles always parse and SHACL-validate each complete context
before opening a GraphDB connection; `--no-validate` is rejected for those
profiles. Local publication can opt in with `--validate`.

Run `sakuna-etl --help` for all commands. The installed command works from the
repository root, the standalone or legacy project, or another working directory. Wheel deployments that
keep data outside the package should set `SAKUNA_REPOSITORY_ROOT` or pass the
explicit input/output options.

The package lives under `src/sakunagraph_etl/`. GDA, EM-DAT, PSGC, NDRRMC, and
DROMIC implementations are owned by `sources/{source}/`; shared transforms,
SHACL validation, enrichment, resolution, storage, and orchestration are also
fully package-owned. The sibling `../etl/` tree preserves `fetch`, `parse`,
`transform`, `mappings`, `semantic_processing`, `pipeline`, `validate`, and
`etl_config` compatibility entry points without making them wheel dependencies.

### NDRRMC

1. **Parse** PDFs into CSVs (ensure PDFs are in `data/raw/ndrrmc/`):
   ```bash
   sakuna-etl parse-ndrrmc --input-dir PATH --output-dir PATH
   # Compatibility form: python -m parse.ndrrmc
   ```

2. **Run** the full transform + mapping pipeline:
   ```bash
   python -m pipeline.run_ndrrmc
   ```

   To validate each output batch before serialization:
   ```bash
   python -m pipeline.run_ndrrmc --validate --batch-size 10
   ```

   Override paths with `--data-dir`, `--out-dir`, and optionally `--debug-dir`.


### DROMIC

1. **Parse** PDFs into CSVs (ensure DOCXs are converted and PDFs are in `data/raw/dromic/`):
   ```bash
   sakuna-etl parse-dromic --year [year] --input-dir PATH --output-dir PATH
   # Compatibility form: python -m parse.dromic --year [year]
   ```

2. **Run** the full transform + mapping pipeline:
   ```bash
   python -m pipeline.run_dromic
   ```

   To validate each 100-event batch before merging into the year graph:
   ```bash
   python -m pipeline.run_dromic --validate --batch-size 100
   ```

   Override paths with `--data-dir`, `--out-dir`, and optionally `--debug-dir`.


### GDA
1. **Collect** Ensure the cleaned digitized archive file is in `data/raw/static`

2. **Run** the transform and mapping logic:
```bash
sakuna-etl gda --input PATH
# Compatibility form: python -m pipeline.run_gda
```

Use `--input` for a specific workbook and `--out-dir` for its RDF destination.

### EM-DAT

1. **Collect** Download the latest EM-DAT report for the Philippines from their website. Select all types and include pre-2000 (historical) data. Ensure the xlsx file is in `data/raw/emdat`

2. **Run** the transform and mapping logic:
```bash
sakuna-etl emdat
# Compatibility form: python -m pipeline.run_emdat
```

Use `--input` for a specific workbook. A `--data-dir` is accepted only when it
contains exactly one workbook; otherwise pass `--input-manifest`. Use
`--out-dir` for its RDF destination.

### PSGC

Convert the PSGC datafile to RDF:
```bash
sakuna-etl psgc --input [file_path]
# Compatibility form: python -m pipeline.run_psgc --input [file_path]
```

To include barangays:
```bash
python -m transform.psgc_datafile -i [file_path] --barangay
```

## Development checks

Run the standalone quality checks from `sakunagraph_etl/`:

```bash
python -m unittest discover -s tests -t . -v
python -m compileall -q src/sakunagraph_etl
python -m pip install --no-deps --no-build-isolation --editable .
python -m pip wheel --no-deps --no-build-isolation --wheel-dir dist .
```

The repository test suite also verifies the preserved sibling wrappers. Legacy
commands should be launched from `../etl/` after installing this package.

Diagnostic CSVs are disabled by default. Pass `--debug-dir PATH` or set
`SAKUNA_DEBUG_ROOT` to write named, source-scoped diagnostics without modifying
parsed input folders.

## Key Technologies

- **Data wrangling**: Polars, Pandas
- **RDF**: rdflib
- **NLP**: sentence-transformers, thefuzz
- **PDF parsing**: pdfplumber, docling
- **Web scraping**: Selenium

## Known Limitations

- OCR for scanned/image-based NDRRMC PDFs is not yet implemented
- Broken parsed assistance tables for DROMIC PDFs
- GraphDB binary backup/restore drills still require a licensed external GraphDB instance
- PSGC location mapping goes down to municipal level only, though PSGC rdf can include barangay level
- Automatic pipeline (from fetch to parse to transform) not yet implemented
- The graph can be enriched by extracting information from unstructured text / narrative inside the reports and tables. LLM or NLP-guided extraction is a possibility.
