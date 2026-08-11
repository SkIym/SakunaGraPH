# AGENTS.md

This file provides guidance to agents when working with code in this repository.

## Project Overview

SakunaGraPH is a knowledge graph system for Philippine disaster data integration. It combines an OWL 2 ontology with an ETL pipeline that merges data from five sources (NDRRMC, GDA, EM-DAT, PSGC, DROMIC) into a unified RDF triple store (GraphDB).

## Running the Pipeline

The standalone production application lives in `sakunagraph_etl/`; install and
test it from that directory. The historical commands remain in the sibling
`etl/` directory and should still be launched there when compatibility behavior
is being exercised. Explicit production paths resolve from the repository root.

```bash
cd sakunagraph_etl/
pip install --editable . --constraint constraints.txt

# Stage 1 quality checks:
python -m unittest discover -s tests -t . -v
python -m compileall -q src/sakunagraph_etl

# Per-source ETL (produces Turtle RDF files):
sakuna-etl ndrrmc --data-dir PATH [--out ndrrmc] [--start 0] [--limit 10]
sakuna-etl gda --input PATH [--out gda.ttl]
python -m sakunagraph_etl.sources.emdat.job [--out emdat.ttl]
sakuna-etl emdat --input PATH [--out emdat.ttl]
sakuna-etl dromic --data-dir PATH --year YYYY

# Prerequisites for NDRRMC:
sakuna-etl parse-ndrrmc --input-dir PATH --output-dir PATH
sakuna-etl psgc --input PATH --out PATH/psgc.ttl

# Cross-source entity resolution (run after all source TTLs are generated):
sakuna-etl align [--sources DIR] [--skip-merge] [--stats] [--incremental] [--verbose]

# DROMIC web scraper (Selenium, stateful/resumable):
sakuna-etl fetch-dromic --year [YYYY]

# Organization registry → RDF:
python -m sakunagraph_etl.enrichment.organization_registry [-i registry.json] [-o orgs.ttl]

# Load into GraphDB (manual):
sakuna-etl load-graphdb
```

Focused regression tests live in `sakunagraph_etl/tests/`. Package and tool metadata
live in `sakunagraph_etl/pyproject.toml`; CI runs the tests and validates editable package
metadata.

The standalone package is `sakunagraph_etl/src/sakunagraph_etl/`. The unified
`sakuna-etl` console command owns production behavior; legacy `python -m
pipeline...` commands under `etl/` remain supported wrappers.

## Architecture

### Data Flow

```
Raw files (PDF/XLSX/Web)
  → sources/{source}/fetch or parse
  → sources/{source}/transform
  → enrichment/ (location/org/disaster-type resolution)
  → sources/{source}/rdf
  → source job and resolution workflow
  → GraphDB    (RDF triple store, loaded manually)
```

### Layer Responsibilities

**`sources/{source}/`** — Each source owns its parser, transform, RDF mappings,
and job. Typed dataclasses remain the internal exchange format.

**`enrichment/`** — Shared services are package-owned and reused across jobs:
- `LOCATION_MATCHER` (`locations.py`) — Resolves messy location strings to PSGC IRIs using hierarchical and single-location matching.
- `ORG_RESOLVER` (`organizations.py`) — Maps organization names to canonical IRIs via the shared registry.
- `DISASTER_CLASSIFIER` (`disaster_types.py`) — Classifies disaster types using sentence-transformers cosine similarity against ontology definitions.
- `PARAMS_EXTRACTOR` (`climate_parameters.py`) — Extracts climate measurements and warnings from NDRRMC narrative text.

**`rdf/` and `sources/{source}/rdf.py`** — Mapping modules accept dataclasses
and emit RDFLib triples. Shared deterministic UUID5 construction lives in
`rdf/iris.py`.

**`orchestration/` and `resolution/`** — Package-owned workflows orchestrate
source jobs, while resolution extracts features, blocks and scores pairs, and
emits deterministic alignments and registry artifacts.

**`ontology/`** — OWL 2 ontology in `sakunagraph.ttl`. Core classes: `DisasterEvent`, `Incident`, impact classes (`AffectedPopulation`, `Casualties`, `HousingDamage`, etc.), response classes (`Relief`, `Assistance`), and geographic hierarchy (`Region` → `Province` → `Municipality` → `Barangay`). Imports GeoSPARQL 1.1, W3C PROV, SKOS, QUDT, and beAWARE.

### Data Directories (relative to `sakunagraph_etl/`)

| Path | Contents |
|---|---|
| `../data/raw/{source}/` | Raw input files (XLSX, PDF) |
| `../data/parsed/{source}/` | Intermediate CSV/structured data |
| `../data/rdf/events/{source}` | Per-source Turtle output (e.g., `ndrrmc-0.ttl`) |
| `../data/rdf/psgc/` | PSGC RDF graph (`psgc.ttl`) |
| `../data/rdf/resolution/` | `alignments.ttl`, `dedup_registry.json` |
| `../logs/` | Execution logs, scrape state, manifests |

### Key Technologies

- **RDF/OWL**: rdflib (graph creation & Turtle serialization)
- **Data wrangling**: Polars (primary), Pandas (compatibility)
- **PDF extraction**: pdfplumber, docling
- **Web scraping**: Selenium (DROMIC, stateful with JSON state file)
- **NLP/matching**: sentence-transformers, thefuzz
- **Graph store**: GraphDB
