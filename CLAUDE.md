# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SakunaGraPH is a knowledge graph system for Philippine disaster data integration. It combines an OWL 2 ontology with an ETL pipeline that merges data from five sources (NDRRMC, GDA, EM-DAT, PSGC, DROMIC) into a unified RDF triple store (GraphDB).

## Running the Pipeline

All pipeline scripts must be run from the `etl/` directory — relative paths like `../data/...` resolve from there.

```bash
cd etl/
pip install -r ../requirements.txt

# Per-source ETL (produces Turtle RDF files):
python -m pipeline.run_ndrrmc [--out ndrrmc] [--start 0] [--count 10]
python -m pipeline.run_gda [--out gda.ttl]
python -m pipeline.run_emdat [--out emdat.ttl]
python -m pipeline.run_dromic

# Prerequisites for NDRRMC:
python ./parse/ndrrmc-v311.py        # PDF → CSV (pdfplumber)
python ./transform/psgc_datafile.py  # PSGC XLSX → psgc.ttl

# Cross-source entity resolution (run after all source TTLs are generated):
python pipeline/build_alignment.py [--sources DIR] [--skip-merge] [--stats] [--incremental] [--verbose]

# DROMIC web scraper (Selenium, stateful/resumable):
python -m fetch.dromic --year [YYYY]

# Organization registry → RDF:
python semantic_processing/org_registry.py [-i registry.json] [-o orgs.ttl]

# Load into GraphDB (manual):
python pipeline/load_graphdb.py
```

There is no automated test suite or lint configuration.

## Architecture

### Data Flow

```
Raw files (PDF/XLSX/Web)
  → fetch/     (Selenium scraping for DROMIC)
  → parse/     (pdfplumber PDF extraction for NDRRMC)
  → transform/ (normalize to typed Python dataclasses)
  → semantic_processing/ (location/org/disaster-type resolution via NLP)
  → mappings/  (dataclass → RDF triples via rdflib)
  → pipeline/  (orchestrate per source, then cross-source alignment)
  → GraphDB    (RDF triple store, loaded manually)
```

### Layer Responsibilities

**`transform/`** — Each source has its own module (`ndrrmc.py`, `emdat.py`, `gda.py`, `dromic.py`) that loads raw/parsed data and produces typed Python dataclasses (e.g., `Event`, `Incident`, `Casualties`, `AffectedPopulation`). These dataclasses are the internal exchange format between transform and mappings.

**`semantic_processing/`** — Three key singletons, each pre-loaded once and reused across the pipeline:
- `LOCATION_MATCHER` (`location_matcher_v2.py`) — Resolves messy location strings to PSGC IRIs by loading `psgc.ttl` and doing multi-tier fuzzy matching (City → Province → Region).
- `ORG_RESOLVER` (`org_resolver.py`) — Maps org names to canonical IRIs via fuzzy matching against `constants/org_registry.json`.
- `DISASTER_CLASSIFIER` (`disaster_classifier.py`) — Classifies disaster types using sentence-transformers cosine similarity against ontology definitions in `disaster_classes.json`.

**`mappings/`** — Each source has a mapping module with 10–20+ functions that accept dataclasses and emit rdflib triples. All IRIs are minted deterministically via UUID5 in `mappings/iris.py`. The IRI namespace is `https://sakuna.ph/{source}/{uuid}` for events; sub-resources extend as `{event_iri}/{segment}/{optional_id}`.

**`pipeline/`** — Orchestrates each source end-to-end. `run_ndrrmc.py` uses `ProcessPoolExecutor` for batch-parallel processing (each batch worker re-initializes logging). `build_alignment.py` performs cross-source entity resolution: it extracts event features from all source TTLs, blocks by date/type, scores pairwise similarity, and outputs `alignments.ttl` (with `owl:sameAs`) plus `dedup_registry.json`.

**`ontology/`** — OWL 2 ontology in `sakunagraph.ttl`. Core classes: `DisasterEvent`, `Incident`, impact classes (`AffectedPopulation`, `Casualties`, `HousingDamage`, etc.), response classes (`Relief`, `Assistance`), and geographic hierarchy (`Region` → `Province` → `Municipality` → `Barangay`). Imports GeoSPARQL 1.1, W3C PROV, SKOS, QUDT, and beAWARE.

### Data Directories (relative to `etl/`)

| Path | Contents |
|---|---|
| `../data/raw/{source}/` | Raw input files (XLSX, PDF) |
| `../data/parsed/{source}/` | Intermediate CSV/structured data |
| `../data/rdf/events/` | Per-source Turtle output (e.g., `ndrrmc-0.ttl`) |
| `../data/rdf/psgc/` | PSGC RDF graph (`psgc.ttl`) |
| `../data/rdf/resolution/` | `alignments.ttl`, `dedup_registry.json` |
| `../logs/` | Execution logs, scrape state, manifests |

### Key Technologies

- **RDF/OWL**: rdflib (graph creation & Turtle serialization)
- **Data wrangling**: Polars (primary), Pandas (compatibility)
- **PDF extraction**: pdfplumber
- **Web scraping**: Selenium (DROMIC, stateful with JSON state file)
- **NLP/matching**: sentence-transformers, thefuzz, spaCy
- **RML mapping**: morph-kgc (used for GDA via `mappings/gda_mappings.rml.ttl`)
- **Graph store**: GraphDB
