# SakunaGraPH

A knowledge graph for Philippine disaster data integration based on an ontology.

## Overview

SakunaGraPH integrates disaster data from multiple Philippine and international sources into a unified RDF knowledge graph. It combines an OWL ontology for disaster modeling with an ETL pipeline that extracts, transforms, and loads data into a triplestore.

## Project Structure

```
SakunaGraPH/
├── ontology/       # OWL ontology and SKOS disaster type scheme
├── etl/            # ETL pipeline (fetch, parse, transform, map, load)
├── logs/           # Pipeline execution logs
└── requirements.txt
```

## Data Sources

- **NDRRMC** — National Disaster Risk Reduction and Management Council situation reports
- **GDA** — Geography Disaster Archive (historical records)
- **EM-DAT** — Emergency Events Database (international disaster data)
- **PSGC** — Philippine Standard Geographic Code (administrative geography)
- **DROMIC** — DSWD Disaster Response Operations Monitoring and Information Center

## Tech Stack

- **Ontology**: OWL 2, SKOS, GeoSPARQL, PROV, QUDT
- **ETL**: Python (rdflib, morph-kgc, Polars, pdfplumber, sentence-transformers)
- **Triplestore**: GraphDB

## Getting Started

```bash
pip install -r requirements.txt
```

See [`etl/README.md`](etl/README.md) for pipeline usage and [`ontology/README.md`](ontology/README.md) for ontology details.
