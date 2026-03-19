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

## Directory Structure

```
etl/
├── fetch/                  # Web scraping (DROMIC situation reports)
├── parse/                  # PDF extraction (NDRRMC reports → CSV)
├── transform/              # Data cleaning and normalization to dataclasses
├── semantic_processing/    # NLP-based disaster classification and location matching
├── mappings/               # Dataclass-to-RDF triple mapping logic + IRI generation
├── udfs/                   # User-defined functions for morph_kgc (GDA RML)
├── pipeline/               # Orchestration scripts that run the full ETL per source
├── config.ini              # morph_kgc configuration (GDA RML pipeline)
└── disaster_classes.json   # 74 disaster type definitions for semantic classification
```

## Pipeline Overview

```
Raw Data (PDF, XLSX)
        │
        ▼
   fetch / parse          Scrape or extract structured data from raw sources
        │
        ▼
     transform            Normalize columns, clean values, resolve locations,
        │                 and load into typed dataclasses
        ▼
 semantic_processing      Classify disaster types (sentence-transformers)
        │                 and match locations to PSGC codes (fuzzy matching)
        ▼
     mappings             Convert dataclasses to RDF triples using rdflib
        │
        ▼
     pipeline             Orchestrate per-source runs, write .nt output
        │
        ▼
   ../data/rdf/           N-Triples output files
        │
        ▼
     GraphDB              Load into triplestore
```

## Usage

### NDRRMC

1. **Parse** PDFs into CSVs (ensure PDFs are in `data/ndrrmc/`):
   ```bash
   python ./parse/ndrrmc-v311.py
   ```

2. **Run** the full transform + mapping pipeline:
   ```bash
   python -m pipeline.run_ndrrmc
   ```

### GDA

```bash
python -m pipeline.run_gda
```

### EM-DAT

```bash
python -m pipeline.run_emdat
```

### PSGC

Convert the PSGC datafile to RDF:
```bash
python ./transform/psgc_datafile.py
```

## Key Technologies

- **Data wrangling**: Polars, Pandas
- **RDF**: rdflib,
- **NLP**: sentence-transformers, thefuzz
- **PDF parsing**: pdfplumber
- **Web scraping**: Selenium

## Known Limitations

- OCR for scanned/image-based NDRRMC PDFs is not yet implemented
- Loading RDF into GraphDB is still manual
- PSGC mapping goes down to municipal level only, though PSGC rdf can include barangay level
- Automatic pipeline not yet implemented
