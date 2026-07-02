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
├── pipeline/               # Orchestration scripts that run the full ETL per source

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

1. **Parse** PDFs into CSVs (ensure PDFs are in `data/raw/ndrrmc/`):
   ```bash
   python -m parse.ndrrmc
   ```

2. **Run** the full transform + mapping pipeline:
   ```bash
   python -m pipeline.run_ndrrmc
   ```

   To validate each output batch before serialization:
   ```bash
   python -m pipeline.run_ndrrmc --validate --batch-size 10
   ```


### DROMIC

1. **Parse** PDFs into CSVs (ensure DOCXs are converted and PDFs are in `data/raw/dromic/`):
   ```bash
   python -m parse.dromic --year [year]
   ```

2. **Run** the full transform + mapping pipeline:
   ```bash
   python -m pipeline.run_dromic
   ```


### GDA
1. **Collect** Ensure the cleaned digitized archive file is in `data/raw/static`

2.  **Rrun** the transform and mapping logic:
```bash
python -m pipeline.run_gda
```

### EM-DAT

1. **Collect** Download the latest EM-DAT report for the Philippines from their website. Select all types and include pre-2000 (historical) data. Ensure the xlsx file is in `data/raw/emdat`

2. **Run** the transform and mapping logic:
```bash
python -m pipeline.run_emdat
```

### PSGC

Convert the PSGC datafile to RDF:
```bash
python -m transform.psgc_datafile -i [file_path]
```

To include barangays:
```bash
python -m transform.psgc_datafile -i [file_path] --barangay
```

## Key Technologies

- **Data wrangling**: Polars, Pandas
- **RDF**: rdflib
- **NLP**: sentence-transformers, thefuzz
- **PDF parsing**: pdfplumber, docling
- **Web scraping**: Selenium

## Known Limitations

- OCR for scanned/image-based NDRRMC PDFs is not yet implemented
- Broken parsed assistance tables for DROMIC PDFs
- Loading and updating RDF into GraphDB is still manual
- PSGC location mapping goes down to municipal level only, though PSGC rdf can include barangay level
- Automatic pipeline (from fetch to parse to transform) not yet implemented
- The graph can be enriched by extracting information from unstructured text / narrative inside the reports and tables. LLM or NLP-guided extraction is a possibility.
