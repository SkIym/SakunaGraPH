# SakunaGraPH ETL

The SakunaGraPH ETL application converts Philippine disaster data from
NDRRMC, DROMIC, GDA, EM-DAT, and PSGC into RDF for the SakunaGraPH knowledge
graph.

Run every command in this guide from the `sakunagraph_etl/` directory.

## Installation

Use Python 3.12 and install the package with its pinned constraints:

```bash
python -m pip install --editable . --constraint constraints.txt
```

DROMIC and NDRRMC PDF processing requires the document dependencies:

```bash
python -m pip install --editable ".[documents]" --constraint constraints.txt
```

Run `sakuna-etl --help` to list the available commands. Source RDF commands
accept `--profile local|onprem|cloud`; `local` is the default.

## EM-DAT

Download an EM-DAT XLSX export containing the Philippines records to process,
then run:

```bash
sakuna-etl emdat \
  --input ../data/raw/emdat/emdat.xlsx \
  --out-dir ../data/rdf/events/emdat \
  --validate
```

`--input` must identify one workbook. The command transforms the workbook,
validates the generated graph when `--validate` is present, and writes the
EM-DAT Turtle output under `--out-dir`.

## GDA

Place the cleaned Geography Disaster Archive workbook in the raw data
directory, then run:

```bash
sakuna-etl gda \
  --input ../data/raw/static/geog-archive-cleaned.xlsx \
  --out-dir ../data/rdf/events/gda \
  --validate
```

The command transforms the selected workbook and writes the GDA Turtle output
under `--out-dir`.

## NDRRMC

Parse the NDRRMC situation-report PDFs:

```bash
sakuna-etl parse-ndrrmc \
  --input-dir ../data/raw/ndrrmc \
  --output-dir ../data/parsed/ndrrmc
```

Build the NDRRMC RDF from the parsed folders:

```bash
sakuna-etl ndrrmc \
  --data-dir ../data/parsed/ndrrmc \
  --out-dir ../data/rdf/events/ndrrmc \
  --batch-size 10 \
  --validate
```

Use `--ocr` on `parse-ndrrmc` when scanned PDFs need the layout-aware OCR
fallback. Use `--start` and `--limit` on `ndrrmc` for a bounded local run.

## DROMIC

If the reports have not been collected yet, fetch the selected year:

```bash
sakuna-etl fetch-dromic --year 2026
```

Convert downloaded DOCX reports to the PDF input directory:

```bash
sakuna-etl convert-dromic --year 2026
```

Run the DROMIC pipeline:

```bash
sakuna-etl dromic \
  --year 2026 \
  --input-dir ../data/raw/dromic/2026 \
  --data-dir ../data/parsed/dromic \
  --out-dir ../data/rdf/events/dromic \
  --batch-size 100 \
  --validate
```

The `dromic` command performs the following work in one run:

1. Parses only PDFs whose recorded acquisition version is not already current.
2. Checks the parsed folders and refreshes the authoritative failure state.
3. Excludes parser failures, duplicate-output failures, and other failed event
   folders from transformation.
4. Selects the latest report revision for each DROMIC post.
5. Transforms the eligible folders and writes the year's Turtle graph.

When a year has no new or updated PDFs and already has an RDF output, the
pipeline leaves that output unchanged and skips failure checking and
transformation for the year. Pass `--force-transform` to rebuild it anyway.

To process every available year, point `--input-dir` at the directory
containing the `YEAR` folders:

```bash
sakuna-etl dromic \
  --all \
  --input-dir ../data/raw/dromic \
  --data-dir ../data/parsed/dromic \
  --out-dir ../data/rdf/events/dromic \
  --validate
```

Each parsed year stores resumable state in `_event_manifest.json`.
`_parsed.txt` and `_needs_rerun.txt` are generated compatibility views of that
state. Existing `_parsed.txt` entries are read before regeneration and seeded
into versioned parser state, so adopting the integrated pipeline does not
reparse those PDFs.

## PSGC

Convert a PSGC workbook into the geographic reference graph:

```bash
sakuna-etl psgc \
  --input ../data/raw/psgc/psgc.xlsx \
  --out ../data/rdf/psgc/psgc.ttl
```

Generate PSGC RDF before running source pipelines that require location
matching against the PSGC hierarchy.

## Cross-source alignment

After the source RDF files have been generated, resolve equivalent events
across sources:

```bash
sakuna-etl align \
  --sources ../data/rdf/events \
  --resolution-dir ../data/rdf/resolution \
  --stats
```

Add `--incremental` to reuse the existing resolution registry, or
`--skip-merge` to emit pairwise alignments without a merged registry.

## GraphDB publication

Publish selected RDF graphs to GraphDB:

```bash
sakuna-etl load-graphdb --scope psgc --replace --validate
sakuna-etl load-graphdb --scope events --replace --validate
sakuna-etl load-graphdb --scope resolution --replace --validate
```

DROMIC contains latest-revision facts and therefore must be published with
`--replace`; append publication is rejected. Configure the repository with
`GRAPHDB_HOST`, `GRAPHDB_REPOSITORY`, `GRAPHDB_USERNAME`, and
`GRAPHDB_PASSWORD`, or the corresponding profile configuration.

To publish the verified output of a recorded run, use its manifest:

```bash
sakuna-etl load-graphdb \
  --input-manifest PATH/manifest.json \
  --replace \
  --validate
```

## Development checks

```bash
python -m unittest discover -s tests -t . -v
python -m compileall -q src/sakunagraph_etl
```
