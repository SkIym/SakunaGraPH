# Stage 0 ETL baseline

This record closes the behavior-characterization work deferred from Stage 0.
These baseline commands were originally run from `etl/`. The production
package now lives in the sibling `sakunagraph_etl/` project, while the legacy
commands remain under `etl/`. Production runs should supply immutable input
manifests; the explicit local-file forms below are the shortest reproducible
smoke commands.

## Source commands and fixtures

| Source | Local command | Representative fixture |
|---|---|---|
| EM-DAT | `sakuna-etl emdat --input PATH/fixture.xlsx --out-dir PATH/rdf --validate` | A one-row workbook built by `tests.test_emdat_stage3._write_workbook`, including dates, location, casualties, affected population, and damage. |
| GDA | `sakuna-etl gda --input PATH/geog-archive-cleaned.xlsx --out-dir PATH/rdf --validate` | One flood event built by `tests.test_stage4_sources.stage4_fixture_graphs`, including dates, location, subtype, and provenance. |
| PSGC | `sakuna-etl psgc --input PATH/psgc.xlsx --out-dir PATH/rdf` | One active region row built by `tests.test_stage4_sources.stage4_fixture_graphs`, including its code, label, level, population, and status. |
| NDRRMC | `sakuna-etl ndrrmc --data-dir PATH/parsed/ndrrmc --out-dir PATH/rdf --validate --batch-size 10` | One typhoon event built by `tests.test_stage4_sources.stage4_fixture_graphs`, including deterministic identity and start/end dates. |
| DROMIC | `sakuna-etl dromic --data-dir PATH/parsed/dromic --year 2026 --out-dir PATH/rdf --validate --batch-size 100` | One flood event built by `tests.test_stage4_sources.stage4_fixture_graphs`, including location and remarks. |

The canonical N-Triples are in `tests/golden/`. `tests/baselines.json` records
the exact byte checksum, triple count, deterministic event/location IRI,
representative triple, and fixture builder for every source.
`tests/test_stage0_baselines.py` rejects any unreviewed drift in that evidence.
The fixture-to-RDF and SHACL checks remain in `test_emdat_stage3.py` and
`test_stage4_sources.py`.

Full-data evidence retained from the migration is deliberately separate from
the small regression fixtures: EM-DAT produced 30,955 triples with canonical
SHA-256 `73e9e03838dd33430d5ffe95b92962c6c0c1a21e62c8a93f74c13d196617827b`;
PSGC produced 16,088 triples; GDA produced 13,047 triples. NDRRMC and DROMIC
remain fixture-baselined because their document inputs and enrichment models
are operational dependencies rather than repository test data.

## External dependency inventory

| Area | Dependency | Requirement or limitation |
|---|---|---|
| Runtime | CPython 3.12 | Supported range is `>=3.12,<3.13`; exact Python packages are pinned by `constraints.txt`. |
| Workbooks/RDF | Polars, Pandas, OpenPyXL, fastexcel, RDFLib, pySHACL | Installed by the core environment. PSGC, ontology, SHACL, and organization-registry resources must be reachable through configuration. |
| Enrichment | sentence-transformers, thefuzz, model cache | The NLP model and revision must be configured or pre-fetched for isolated execution. |
| DROMIC acquisition | Chrome/Chromium, compatible driver, Selenium, network access | Needed only by the fetch worker. Fetch state and downloaded reports require writable configured roots. |
| Document parsing | pdfplumber, Docling, GLiNER2 | Needed only by document workers. OCR for scanned NDRRMC PDFs is not implemented. |
| DOC/DOCX conversion | LibreOffice in the Linux document image; `docx2pdf` plus Microsoft Word for the legacy desktop path | Microsoft Word automation is not usable in the Linux core image. |
| Publication | GraphDB/RDF4J endpoint, repository, TLS credentials | Needed only for publication, integration rollback drills, backup, and restore. Unit rollback tests use a fake transport. |
| Cloud profile | boto3 and S3-compatible object storage | Not imported by local/on-premise core execution. |

## Imports, paths, debug outputs, and entry points

Production ownership is under `sakunagraph_etl/src/sakunagraph_etl/`. The old
top-level modules under `etl/`—`fetch`, `parse`, `transform`, `mappings`,
`semantic_processing`, `pipeline`,
and `validate` packages are import and command compatibility shims. Tests in
`test_stage4_sources.py` and `test_stage5_services.py` assert identity between
the important old exports and package-owned implementations.

All package jobs load typed paths from `sakunagraph_etl.config` and accept
explicit input/output paths or input manifests. Compatibility commands may
still use configured discovery defaults. Relative legacy paths resolve from
the legacy `etl/` directory; installed commands can run from any directory when
given explicit paths or `SAKUNA_REPOSITORY_ROOT`. Runtime data belongs beneath configured raw,
parsed, RDF, artifact, quarantine, manifest, and log roots, never inside the
installed package.

The historical fixed diagnostic files `sam.csv`, `hakdog.csv`, and
`dump/assistance.csv` are not production write targets. Diagnostics require
`--debug-dir` or `SAKUNA_DEBUG_ROOT` and use source-scoped names. Existing
untracked copies in a developer checkout are not pipeline inputs.

The supported operator entry point is `sakuna-etl`; source commands are
`emdat`, `gda`, `psgc`, `ndrrmc`, and `dromic`. Acquisition/quality commands
are `fetch-dromic`, `convert-dromic`, `parse-dromic`, `parse-ndrrmc`, and
`check-dromic`. Shared service commands are `align`, `load-graphdb`,
`graphdb-admin`, `artifacts`, and `workflow`. Historical `python -m` commands
documented in `README.md` remain wrappers during the compatibility window.

## Baseline revision policy

The golden files and catalog are the rollback evidence that can be verified in
a dirty checkout. A stage-boundary Git tag must point to the reviewed commit
that contains them; do not label the current working tree as known-good before
it is committed and the full quality gate passes.
