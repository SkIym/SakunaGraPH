# Contributing to SakunaGraPH

SakunaGraPH welcomes focused fixes, tests, documentation, mappings, ontology improvements, and
well-scoped product changes. Contributions must preserve source provenance, deterministic output,
and the distinction between repository fixtures and externally licensed data.

## Before opening a change

1. Open an issue for changes that alter ontology meaning, public API contracts, entity-resolution
   behavior, publication semantics, or user-visible workflows.
2. Describe the information need or failure before proposing a representation.
3. Identify affected sources, ontology terms, SHACL shapes, API contracts, and migration concerns.
4. Never attach restricted reports, source exports, credentials, or production artifacts to an
   issue or pull request.

Small documentation fixes and narrowly scoped test corrections can go directly to a pull request.

## Repository ownership boundaries

| Change | Primary location |
| --- | --- |
| Ontology semantics and constraints | `ontology/` |
| Production ingestion and publication | `sakunagraph_etl/src/sakunagraph_etl/` |
| Historical command compatibility | `etl/` |
| HTTP contracts and graph retrieval | `api/` |
| User interface | `frontend/` |
| Application gateway and deployment | `deploy/`, `sakunagraph_etl/deploy/` |

Do not add new production logic to the historical `etl/` tree. Compatibility wrappers may delegate
to the standalone package.

## Development setup

### ETL

Use Python 3.12 and run from `sakunagraph_etl/`:

```bash
python -m pip install --editable . --constraint constraints.txt
python -m unittest discover -s tests -t . -v
python -m compileall -q src/sakunagraph_etl
```

Document parsing requires the optional dependency layer:

```bash
python -m pip install --editable ".[documents]" --constraint constraints.txt
```

### API

Run from `api/`:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
fastapi dev src/main.py
```

Most unit tests use fakes and do not require GraphDB or a model server. Live behavior requires the
endpoints documented in `api/README.md`.

### Frontend

Run from `frontend/` with Node.js 22:

```bash
npm ci
npx playwright install chromium
npm run quality
npm run test:e2e
```

Additional CI suites cover streaming, accessibility, performance, visual regression, API
contracts, and deployment.

## Change requirements

### Ontology changes

- Explain the real-world meaning and the intended competency question.
- Reuse established vocabulary terms when their semantics match.
- Update labels and definitions for new public terms.
- Add or update SHACL where data conformance changes.
- Add a positive and negative example when a new constraint is introduced.
- Update competency questions and affected mappings.
- Check whether existing RDF or queries require migration.
- Do not silently change the meaning of an existing IRI.

### ETL and resolution changes

- Keep source-specific interpretation in the owning source module.
- Preserve typed internal records and deterministic identifiers.
- Retain source, row/report, acquisition, and transformation provenance.
- Update golden RDF only after reviewing the semantic diff.
- Add regression tests for rejected input as well as the happy path.
- Resolution threshold changes require hard-negative examples and an explanation of expected false
  positive/negative behavior.

### API and Ask changes

- Treat model output as untrusted data.
- Keep graph operations read-only and bounded.
- Do not interpolate user text into SPARQL.
- Update Pydantic contracts, contract tests, and frontend consumers together.
- Preserve evidence IDs, provenance, warnings, and ambiguity behavior.
- Add evaluation cases for changes to planning, resolution, query construction, or grounding.

### Frontend changes

- Support keyboard and touch interaction.
- Preserve mobile layouts and readable contrast.
- Respect reduced-motion preferences.
- Add or update component, end-to-end, accessibility, and visual coverage in proportion to risk.
- Do not imply real-time coverage or emergency-service reliability.

## Data policy

Only synthetic or explicitly redistributable fixtures belong in Git. Raw data paths, downloads,
logs, generated RDF, secrets, and local model artifacts must remain ignored. Consult
`DATA_SOURCES.md` before adding a fixture derived from an external source.

## Pull request checklist

- [ ] The change is scoped to the correct package or layer.
- [ ] Relevant tests pass locally.
- [ ] Documentation and public contracts are updated.
- [ ] Ontology or RDF changes include a reviewed semantic diff.
- [ ] New data is synthetic or has documented redistribution permission.
- [ ] No credentials, personal data, restricted reports, or generated runtime artifacts are added.
- [ ] Failure behavior, provenance, and rollback implications were considered.
- [ ] User-facing claims remain measurable and do not overstate coverage or freshness.

## Commit and release conventions

Use small, descriptive commits. Conventional prefixes such as `feat:`, `fix:`, `docs:`, `test:`,
and `refactor:` are encouraged because the existing history already follows that style.

The repository's first knowledge graph and ontology release is tagged `sakunagraphv.1.0`. Future
release notes should state which artifacts are included, their checksums, source coverage and access
dates, ontology version, validation status, evaluation results, and any redistribution restrictions.

