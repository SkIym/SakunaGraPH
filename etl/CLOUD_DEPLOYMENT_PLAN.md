# SakunaGraPH ETL Staged Refactoring and Deployment Plan

## Purpose

This plan evolves the SakunaGraPH ETL from a local, working-directory-dependent
pipeline into a reproducible application that can run locally, on premises, or
in the cloud. Refactoring is divided into independently releasable stages so
that the pipeline remains usable throughout the migration. Each stage must
preserve the current RDF mappings, deterministic IRIs, provenance, SHACL
validation, and source-specific transformation behavior unless a separately
approved change says otherwise.

The recommended cloud operating model is managed container jobs coordinated by
a workflow service. Local Python execution and on-premise scheduling remain
first-class deployment modes. A distributed processing framework is not
currently needed; Polars and source-level or event-level batch parallelism
remain appropriate.

## Refactoring Principles

- Keep the pipeline runnable at the end of every stage.
- Add replacement interfaces before moving their existing implementations.
- Migrate one source through the complete pipeline before migrating the next.
- Preserve old entry points with thin compatibility wrappers until all callers
  have moved.
- Compare RDF output against golden fixtures before and after each migration.
- Separate structural refactoring from changes to domain or mapping behavior.
- Implement local adapters first; add on-premise and cloud adapters behind the
  same interfaces.
- Make every stage small enough to review, release, and revert independently.

## Goals

- Run every command from any working directory.
- Package the ETL as an installable Python application.
- Preserve simple local execution without requiring containers or cloud
  services.
- Support on-premise operation with local or network storage and a local
  GraphDB deployment.
- Make all inputs, outputs, configuration, and credentials explicit.
- Support local paths and cloud object-storage URIs through one interface.
- Make retries safe and pipeline results reproducible.
- Validate data before publishing it to GraphDB.
- Replace GraphDB named graphs atomically.
- Record enough operational metadata to reproduce and audit every run.
- Deploy source jobs independently when their runtime requirements differ.

## Non-goals

- Rewriting established RDF mappings solely for structural consistency.
- Introducing Spark, Kafka, Kubernetes, or a lakehouse platform without a
  demonstrated scale or latency requirement.
- Moving ontology, RDF, or SHACL semantics into an orchestration platform.
- Combining every source into one mandatory runtime image.

## Current State Summary

### Strengths to preserve

- Clear fetch, parse, transform, semantic-processing, mapping, validation, and
  pipeline responsibilities.
- Deterministic UUID-based event and sub-resource IRIs.
- Source provenance in the generated RDF.
- SHACL validation support in the source pipeline runners.
- Staged validation and promotion in the PSGC pipeline.
- Stateful and resumable DROMIC fetching.
- Cross-source blocking, scoring, clustering, and alignment.
- GraphDB named-graph loading, scoped selection, dry runs, and atomic context
  replacement.

### Deployment constraints to remove

- Top-level imports such as `from transform...` require execution from `etl/`.
- Many paths are hard-coded as `../data/...` or `../logs/...`.
- `config.ini` is unused and refers to files that no longer exist.
- Dependencies are unpinned and combine browser, PDF, NLP, RDF, and validation
  workloads in one environment.
- Fixed debug outputs such as `dump/assistance.csv`, `sam.csv`, and
  `hakdog.csv` can collide across concurrent jobs.
- Some inputs are selected by local modification time instead of an explicit
  version or manifest.
- SHACL validation is generally optional instead of a production gate.
- There is no automated test suite, continuous integration workflow, container
  definition, or infrastructure-as-code structure.
- Operational run metadata is distributed across logs, manifests, and ad hoc
  rerun files.

## Target Architecture

```text
Scheduler or workflow service
    |
    +--> fetch job per source
    |       |
    |       +--> immutable raw object storage
    |
    +--> parse job per source or partition
    |       |
    |       +--> parsed artifacts and rejected-record quarantine
    |
    +--> transform and semantic-enrichment job
    |       |
    |       +--> typed curated artifacts
    |
    +--> RDF mapping and SHACL validation
    |       |
    |       +--> versioned published RDF
    |
    +--> cross-source alignment
    |       |
    |       +--> versioned alignment and registry artifacts
    |
    +--> atomic GraphDB named-graph replacement
            |
            +--> active production graph
```

Each stage passes artifact URIs and small metadata values to the next stage.
Large files must not be passed through an orchestrator's message or metadata
store.

## End-State Repository Structure

This is the structure after the staged migration, not a directory tree to create
in one change. Use a hybrid source-oriented layout: source-specific behavior
stays together, while RDF infrastructure, enrichment, storage, resolution, and
validation remain shared.

```text
etl/
|-- pyproject.toml
|-- README.md
|-- CLOUD_DEPLOYMENT_PLAN.md
|-- src/
|   `-- sakunagraph_etl/
|       |-- __init__.py
|       |-- config.py
|       |-- cli/
|       |   |-- __init__.py
|       |   |-- main.py
|       |   |-- fetch.py
|       |   |-- run.py
|       |   |-- align.py
|       |   `-- publish.py
|       |-- io/
|       |   |-- storage.py
|       |   |-- manifests.py
|       |   `-- graphdb.py
|       |-- sources/
|       |   |-- dromic/
|       |   |   |-- fetch.py
|       |   |   |-- parse.py
|       |   |   |-- transform.py
|       |   |   |-- rdf.py
|       |   |   `-- job.py
|       |   |-- ndrrmc/
|       |   |-- emdat/
|       |   |-- gda/
|       |   `-- psgc/
|       |-- enrichment/
|       |   |-- locations.py
|       |   |-- organizations.py
|       |   |-- disaster_types.py
|       |   `-- climate.py
|       |-- resolution/
|       |   |-- features.py
|       |   |-- blocking.py
|       |   |-- scoring.py
|       |   |-- clustering.py
|       |   `-- registry.py
|       |-- rdf/
|       |   |-- graph.py
|       |   |-- iris.py
|       |   `-- common_mappings.py
|       `-- quality/
|           |-- schemas.py
|           `-- shacl.py
|-- tests/
|   |-- unit/
|   |-- integration/
|   |-- fixtures/
|   `-- golden/
|-- orchestration/
|   `-- airflow/ or managed-workflow definitions
`-- deploy/
    |-- docker/
    `-- terraform/ or provider-equivalent IaC
```

The `data/`, `logs/`, ontology, and other runtime artifacts should remain
outside the installed Python package. In cloud environments, data and logs move
to managed storage and logging services rather than into the container image.

## Migration Mapping

| Current location | Target location |
|---|---|
| `fetch/dromic.py` | `sources/dromic/fetch.py` |
| `parse/dromic.py` | `sources/dromic/parse.py` or a `parse/` subpackage |
| `parse/ndrrmc.py` | `sources/ndrrmc/parse.py` or a `parse/` subpackage |
| `transform/{source}.py` | `sources/{source}/transform.py` |
| `mappings/{source}.py` | `sources/{source}/rdf.py` |
| `mappings/graph.py` | `rdf/graph.py` |
| `mappings/iris.py` | `rdf/iris.py` |
| `semantic_processing/location_*` | `enrichment/locations.py` |
| `semantic_processing/org_*` | `enrichment/organizations.py` |
| `semantic_processing/disaster_classifier.py` | `enrichment/disaster_types.py` |
| `semantic_processing/event_resolver.py` | `resolution/` modules |
| `validate/validate.py` | `quality/shacl.py` |
| `pipeline/run_{source}.py` | `sources/{source}/job.py` |
| `pipeline/build_alignment.py` | `resolution/job.py` or CLI command |
| `pipeline/load_graphdb.py` | `io/graphdb.py` plus publish CLI |
| `fetch/airflow_fetch_sources_template.py` | `orchestration/airflow/` |

Do not perform this mapping in one large rename. Introduce the package and
compatibility entry points first, then migrate one source at a time.

## Standard Data Contract

Every stage should consume an explicit input specification and return an
artifact result. Avoid stage functions that discover arbitrary files from a
working directory.

### Input specification

```text
source
source_version or as_of_date
input_uri
input_checksum
run_id
partition, if applicable
configuration_version
```

### Artifact result

```text
run_id
source
stage
output_uri
output_checksum
record_count
rejected_count
warning_count
started_at
completed_at
code_version
container_image_digest
model_versions
validation_status
```

### Storage layout

```text
raw/source=dromic/year=2025/...
parsed/source=dromic/year=2025/run_id=<id>/...
curated/source=dromic/year=2025/run_id=<id>/...
published/rdf/source=dromic/run_id=<id>/part-0001.ttl
resolution/run_id=<id>/alignments.ttl
resolution/run_id=<id>/dedup_registry.json
quarantine/source=dromic/run_id=<id>/...
manifests/source=dromic/run_id=<id>.json
```

Raw inputs must be immutable. Published artifacts should be written under a
versioned run prefix before an active-version marker or GraphDB graph is
updated.

CSV and JSON may remain as parser-debugging artifacts. Prefer Parquet for typed
curated tables when data is tabular, while keeping Turtle or another appropriate
RDF format for published graphs.

## Configuration and Secrets

Create one typed configuration module with these precedence rules:

1. Explicit CLI arguments.
2. Environment variables or orchestrator-provided values.
3. Safe development defaults.

Provide explicit deployment profiles without branching the business logic:

- `local`: local paths, local logs, direct Python execution, and local GraphDB.
- `onprem`: local or network-mounted storage, injected secrets, scheduled or
  containerized jobs, and an on-premise GraphDB endpoint.
- `cloud`: object storage, managed secrets, managed logging, and workflow jobs.

At minimum, configure:

- Raw, parsed, curated, published, quarantine, and manifest roots.
- GraphDB host, repository, request timeout, and authentication source.
- PSGC, ontology, organization-registry, and SHACL resource locations.
- Batch size, worker count, and retry policy.
- NLP model identifiers, revisions, and cache paths.
- Structured logging level and format.

Credentials must come from a cloud secret manager or orchestration secret
backend. Do not store passwords or tokens in tracked configuration files.

Remove `config.ini` after confirming no external process depends on its obsolete
GDA RML settings.

## Runtime and Container Strategy

Start with two images and split further only when operational evidence justifies
it.

### Image 1: core ETL

Contains:

- Polars and Pandas compatibility dependencies.
- RDFLib and SHACL validation.
- Location, organization, and disaster-type enrichment.
- Source transforms, RDF mappings, alignment, and GraphDB publishing.

### Image 2: document acquisition and parsing

Contains only the relevant system dependencies for:

- Selenium and a headless browser.
- PDF parsing and OCR.
- Docling or other document-conversion tooling.

The current `docx2pdf` workflow requires Microsoft Word on Windows or macOS.
Choose one cloud-compatible approach before containerizing DROMIC parsing:

- Replace it with a tested Linux-compatible converter.
- Use a managed document-conversion service.
- Run this isolated step on a managed Windows worker.

### Image requirements

- Pin the Python version and base-image digest.
- Run as a non-root user.
- Do not copy local data, logs, `.venv`, caches, or secrets into the image.
- Pre-fetch NLP models at image build time or mount a versioned model cache.
- Pin model revisions rather than downloading an unspecified latest revision.
- Add a health-free batch entry point that exits nonzero on failure.
- Generate an image software bill of materials during CI.

## Orchestration Design

Use one workflow per source and one downstream integration workflow.

### Source workflow

```text
fetch -> parse -> transform/enrich -> RDF map -> SHACL validate -> publish
```

### Integration workflow

```text
wait for required source publications
    -> build alignment
    -> validate alignment
    -> replace GraphDB contexts
    -> publish run manifest and metrics
```

### Task rules

- A retry with the same inputs must produce the same output.
- A task writes only to its run-specific artifact location.
- A task never relies on another worker's local filesystem.
- Large artifacts are passed by URI, not through task metadata.
- Each task has bounded retries, timeout, memory, and CPU.
- A partial run must not update an active production graph.
- Source workflows may fan out by year, report, or event folder where safe.
- GraphDB publishing must limit concurrency by destination context.

For the current workload, prefer managed container jobs and a managed workflow
service. Adopt managed Airflow when backfills, dependencies, scheduling, or
operational visibility justify its additional platform overhead.

## Testing and Data Quality Strategy

### Unit tests

- Numeric and date normalization.
- Header and token mapping.
- Location and organization matching.
- Disaster classification rules and model-routing behavior.
- Deterministic IRI generation.
- Impact-row filtering and null handling.
- Entity-resolution feature extraction and scoring.
- Storage URI and manifest behavior.

### Golden tests

Maintain a small representative fixture for each source:

- Input document, workbook, or parsed table.
- Expected normalized entities.
- Expected key RDF triples or canonical Turtle output.
- Expected warnings and rejected rows.

Avoid large or confidential production fixtures.

### Integration tests

- Source pipeline from fixture to RDF.
- SHACL success and expected failure cases.
- Atomic GraphDB replacement using a disposable repository or named graph.
- Failed GraphDB input rolls back without modifying the prior graph.
- Alignment reruns preserve deterministic cluster identifiers.

### Production quality gates

- Parsed schema validation.
- Required field and datatype checks.
- Unexpected-column detection.
- Rejected-record threshold.
- RDF parse validation.
- Mandatory SHACL conformance.
- Nonzero output and expected record-count bounds.
- Input and output checksum recording.

Validation should be enabled by default in production. Any bypass must be
explicit, logged, and unavailable to normal scheduled workflows.

## Observability and Operational Lineage

Write structured logs to standard output. Every log entry should include, where
applicable:

- `run_id`
- `source`
- `stage`
- `partition`
- `input_uri`
- `output_uri`
- `record_count`
- `triple_count`
- `duration_ms`
- `validation_status`
- `error_type`

Publish metrics for:

- Successful and failed runs.
- Stage duration.
- Input, accepted, rejected, and output counts.
- Location and organization match rates.
- Disaster-classification confidence distribution.
- SHACL violations by constraint.
- GraphDB upload duration and failure count.

The domain-level PROV data in RDF should remain. Add operational lineage through
run manifests first; adopt OpenLineage later if a compatible orchestrator or
metadata service is introduced.

## Security and Reliability

- Use a distinct service identity for fetching, processing, and GraphDB loading.
- Give each identity access only to its required storage prefixes and services.
- Keep GraphDB on a private network endpoint when deployed in cloud.
- Require TLS and managed-secret injection for GraphDB credentials.
- Isolate untrusted PDF, DOCX, and web content processing with CPU, memory, and
  execution-time limits.
- Scan dependencies and images in CI.
- Retain immutable raw inputs and versioned manifests for recovery.
- Back up GraphDB before ontology, ruleset, or large schema changes.
- Use atomic named-graph replacement rather than clear-then-upload behavior.

## Staged Refactoring Roadmap

The stages below deliberately separate internal refactoring from deployment
changes. Do not start a stage until the preceding stage meets its exit criteria.
Tag or otherwise record a known-good revision at every stage boundary.

### Stage 0: Freeze and characterize current behavior

Objective: create a safety net without changing module locations or runtime
behavior.

Structural state: keep the current `fetch/`, `parse/`, `transform/`,
`semantic_processing/`, `mappings/`, and `pipeline/` directories unchanged.

Changes:

- [ ] Record the successful local command for every source.
- [ ] Select a small, representative fixture for every source.
- [ ] Capture output counts, deterministic IRIs, and representative RDF triples.
- [ ] Add golden-output comparisons for the most important RDF behavior.
- [ ] Document external dependencies such as Chrome, OCR, Word, and GraphDB.
- [ ] Inventory all imports, path assumptions, debug outputs, and entry points.

Compatibility guarantee: all current commands and directory conventions remain
unchanged.

Exit criteria:

- Every source has a reproducible baseline or a documented reason it cannot yet
  be reproduced.
- Golden comparisons detect changes to key RDF triples and IRIs.
- A known-good revision is recorded for rollback.

### Stage 1: Stabilize the existing layout

Objective: remove unsafe runtime assumptions before moving code.

Structural state: production modules remain in their current directories; add
only foundational files such as `pyproject.toml`, `tests/`, and central
configuration helpers.

Changes:

- [ ] Add `pyproject.toml` with the supported Python version and development
  commands.
- [ ] Add typed configuration with `local`, `onprem`, and `cloud` profiles.
- [ ] Replace string-concatenated paths with `Path` or storage abstractions.
- [ ] Make pipeline inputs and outputs explicit CLI arguments.
- [ ] Remove or gate fixed debug writes such as `sam.csv`, `hakdog.csv`, and
  `dump/assistance.csv`.
- [ ] Pin dependencies with a lock or constraints file.
- [ ] Add unit tests around normalization, IRI generation, and impact filtering.
- [ ] Add CI for tests and package metadata validation.

Compatibility guarantee: running from `etl/` continues to work exactly as it
does today. New explicit paths and profiles are additive.

Exit criteria:

- Local runs no longer depend on accidental debug files or implicit output
  destinations.
- The test suite runs from a clean checkout.
- The `local` profile reproduces baseline RDF.

### Stage 2: Introduce the package shell and shared interfaces

Objective: establish the target package without immediately relocating every
source.

Structural state:

```text
etl/
|-- pyproject.toml
|-- src/sakunagraph_etl/
|   |-- config.py
|   |-- cli/
|   |-- io/
|   |-- rdf/
|   `-- quality/
|-- tests/
`-- fetch/, parse/, transform/, mappings/, pipeline/  # still active
```

Changes:

- [ ] Add the installable `sakunagraph_etl` package and unified CLI shell.
- [ ] Define interfaces for storage, manifests, SHACL validation, and GraphDB
  publication.
- [ ] Implement the local-filesystem adapter first.
- [ ] Move only clearly shared, stable utilities such as IRI construction and
  graph helpers.
- [ ] Leave compatibility imports or re-exports at their original locations.
- [ ] Add console commands that initially delegate to existing pipeline runners.

Compatibility guarantee: both `python -m pipeline...` and the new `sakuna-etl`
commands invoke the same implementation and produce equivalent RDF.

Exit criteria:

- The package installs in a clean virtual environment.
- Commands run from the repository root, `etl/`, and an unrelated working
  directory.
- Local and on-premise execution require no cloud SDK or service.

### Stage 3: Migrate one reference source vertically

Objective: prove the source-oriented layout from input to validated RDF before
moving the other sources.

Use EM-DAT as the default reference because it has fewer browser and document
conversion dependencies. Choose DROMIC instead only if resolving its acquisition
and conversion risks is the explicit priority.

Structural state:

```text
src/sakunagraph_etl/sources/emdat/
|-- parse.py
|-- transform.py
|-- rdf.py
`-- job.py
```

Changes:

- [ ] Move the reference source's parser, transform, mapping, and runner behind
  its source package.
- [ ] Keep data contracts between stages explicit and typed.
- [ ] Use the new configuration, storage, validation, and manifest interfaces.
- [ ] Leave thin modules at the old paths that delegate to the new package.
- [ ] Add unit, golden, and end-to-end fixture tests for the migrated source.
- [ ] Compare output checksums or canonical RDF with the Stage 0 baseline.

Compatibility guarantee: the old source command remains supported and produces
the same semantic output as the new command.

Exit criteria:

- The reference source runs end to end through both entry points.
- Any RDF difference is reviewed and documented as intentional.
- No other source needs to import the reference source's private modules.

### Stage 4: Migrate the remaining sources incrementally

Objective: complete source ownership without a repository-wide rename.

Recommended order: GDA, PSGC, NDRRMC, then DROMIC. Reorder only for a concrete
delivery need. Complete and release each source before starting the next.

For each source:

- [ ] Move fetch, parse, transform, RDF mapping, and job code into
  `sources/{source}/` as applicable.
- [ ] Preserve old entry points with compatibility wrappers.
- [ ] Replace implicit file discovery with explicit inputs or manifests.
- [ ] Add source-specific unit, golden, and integration tests.
- [ ] Run the source locally and with the `onprem` profile.
- [ ] Compare output with its Stage 0 baseline.

Complex-module work within this stage:

- [ ] Split DROMIC parsing by metadata extraction, table extraction,
  normalization, and output responsibilities.
- [ ] Split NDRRMC parsing by extraction mode and report component.
- [ ] Split NDRRMC transforms and mappings by impact family where this reduces
  coupling without changing RDF behavior.
- [ ] Rename files containing hyphens to importable underscore names.
- [ ] Move executable smoke tests out of production modules.

Compatibility guarantee: a source's original command is removed only after its
new command has been released, documented, and used successfully in the target
environment.

Exit criteria:

- Every source has an independently testable `job.py`.
- All source workflows pass golden and SHACL validation.
- Remaining old modules are wrappers rather than duplicate implementations.

### Stage 5: Refactor cross-source and publication services

Objective: migrate the components used by multiple source jobs after their
interfaces have been proven.

Changes:

- [ ] Split event resolution into feature extraction, blocking, scoring,
  clustering, and registry modules.
- [ ] Unify location matcher implementations behind one interface while
  preserving required source-specific strategies.
- [ ] Move organization and disaster-type enrichment into shared services.
- [ ] Move alignment orchestration behind a dedicated CLI command.
- [ ] Move GraphDB access behind the publication interface while preserving
  atomic named-graph replacement.
- [ ] Add rollback tests using a disposable GraphDB repository or named graph.
- [ ] Require SHACL success before publication in `onprem` and `cloud` profiles.

Compatibility guarantee: existing alignment and GraphDB commands remain as
wrappers until operational scripts have migrated.

Exit criteria:

- Cross-source alignment preserves deterministic cluster identifiers.
- Failed RDF or GraphDB requests cannot replace the active graph.
- All production behavior is owned by the package; legacy locations contain
  wrappers only.

### Stage 6: Add artifact versioning and deployment packaging

Objective: make the refactored application reproducible across local, on-premise,
and cloud runtimes.

Changes:

- [ ] Add run-specific artifact paths, checksums, manifests, and quarantine.
- [ ] Make local storage the reference implementation of the storage interface.
- [ ] Add network-filesystem behavior needed by the `onprem` profile.
- [ ] Add the selected object-storage adapter for the `cloud` profile.
- [ ] Replace modification-time discovery and `_needs_rerun.txt` state with
  explicit manifests.
- [ ] Build the minimal core ETL container.
- [ ] Build a separate document/browser image if DROMIC or NDRRMC requires it.
- [ ] Pin base images and NLP model revisions and run containers as non-root.
- [ ] Provide local Python and Docker Compose/on-premise run instructions.

Compatibility guarantee: containers are optional. The `local` profile remains
fully runnable in a virtual environment.

Exit criteria:

- One input version can be reproduced from its manifest in all supported
  profiles.
- Retries do not duplicate or corrupt published artifacts.
- Each RDF artifact has an input checksum, output checksum, validation status,
  and code version.

### Stage 7: Add orchestration and production hardening

Objective: automate the stable application without embedding business logic in
the workflow platform.

Changes:

- [ ] Define source and integration workflows using CLI commands as task
  boundaries.
- [ ] Pass artifact URIs and small metadata values between tasks.
- [ ] Add schedules, backfills, bounded retries, timeouts, and concurrency
  controls.
- [ ] Provide an on-premise scheduler option alongside the selected managed
  workflow service.
- [ ] Add structured logs, metrics, alerts, and operational dashboards.
- [ ] Add infrastructure as code for cloud resources.
- [ ] Add GraphDB backup, restore, full-rebuild, and disaster-recovery runbooks.
- [ ] Add storage retention, dependency scanning, and image update automation.
- [ ] Evaluate OpenTelemetry and OpenLineage after run manifests stabilize.

Compatibility guarantee: orchestration invokes the same package commands used
manually; a workflow service is not required to operate the ETL locally.

Exit criteria:

- Failed validation prevents alignment and GraphDB publication.
- A workflow resumes from a failed stage without needlessly repeating completed
  immutable stages.
- Operators can identify the source, stage, input, and run ID for a failure.
- On-premise and cloud recovery procedures have been tested.

## Stage Gates and Change Control

At the end of every stage:

1. Run unit, golden, integration, and SHACL tests applicable to that stage.
2. Compare output counts, deterministic identifiers, and canonical RDF with the
   recorded baseline.
3. Run a local smoke test and, from Stage 3 onward, an on-premise-profile smoke
   test.
4. Record intentional behavior changes in an architecture decision record.
5. Tag the known-good revision before beginning the next stage.
6. Remove no compatibility wrapper unless its callers and replacement command
   are documented.

## Recommended First Refactoring Milestone

Complete Stages 0 through 3 before adding cloud-provider-specific code. This
creates the test safety net, package shell, deployment profiles, and one proven
source-oriented vertical slice while keeping the current pipeline operational.

The first milestone is complete when:

1. Existing commands still work from `etl/`.
2. The package is installable and the new CLI runs from any working directory.
3. Local, on-premise, and cloud configuration profiles are defined, with local
   behavior fully implemented.
4. Fixed debug writes and implicit output paths are removed.
5. Golden and SHACL tests run in CI.
6. EM-DAT, or the explicitly selected reference source, runs through the new
   source package and reproduces its baseline RDF.

## Decision Log

Record architecture decisions here or link to separate ADR files.

| Decision | Status | Notes |
|---|---|---|
| Managed container jobs as the initial compute model | Proposed | Prefer lower operational overhead over an always-running cluster. |
| Hybrid source-oriented Python package | Proposed | Keeps source ownership local while sharing RDF and enrichment infrastructure. |
| Object storage for inter-stage artifacts | Proposed | Required for distributed workers and reproducible retries. |
| Parquet for typed curated tables | Proposed | Keep CSV/JSON when human inspection is the primary purpose. |
| Mandatory production SHACL validation | Proposed | Development bypass must be explicit. |
| Atomic named-graph replacement | Accepted | Implemented in `pipeline/load_graphdb.py`. |
| Cloud provider and workflow service | Open | Select after reference-source requirements are measured. |
| DROMIC DOCX conversion approach | Open | Linux converter, managed service, or Windows worker. |

## Reference Guidance

- Python packaging: <https://packaging.python.org/en/latest/guides/writing-pyproject-toml/>
- Airflow production practices: <https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html>
- Docker build practices: <https://docs.docker.com/build/building/best-practices/>
- Application configuration: <https://www.12factor.net/config>
- Apache Parquet: <https://parquet.apache.org/>
- OpenTelemetry: <https://opentelemetry.io/docs/>
- OpenLineage: <https://openlineage.io/>
- GraphDB graph replacement: <https://graphdb.ontotext.com/documentation/11.0/replace-graph.html>
