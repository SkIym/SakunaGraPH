# Semantic validation

This directory turns the paper's 20 competency questions into a frozen regression suite. The
paper's statement that all 20 questions succeeded remains a historical result for its graph
snapshot; this suite is current repository evidence and does not retroactively reproduce the
paper's dataset.

The study integrated four disaster datasets (NDRRMC, DROMIC, GDA, and EM-DAT). PSGC is the
authoritative geographic reference dataset, so repository-wide descriptions count five integrated
sources. The competency fixture is synthetic and contains no restricted source records.

## Files

| Path | Purpose |
| --- | --- |
| `competency-manifest.json` | Query category, paper pages, requirement IDs, columns, assertion, ordering, fixture, and inference contract |
| `fixtures/competency-v1.ttl` | Immutable synthetic RDF fixture and GraphDB inference probes |
| `queries/cq01.rq` through `queries/cq20.rq` | One executable SPARQL query per paper competency question |
| `expected/cq01.csv` through `expected/cq20.csv` | Exact expected rows using normalized N-Triples terms |
| `reports/` | Ignored location for generated machine-readable reports |
| `competency_questions.md` | Human-readable index and historical transcription notes |

All six categories have executable assertions: event/type classification, casualties/population,
damage, service disruptions, response/preparedness, and provenance.

## Offline development preflight

Install the production package and its pinned dependencies from `sakunagraph_etl/`, then run the
command from the repository root:

```bash
cd sakunagraph_etl
python -m pip install --editable . --constraint constraints.txt
cd ..
python scripts/validate_semantics.py --competency
```

The command verifies the fixture checksum, parses every RDF/OWL/Turtle artifact under `ontology/`,
applies an OWL 2 RL closure with `owlrl`, checks inference probes, runs all 20 queries, validates
their declared columns, and compares normalized RDF terms with the expected CSVs. It exits nonzero
for a parse, catalog, inference, query, column, or row mismatch.

RDFLib plus `owlrl` is explicitly a portable preflight. It is not evidence that RDFLib implements
the same behavior as GraphDB.

To write a machine-readable report:

```bash
python scripts/validate_semantics.py --competency \
  --report ontology/validation/reports/competency-local.json
```

## GraphDB production-acceptance lane

Release acceptance uses GraphDB 11.1.3 with the `OWL2-RL` ruleset, as required by ODR-0015. Create
a dedicated disposable repository with that exact configuration and load only:

1. the four snapshots in `ontology/imports/`;
2. `ontology/sakunagraph.ttl`;
3. `ontology/disaster_type_scheme.ttl`; and
4. `ontology/validation/fixtures/competency-v1.ttl`.

Then run:

```bash
python scripts/validate_semantics.py --competency --engine graphdb \
  --graphdb-endpoint http://localhost:7200/repositories/sakunagraph-competency \
  --report ontology/validation/reports/competency-graphdb.json
```

Set both `GRAPHDB_READ_ONLY_USERNAME` and `GRAPHDB_READ_ONLY_PASSWORD` when authentication is
required. The runner performs SELECT/ASK requests only and never writes to or clears the
repository. It refuses to execute the 20 cases unless the fixture marker and the subclass,
property-chain, and transitivity inference probes are present. Because a read-only SPARQL endpoint
does not expose trustworthy administrative configuration, the report distinguishes the pinned
version/ruleset declaration from the behavior verified by those probes.

Use a dedicated repository: additional data changes exact result rows and correctly fails the
suite. A GraphDB report, not the RDFLib preflight report, is the production inference artifact.

## Expected-result maintenance

`competency-v1.ttl` and its digest are immutable for a published suite version. A deliberate
fixture or query change requires review of its requirement and paper traceability, a new fixture
version when prior evidence must remain reproducible, and a semantic diff of every changed CSV.
After updating the fixture digest in `competency-manifest.json`, maintainers may regenerate CSVs:

```bash
python scripts/validate_semantics.py --competency --update-expected
python scripts/validate_semantics.py --competency
```

Never regenerate expectations merely to make an unexplained semantic change pass.
