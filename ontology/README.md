# SakunaGraPH Ontology

SakunaGraPH is an OWL ontology for modeling Philippine disaster knowledge. It
captures disaster events, incidents, impacts, responses, preparedness actions,
geographic hierarchy, and source provenance. The model extends the
[beAWARE ontology](https://github.com/beAWARE-project/ontology) and is used by
the ETL pipeline to populate the knowledge graph.

The published knowledge graph and ontology baseline is available in the
[`sakunagraphv.1.0` release](https://github.com/SkIym/SakunaGraPH/releases/tag/sakunagraphv.1.0).

## Folder layout

| Path | Purpose |
|------|---------|
| `sakunagraph.ttl` | Main SakunaGraPH OWL ontology in Turtle format. |
| `disaster_type_scheme.ttl` | SKOS disaster-type classification aligned with EM-DAT. |
| `imports/beAWARE_ontology.owl` | Local copy of the imported beAWARE ontology. |
| `imports/geosparql-1.1.ttl` | Pinned GeoSPARQL 1.1 ontology snapshot. |
| `imports/skos-2009.rdf` | Pinned W3C SKOS Recommendation namespace document. |
| `imports/prov-o-20130430.owl` | Pinned W3C PROV-O Recommendation snapshot. |
| `imports/catalog-v001.xml` | XML catalog that resolves local ontology imports. |
| `imports/README.md` | Import snapshot checksums, provenance, licenses, and offline status. |
| `imports/THIRD_PARTY_NOTICES.md` | Upstream attribution and redistribution notices. |
| `shapes/shapes.ttl` | SHACL shapes for SakunaGraPH event and impact RDF. |
| `shapes/psgc/shapes.ttl` | SHACL shapes for Philippine Standard Geographic Code (PSGC) RDF. |
| `docs/requirements.md` | Normative ontology requirements OR-01 through OR-08. |
| `docs/paper-evidence-map.md` | Traceability from the project paper to ontology terms, CQs, shapes, tests, and gaps. |
| `docs/modeling-decisions/` | Paper-backed and maintainer-approved ontology decision records. |
| `docs/versioning-policy.md` | Stable namespace, SemVer, migration, import, and release policy. |
| `release/manifest.schema.json` | Required structure for checksum-bearing ontology release manifests. |
| `release/README.md` | Release identity and manifest preparation guide. |
| `validation/README.md` | Reproducible competency-suite workflow and GraphDB evidence boundary. |
| `validation/competency-manifest.json` | Machine-readable metadata for all 20 competency questions. |
| `validation/queries/`, `validation/expected/`, `validation/fixtures/` | Executable SPARQL, normalized expected rows, and the immutable synthetic graph. |
| `validation/competency_questions.md` | Human-readable competency-question index and transcription notes. |
| `validation/neontometrics-2.csv` | Ontology metrics export. |
| `validation/pitfall-scanner-results-2.xml` | OOPS! Pitfall Scanner validation output. |

## Scope

The [ontology requirements](docs/requirements.md) define the current engineering contract, while
the [paper evidence map](docs/paper-evidence-map.md) separates paper-era findings from later
repository evidence and open verification gaps. The
[ontology decision records](docs/modeling-decisions/README.md) preserve the rationale for the
modeling patterns, accepted maintainer choices, deferred scope, and implementation gaps.

The ontology models:

- disaster events and incidents
- impact categories such as casualties, affected population, damage, and service disruptions
- preparedness and response actions such as evacuation, rescue, assistance, and calamity declarations
- Philippine administrative geography down to barangay level
- provenance for source documents and reports
- a SKOS disaster-type classification aligned with EM-DAT

## Namespace

```
Base IRI: https://sakuna.ph/
```

The historical ontology IRI and public term namespace are `https://sakuna.ph/`. As of 2026-09-14,
the maintainer plans to acquire but does not yet own that domain. Existing IRIs remain historical
public identifiers, but the repository does not claim that they are currently dereferenceable or
under project control. Version numbers do not appear in class, property, concept, or data-resource
IRIs.

Cluster identifiers use `https://sakuna.ph/cluster/{uuid}` so all project-owned identifiers remain
under one namespace. ODR-0016 changes the cluster membership graph without reminting cluster IRIs.

## Versioning

The historical combined knowledge-graph/ontology release remains tagged `sakunagraphv.1.0`.
Future ontology releases use three-part semantic versions and are versioned separately from ETL,
API, frontend, and data snapshots. The next approved ontology release candidate is 2.0.0, using the
tag `ontology-v2.0.0`. Releases omit `owl:versionIRI` and use `owl:versionInfo`, the immutable Git
tag, and the checksum manifest as their release identity. The current working ontology is not
stamped yet because namespace control and release validation remain unresolved.

See the [namespace and versioning policy](docs/versioning-policy.md) before changing public IRIs,
OWL axioms, SHACL publication behavior, imports, or release metadata. The
[import snapshot manifest](imports/README.md) records the current offline-resolution status.

## Imported vocabularies

- `geo:` GeoSPARQL 1.1 for spatial features
- `prov:` PROV-O for source and provenance modeling
- `skos:` SKOS for the disaster-type concept scheme
- `qudt:` QUDT for numeric quantities and currency values
- `baw:` beAWARE classes and properties reused by SakunaGraPH

## Core model

### Events

- `:DisasterEvent` is the top-level class for all modeled events.
- `:MajorEvent` represents large, aggregated events.
- `:Incident` represents localized sub-events related to a larger event.

### Impact

- `:AffectedPopulation` for displaced and affected families or persons
- `:Casualties` for deaths, injuries, and missing persons
- `:HousingDamage`, `:InfrastructureDamage`, `:AgricultureDamage`
- `:PowerDisruption`, `:CommunicationLineDisruption`, `:WaterDisruption`
- `:RoadAndBridgesDamage`, `:SeaportDisruption`, `:AirportDisruption`
- `:ClassSuspension`, `:WorkSuspension`, `:FlightDisruption`, `:StrandedEvent`

### Preparedness and response

- `:PreemptiveEvacuation`
- `:Rescue`
- `:Assistance`
- `:DeclarationOfCalamity`
- `:Recovery`
- `:Warning`

### Geography

- `:Country`, `:IslandGroup`, `:Region`, `:Province`, `:City`, `:Municipality`, `:Barangay`, `:SubMunicipality`
- Geographic membership is modeled through `:isPartOf` chains rather than a separate region property.

### Provenance

- `:Source` stores report metadata such as report name, URL, format, and acquisition dates.
- `prov:wasDerivedFrom` and related provenance links connect events to their source materials.

## Disaster type scheme

`disaster_type_scheme.ttl` defines the SKOS concept scheme used by the ETL
classifier. Its top-level branches are `:Natural` and `:Technological`.
`skos:note` annotations on leaf concepts provide matching context for the
semantic classifier.

## Validation

Use `shapes/shapes.ttl` to validate event and impact data, and
`shapes/psgc/shapes.ttl` to validate PSGC data. The pipeline defaults are
implemented by `sakunagraph_etl.quality.shacl` and the package-owned source
jobs under `sakunagraph_etl.sources`.

All 20 competency questions are executable against a frozen synthetic graph. Run the offline
preflight from the repository root:

```bash
python scripts/validate_semantics.py --competency
```

See [`validation/README.md`](validation/README.md) for the GraphDB 11.1.3/OWL2-RL acceptance lane,
the machine-readable report option, and the distinction between the paper baseline and current
test evidence. OOPS! results and ontology metrics remain in `validation/` as reference artifacts.

From the repository root, the infrastructure-free portfolio demo parses the
ontology and shape graphs, verifies the five-source synthetic RDF fixtures,
and executes a SPARQL query:

```bash
python scripts/portfolio_demo.py
```

RDFLib 7.6.0 is the only dependency needed for that smoke demo.

## License and external terms

Original SakunaGraPH ontology content is available under the repository's MIT
license. Imported ontologies and vocabularies remain under their respective
owners' terms. Source data and derived RDF may have separate access,
attribution, and redistribution requirements; see [`DATA_SOURCES.md`](../DATA_SOURCES.md).

## Notes

- The ontology is intentionally aligned with the ETL pipeline, so class and property names reflect the structure of the source disaster reports.
- For the full class and property inventory, inspect `sakunagraph.ttl` directly.
