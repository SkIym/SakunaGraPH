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
| `imports/catalog-v001.xml` | XML catalog that resolves local ontology imports. |
| `shapes/shapes.ttl` | SHACL shapes for SakunaGraPH event and impact RDF. |
| `shapes/psgc/shapes.ttl` | SHACL shapes for Philippine Standard Geographic Code (PSGC) RDF. |
| `validation/competency_questions.md` | SPARQL competency questions for evaluating the graph. |
| `validation/neontometrics-2.csv` | Ontology metrics export. |
| `validation/pitfall-scanner-results-2.xml` | OOPS! Pitfall Scanner validation output. |

## Scope

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

The documented competency questions are in
[`validation/competency_questions.md`](validation/competency_questions.md).
OOPS! results and ontology metrics are retained in `validation/` as reference
artifacts.

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
