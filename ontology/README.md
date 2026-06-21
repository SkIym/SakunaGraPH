# SakunaGraPH Ontology

SakunaGraPH is an OWL ontology for modeling Philippine disaster knowledge. It captures disaster events, incidents, impacts, responses, preparedness actions, geographic hierarchy, and source provenance. The model extends the [beAWARE ontology](https://github.com/beAWARE-project/ontology) and is used by the ETL pipeline to populate the knowledge graph.

## What is in this folder

| File | Purpose |
|------|---------|
| `sakunagraph.ttl` | Main ontology in Turtle format |
| `competency_questions.md` | SPARQL competency questions used to validate the graph |
| `run_cqs.py` | Helper script that runs the competency questions against a local GraphDB repository |
| `catalog-v001.xml` | Ontology catalog used by RDF/OWL tooling |
| `pitfall-scanner-results*.xml` | OOPS! validation output |
| `neontometrics-2.csv` | Ontology metrics export |

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

## Imported Vocabularies

- `geo:` GeoSPARQL 1.1 for spatial features
- `prov:` PROV-O for source and provenance modeling
- `skos:` SKOS for the disaster-type concept scheme
- `qudt:` QUDT for numeric quantities and currency values
- `baw:` beAWARE classes and properties reused by SakunaGraPH

## Core Model

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

### Preparedness and Response

- `:PreemptiveEvacuation`
- `:Rescue`
- `:Assistance`
- `:DeclarationOfCalamity`
- `:Recovery`
- `:Warning`

### Geography

- `:Country`, `:IslandGroup`, `:Region`, `:Province`, `:City`, `:Municipality`, `:Barangay`, `:SubMunicipality`
- Geographic membership is modeled through `:isPartOf` chains rather than a separate region property

### Provenance

- `:Source` stores report metadata such as report name, URL, format, and acquisition dates
- `prov:wasDerivedFrom` and related provenance links connect events to their source materials

## Disaster Type Scheme

The ontology includes a SKOS concept scheme for disaster classification based on EM-DAT.

Top-level branches:

- `:Natural`
- `:Technological`

Examples of major branches and leaves:

- Natural: `:Biological`, `:Climatological`, `:Geophysical`, `:Hydrological`, `:Meteorological`
- Technological: `:ArmedConflict`, `:IndustrialAccident`, `:MiscellaneousAccident`, `:Transport`

Leaf concepts include `skos:note` annotations that support fuzzy matching in the ETL semantic classifier.

## Validation And Queries

The ontology has been checked with [OOPS! Pitfall Scanner](https://oops.linkeddata.es/). The reported XML files in this folder contain the validation results.

Twenty competency questions are documented in [`competency_questions.md`](competency_questions.md). The questions are also runnable against a local GraphDB repository through [`run_cqs.py`](run_cqs.py):

```bash
python run_cqs.py
```

The script expects GraphDB to be available at `http://localhost:7200/repositories/SakunaGraph` and writes an HTML results file next to the script.

## Notes

- The ontology is intentionally aligned with the ETL pipeline, so class and property names reflect the structure of the source disaster reports.
- For the full class and property inventory, inspect `sakunagraph.ttl` directly.
