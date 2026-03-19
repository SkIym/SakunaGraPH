# SakunaGraPH Ontology

OWL ontology for modeling Philippine disaster events, their impacts, responses, and geographic context. Extends the [beAWARE ontology](https://github.com/beAWARE-project/ontology) with domain-specific classes for Philippine disaster management.

## Files

| File | Description |
|------|-------------|
| `sakunagraph.ttl` | Main ontology (Turtle format) |
| `sakunaph.owl` | WebProtege export (RDF/XML format) |
| `beAWARE_ontology.owl` | Imported base ontology for disaster events and locations |
| `pitfall-scanner-results*.xml` | OOPS! pitfall scanner validation results |

## Namespace

```
Base IRI: https://sakuna.ph/
```

## Imports

- **GeoSPARQL 1.1** — geospatial representation and queries
- **W3C PROV** — provenance tracking for data sources and reports
- **W3C SKOS** — concept scheme organization (disaster types)
- **QUDT** — quantities, units, and currency handling for monetary values
- **beAWARE** — base disaster event and location concepts

## Core Concepts

### Disaster Events
- `DisasterEvent` — top-level event class (major events vs. incidents)
- `DisasterType` — hierarchical classification via SKOS (natural vs. technological)
- `Incident` — sub-events within a major disaster event

### Impact
- `AffectedPopulation` — displaced families, served individuals, evacuation centers
- `Casualties` — deaths, injuries, missing persons (by casualty type)
- `HousingDamage`, `InfrastructureDamage`, `AgriculturalDamage` — sectoral damage
- `PowerDisruption`, `CommunicationLineDisruption`, `WaterDisruption` — lifeline impacts
- `RoadAndBridgesDamage`, `SeaportDisruption`, `AirportDisruption` — transport impacts

### Response & Recovery
- `Relief`, `Assistance` — aid distribution and fund allocation
- `PreemptiveEvacuation`, `Rescue` — preparedness and response actions
- `Recovery` — reconstruction costs, insured damages
- `DeclarationOfCalamity` — government declarations

### Geography
- `Region`, `Province`, `Municipality`, `Barangay` — Philippine administrative hierarchy
- Linked to PSGC codes via `isPartOf` relationships

### Provenance
- `prov:Entity` — source reports and documents
- Tracks report names, dates obtained, recording agencies

## Disaster Type Scheme

`skg-disaster-type-scheme.ttl` defines a two-tier SKOS hierarchy based on EM-DAT:

- **Natural**: Biological, Climatological, Extraterrestrial, Geophysical, Hydrological, Meteorological
- **Technological**: Industrial accidents, transport accidents, armed conflicts

Each leaf concept includes a `skos:definition` used by the ETL semantic classifier for fuzzy matching.

## Validation

Ontology quality checked with [OOPS! Pitfall Scanner](https://oops.linkeddata.es/). Results in `pitfall-scanner-results*.xml`. Remaining minor pitfalls (missing annotations) are acknowledged but deprioritized.
