## mappings

Source-specific modules that translate parsed records into RDF triples for the Sakuna knowledge graph. Each per-source module defines `@dataclass` row types and `*_mapping` functions that consume lists of those rows and write triples into a shared `rdflib.Graph`.

### Shared infrastructure

**`graph.py`** — graph factory and shared namespaces:
- `SKG` (`https://sakuna.ph/`), `PROV`, `QUDT`, `CUR`, `ORG`, `GEO`
- `create_graph()` — returns a fresh `rdflib.Graph` with all namespace prefixes bound
- `add_monetary(g, subject, predicate, value, unit)` — attaches a QUDT `QuantityValue` blank node (numeric value + unit) for monetary fields

**`iris.py`** — deterministic IRI minting for events and their sub-resources:
- `event_uri(source, source_record_id)` — UUIDv5-based event IRI under `https://sakuna.ph/{source}/{uuid}`, so re-runs over the same input always produce the same IRI
- `sub_iri(event_id, segment, r_id=None)` — builds a sub-resource IRI scoped under an event
- Named aliases (`incident_iri`, `aff_pop_iri`, `casualties_iri`, `housing_iri`, `infra_iri`, `power_iri`, `comms_iri`, `rnb_iri`, `seaport_iri`, `water_iri`, `pevac_iri`, `assistance_iri`, `relief_iri`, `recovery_iri`, `damage_gen_iri`, `doc_iri`, etc.) — thin wrappers around `sub_iri` that fix the segment slug
- `prov_iri(report)` and `org_iri(slug)` — IRIs for provenance documents and organizations

### Source mappings

**`gda_mapping.py`** — Government Disaster Assessment (GDA) records. Defines dataclasses and `*_mapping` functions for:
- `Event` / `Incident` (events tagged `eventClass == "I"` are emitted as `SKG.Incident`, otherwise `SKG.MajorEvent`; incidents are linked back to their parent event via `SKG.hasRelatedIncident`)
- `Preparedness`, `Evacuation` (preemptive), `Rescue`, `DeclarationOfCalamity` — declaration text without "calamity" is downgraded to a `Preparedness` announcement
- `AffectedPopulation`, `Casualties` (split into one `SKG.Casualties` node per `DEAD`/`INJURED`/`MISSING` count)
- `HousingDamage`, `InfrastructureDamage`, `DamageGeneral` — monetary fields go through `_to_millions` (values > 1000 are assumed to be in pesos and divided to PHP millions) and `add_monetary`
- `PowerDisruption`, `CommunicationLineDisruption`, `RoadAndBridgesDamage`, `SeaportDisruption`, `WaterDisruption`
- `Assistance` (allocated funds, agency/LGU presence, NGO/international amounts) and `Relief` (typed by `goods`/`water`/`clothing`/`medicine`/`unspecified`)
- `Recovery` (SRR, policy changes, post-disaster training, structure cost)

Free-text agency/org fields (`agencyLGUsPresent*`, `internationalOrgsPresent`, `rescueUnit`, `itemQty`) are run through `ORG_RESOLVER.split_and_resolve` so each resolved organization is attached as `SKG.contributingOrg`.

**`emdat.py`** — EM-DAT records. Dataclasses for `Event` (with magnitude/lat/lon and `lastUpdateDate`/`entryDate`), `Assistance`, `Recovery`, `DamageGeneral` (insured + general + CPI), `Casualties`, `AffectedPopulation`, and `Source`. Emits triples under the `emdat` source namespace.

**`ndrrmc.py`** — NDRRMC situation report records. Dataclasses for `Event` and the various sub-resources used by NDRRMC reports (preemptive evacuation, class/work suspensions, stranded events, airport/flight disruptions, agriculture damage, etc.), emitted under the `ndrrmc` source namespace.

### Conventions

- Every source mapping resolves event IRIs through `event_uri("<source>", record.id)` so identical input IDs always collapse to the same IRI across runs.
- Sub-resources are minted via the named `*_iri` helpers in `iris.py` — never construct sub-IRIs by hand.
- Monetary amounts always go through `add_monetary(...)` with a QUDT unit (typically `SKG.PHP_millions`); use `_to_millions` first if the source value may be in raw pesos.
- Pipe-separated location/type strings (`"loc1|loc2"`) are split by `_add_location` / `_add_type_iri` in `gda_mapping.py` and emitted as multiple triples.
