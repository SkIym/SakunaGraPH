# SakunaGraPH ontology requirements

## Purpose and authority

This document is the repository-native requirements specification for the SakunaGraPH ontology.
It extracts the study requirements from the [project paper](../../nej-main.pdf) and connects them
to the current semantic artifacts. Paper page numbers refer to the printed page number visible in
the PDF.

The paper remains the scholarly source for the research method and reported results. This document
is normative for current engineering work: when the ontology, ETL mappings, SHACL shapes, or query
behavior changes, the affected requirement and its evidence map must be reviewed.

The keywords **MUST**, **SHOULD**, and **MAY** describe required, recommended, and optional behavior.

## Scope conventions

- **Four disaster sources:** NDRRMC, DROMIC, GDA, and EM-DAT.
- **Fifth integrated reference source:** PSGC, used as the authoritative geographic hierarchy.
- **Paper-era exclusions:** raw climate/sensor observations and full rescue mission structures were
  outside the evaluated data scope.
- **Current reconciliation:** ODR-0010 defines climate values as measurements extracted from
  NDRRMC reports, not raw sensor ingestion. ODR-0011 keeps rescue modeling at report level. These
  post-paper clarifications do not change what the paper evaluated.
- **Open-world model:** an omitted fact means it is not asserted. It does not mean the fact is false
  or equal to zero.

## Evidence status

| Status               | Meaning                                                                                                 |
| -------------------- | ------------------------------------------------------------------------------------------------------- |
| Implemented          | Current ontology and production modules represent the requirement.                                      |
| Partial verification | Implementation exists, but requirement-level automated acceptance is incomplete.                        |
| Maintainer decision  | A post-paper choice was explicitly approved and recorded in a numbered ODR.                          |

## Requirement summary

| ID    | Requirement                                                                        | Current status                                                                      |
| ----- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| OR-01 | Integrate heterogeneous disaster data without requiring absent/null source fields. | Implemented; partial verification                                                   |
| OR-02 | Ground locations in PSGC and support administrative-hierarchy queries.             | Implemented; generic Philippines fallback confirmed; tests incomplete               |
| OR-03 | Represent both major events and localized or human-induced incidents.              | Implemented; partial verification                                                   |
| OR-04 | Preserve source-specific impact instances and their reporting granularity.         | Implemented; partial verification                                                   |
| OR-05 | Trace graph entities to sources, extraction lineage, and alternate records.        | Implemented in core paths; provenance constraints are incomplete                    |
| OR-06 | Represent quantitative values with explicit units and currencies.                  | Implemented; QUDT completeness tests are incomplete                                 |
| OR-07 | Represent preparedness, response, assistance, relief, and service disruption.      | Implemented for the current data scope; full mission structures remain out of scope |
| OR-08 | Classify events with an IRDR/EM-DAT-aligned SKOS taxonomy.                         | Implemented; leaf-only classification is not yet enforced by SHACL                  |

The companion [paper evidence map](paper-evidence-map.md) provides the compact traceability view;
the [ontology decision index](modeling-decisions/README.md) records the design rationale.

## OR-01: Heterogeneous source integration

### Normative requirement

SakunaGraPH MUST map heterogeneous disaster records into a shared RDF model without manufacturing
placeholder values for fields a source does not report. Source-specific detail MAY be retained
without requiring every other source to supply the same field. Every emitted resource MUST retain
enough source identity to be audited.

Per ODR-0018, publication validation MUST combine shared integrity shapes with source-specific
SHACL requirements. `sh:Violation` findings MUST block publication; warnings and informational
findings MUST remain reportable without manufacturing missing facts.

### Paper evidence

- Pages 1-7 describe fragmented, multi-temporal PDF and spreadsheet records and the need for a
  flexible semantic layer.
- Page 7 describes pre-ontology analysis of common, overlapping, and source-specific attributes.
- Pages 13-14 explain how the concept map connected source attributes to classes and properties.

### Supporting source attributes

- All sources: event name/identifier, disaster type, dates, location text, and source identity when
  available.
- NDRRMC and DROMIC: detailed population, casualty, damage, disruption, preparedness, and response
  fields extracted from reports.
- GDA and EM-DAT: historical event and aggregate impact fields from spreadsheets.
- PSGC: standardized administrative identifiers, labels, levels, and parent relationships.

An absent field is represented by no triple. Literal placeholders such as `"N/A"`, `"none"`, or an
empty string MUST NOT be emitted as factual values.

### Semantic realization

- Core terms: `:DisasterEvent`, `:MajorEvent`, `:Incident`, `:Impact`, `:Preparedness`,
  `:Response`, and `:Source`.
- RDF mappings: `sakunagraph_etl/src/sakunagraph_etl/sources/*/rdf.py`.
- Stable resource construction: `sakunagraph_etl/src/sakunagraph_etl/rdf/iris.py`.
- Publication contract: `ontology/shapes/shapes.ttl`.

### Functional evidence

- Competency questions: CQ1-CQ20 collectively exercise the integrated model; CQ1, CQ6, CQ15,
  CQ19, and CQ20 provide representative coverage across event, impact, response, source, and
  cross-source identity.
- SHACL: `sgsh:DisasterEventShape`, `sgsh:ImpactShape`, `sgsh:PreparednessShape`,
  `sgsh:ResponseShape`, and `sgsh:SourceShape`.
- Current checks: `scripts/portfolio_demo.py` validates all five golden source fixture digests,
  parses their combined RDF, and executes a smoke query.

### Acceptance condition

For one legally redistributable fixture from each of NDRRMC, DROMIC, GDA, EM-DAT, and PSGC:

1. the source job produces parseable RDF;
2. omitted source fields produce no placeholder literal;
3. emitted terms conform to the applicable shared and source-specific SHACL profile, with no
   blocking violations;
4. rerunning the same input produces the same resource IRIs and golden N-Triples; and
5. every disaster record has auditable source identity.

### Remaining evidence gap

The golden fixtures prove deterministic RDF but do not yet execute every requirement-level
assertion above in one named OR-01 test. The versioned source-profile and severity system required
by ODR-0018 is also not yet complete. Add both when the semantic validation runner is implemented.

## OR-02: PSGC-grounded location and hierarchy

### Normative requirement

Locations MUST resolve to PSGC-grounded IRIs when an unambiguous administrative unit is available.
The graph MUST preserve the PSGC containment hierarchy and MUST support queries from the most
granular asserted location to its municipality/city, province, region, island group, and country as
applicable.

Only the most granular known location SHOULD be asserted on an event or related record.
Higher-level locations SHOULD be obtained through `:isPartOf` and the documented inference
regime.

### Paper evidence

- Page 7 establishes PSGC as the authoritative reference for geographic normalization and
  hierarchy.
- Page 10 explains the different source granularities, most-granular assertion policy, and
  `:isPartOf` inference.
- Page 11 states that `:hasLocation` property chains support location queries across event,
  impact, preparation, and response instances.

### Supporting source attributes

- Free-text region, province, municipality/city, barangay, and national locations.
- PSGC correspondence codes, administrative level, alternate labels, and parent units.
- Spatially disaggregated impact, preparedness, and response locations from NDRRMC/DROMIC.
- Coarse or missing GDA/EM-DAT location descriptions.

### Semantic realization

- Classes: `:Country`, `:IslandGroup`, `:Region`, `:Province`, `:Municipality`, `:City`,
  `:SubMunicipality`, `:Barangay`, and `:Location`.
- Properties: `:hasLocation` and transitive `:isPartOf`, including `:hasLocation` property-chain
  axioms.
- Matcher: `sakunagraph_etl/src/sakunagraph_etl/enrichment/locations.py`.
- Reference mapping: `sakunagraph_etl/src/sakunagraph_etl/sources/psgc/`.

### Functional evidence

- Primary competency questions: CQ2 and CQ4.
- Additional hierarchy-dependent questions: CQ5-CQ18.
- SHACL: `sgsh:PSGCLocationShape`, `sgsh:PSGCAdministrativeLocationShape`, the country/island
  group/region/province/municipality/city/sub-municipality/barangay shapes, and the
  `hasLocation` properties on event, impact, preparedness, and response shapes.
- Current query tests:
  `api/tests/test_ask_query_compiler.py`,
  `api/tests/test_analysis_events.py`, and
  `api/tests/test_analysis_metrics.py` assert hierarchy traversal in generated SPARQL.

### Acceptance condition

Given a fixture event asserted only at barangay or city level, CQ2 MUST return that location and the
correct containing region under the production GraphDB inference ruleset. A PSGC fixture with an
invalid parent class/code MUST fail SHACL. The same location string in the same parent scope MUST
resolve deterministically.

### Maintainer decision

ODR-0012 confirms the paper-era behavior: a missing or ambiguous raw location MUST map to the
generic `:Philippines` location, consistent with EM-DAT handling. It MUST NOT be mapped to an
invented subnational location. The fallback means that no more specific location was resolved; it
does not by itself prove that the entire country was affected. No separate uncertainty marker is
currently required.

## OR-03: Major-event and incident scope

### Normative requirement

The ontology MUST distinguish large-scale `:MajorEvent` resources from localized or human-induced
`:Incident` resources while allowing both to participate in shared `:DisasterEvent` queries and use
the same disaster-type taxonomy. A resource MUST NOT be inferred as both classes because they are
declared disjoint.

### Paper evidence

- Page 10 introduces the two event scopes and their shared taxonomy.
- Pages 10-11 explain that NDRRMC typically reports major-event impacts while DROMIC commonly
  reports localized incidents.

### Supporting source attributes

- Event/report scope, disaster type, event name, date, and location.
- NDRRMC major situation reports and DROMIC localized incident reports.
- Explicit relationships between a major event and its related incidents where supported.

### Semantic realization

- `:MajorEvent rdfs:subClassOf :DisasterEvent`.
- `:Incident rdfs:subClassOf :DisasterEvent`.
- `:Incident owl:disjointWith :MajorEvent`.
- `:hasRelatedIncident` links major events to incidents.
- `:isRelatedTo` and `:incidentDerivedFrom` support related-event/source traversal.

### Functional evidence

- Competency questions: CQ1, CQ5, CQ19, and CQ20.
- SHACL: `sgsh:DisasterEventShape`, `sgsh:MajorEventShape`, and `sgsh:IncidentShape`.
- Current tests:
  `api/tests/test_event_details.py`,
  `api/tests/test_ask_query_compiler.py`,
  `api/tests/test_analysis_metrics.py`, and
  `frontend/tests/component/VisualInteractionRegression.test.js` exercise both event types.

### Acceptance condition

A fixture containing one major event and one related incident MUST:

1. remain two independently addressable resources;
2. return both in shared disaster-event queries;
3. return the relationship in CQ1;
4. preserve each resource's own source and impact records; and
5. produce a reasoner inconsistency or explicit validation failure if one resource is asserted as
   both `:MajorEvent` and `:Incident`.

### Remaining evidence gap

The current SHACL shapes validate class-specific properties but do not themselves enforce the OWL
disjointness rule. The future semantic runner must execute the intended reasoner and include a
negative dual-type fixture.

## OR-04: Source-preserving impact granularity

### Normative requirement

Impact data MUST be represented as resources separate from the disaster event. Each impact resource
MUST retain its source-supported location and reporting scope so incident-level, major-event-level,
and aggregate source values are not collapsed into a false single measurement.

Damage categories SHOULD be modeled as direct subclasses of `:Impact` when the sources provide
aggregate category values rather than individually identified damaged assets.

### Paper evidence

- Page 8 explains why beAWARE's `VulnerableObject` indirection was collapsed for aggregate source
  data.
- Pages 10-11 distinguish DROMIC incident impacts, NDRRMC major-event impacts, and coarser GDA and
  EM-DAT aggregate impacts.
- Page 11 applies the same separation principle to preparation and response resources.

### Supporting source attributes

- Affected/displaced persons and families, evacuation centers, and casualty counts/types.
- Partially/totally damaged houses.
- Infrastructure, agriculture, general, insured, and production-loss values.
- Airport, seaport, road/bridge, power, water, communications, class, work, flight, and stranded
  disruptions.
- Impact location, source report, aggregation level, and remarks.

### Semantic realization

- `:Impact` and its domain subclasses, including `:AffectedPopulation`, `:Casualties`,
  `:HousingDamage`, `:InfrastructureDamage`, `:AgricultureDamage`, and disruption classes.
- Event-to-impact subproperties such as `:hasAffectedPopulation`, `:hasCasualties`,
  `:hasHousingDamage`, and `:hasInfrastructureDamage`.
- `:hasLocation` on each impact resource instead of copying a source aggregate into every
  descendant location.

### Functional evidence

- Competency questions: CQ6-CQ14.
- SHACL: `sgsh:ImpactShape` and all targeted impact subclass shapes from
  `sgsh:AffectedPopulationShape` through `sgsh:WorkSuspensionShape`.
- Current tests:
  `sakunagraph_etl/tests/test_impact.py` checks impact extraction behavior, while
  `api/tests/test_analysis_metrics.py` and `api/tests/test_analysis_events.py` exercise graph
  aggregation/query construction.

### Acceptance condition

A mixed-source fixture MUST preserve separate impact IRIs for an incident-level DROMIC value and a
major-event/aggregate value from another source. CQ6-CQ14 MUST return the expected counts, values,
locations, and units without multiplying an aggregate value through inferred location ancestors.
An impact resource with only `:hasLocation` and no reported detail MUST fail
`sgsh:RequiresDetailBeyondLocationConstraint`.

### Remaining evidence gap

Current tests do not execute CQ6-CQ14 as a frozen end-to-end semantic regression suite, and there is
no requirement-level test demonstrating that cross-source aggregation avoids double counting.

## OR-05: Provenance and alternate source records

### Normative requirement

Every published disaster, impact, preparedness, and response assertion MUST be traceable to its
source record or report. Extraction and revision lineage SHOULD be represented when available.
Records judged to describe the same event MUST retain their original IRIs and SHOULD be connected
through a canonical alignment using `prov:alternateOf` rather than destructively merged.

### Paper evidence

- Page 9 states that PROV-O records the source document, extraction lineage, and counterpart
  entities.
- Page 12 describes canonical alignment IRIs and `prov:alternateOf` links that preserve source
  representations.
- Pages 20-21 demonstrate source comparison and multi-source event competency questions.

### Supporting source attributes

- Source/report name, URL or local identifier, format, obtained date, last-update/revision date,
  publishing organization, and extraction time.
- Original event identifier and source-specific record IRI.
- Cross-source match membership and canonical alignment IRI.

### Semantic realization

- `:Source rdfs:subClassOf prov:Entity`.
- `prov:wasDerivedFrom`, `prov:wasAttributedTo`, and `prov:alternateOf`.
- `:incidentDerivedFrom` as a subproperty/property-chain specialization of
  `prov:wasDerivedFrom`.
- Deterministic resolution registry and alignment RDF under
  `sakunagraph_etl/src/sakunagraph_etl/resolution/`.
- Target alignment graph from ODR-0016: explicit cluster membership plus pairwise
  `prov:alternateOf` between matched source event records.

### Functional evidence

- Competency questions: CQ8, CQ15, CQ19, and CQ20.
- SHACL: `sgsh:SourceShape` validates source metadata; organization-valued response properties
  require `prov:Organization`.
- Current tests:
  `api/tests/test_analysis_events.py`,
  `api/tests/test_analysis_metrics.py`, and
  `api/tests/test_ask_query_compiler.py` assert `prov:alternateOf` traversal in query construction.

### Acceptance condition

For a fixture with two aligned source records:

1. both original IRIs and source metadata remain queryable;
2. one deterministic collection/cluster identifies the members through explicit membership;
3. CQ19 attributes counts to the correct source organizations;
4. CQ20 discovers the multi-source event through pairwise source-record `prov:alternateOf` links;
5. the cluster itself is not linked to its members with `prov:alternateOf`; and
6. conflicting values remain associated with their respective source reports without erasing the
   prior evidence or materializing a consolidated value on the cluster identifier.

### Remaining evidence gap

`sgsh:SourceShape` validates source metadata but the current shapes do not require
`prov:wasDerivedFrom`, `prov:wasAttributedTo`, or `prov:alternateOf` on the relevant resources.
ODR-0013 and ODR-0014 now define cluster-only canonical identity and report-preserving conflict
behavior. ODR-0016 requires explicit cluster membership plus pairwise source-record alternates. The
current resolver still needs that graph-shape change. The exact granularity of fact-level
provenance and its enforcement also need fixtures.

## OR-06: Explicit quantities, units, and currencies

### Normative requirement

Monetary and other quantitative values MUST retain an explicit unit whenever the source provides
one. Monetary values MUST distinguish PHP, USD, and scaled units such as millions of PHP or
thousands of USD. A number MUST NOT be compared or aggregated across incompatible units without a
documented conversion.

### Paper evidence

- Page 9 explains the adoption of QUDT for heterogeneous PHP and USD monetary impact values.
- Pages 18-20 demonstrate quantitative competency queries for population, damage, production loss,
  and assistance.

### Supporting source attributes

- Infrastructure, housing, agriculture, general, insured, production-loss, contribution, item, and
  recovery cost.
- Currency and scale: PHP, PHP millions, USD, and USD thousands.
- Non-currency measures such as hectares, metric tons, hours, people, families, houses, and damaged
  units.

### Semantic realization

- `qudt:QuantityValue` and `qudt:CurrencyUnit`.
- `qudt:numericValue` and unit/currency resources.
- `cur:PHP`, `cur:USD`, `:PHP_millions`, and `:USD_thousands`.
- Amount properties such as `:infraDamageAmount`, `:agriDamageAmount`,
  `:productionLossCost`, `:contributionAmount`, and `:housingDamageAmount`.

### Functional evidence

- Competency questions: CQ9, CQ11, and CQ15.
- SHACL: amount property shapes require `qudt:QuantityValue`; `sgsh:QuantityValueShape` and
  `sgsh:CurrencyUnitShape` target QUDT resources.
- Current tests: `api/tests/test_analysis_metrics.py` validates accepted QUDT unit identifiers in
  the analysis API.

### Acceptance condition

A fixture containing PHP millions, USD thousands, and a production-loss volume MUST preserve the
numeric value and unit for each quantity. CQ9, CQ11, and CQ15 MUST return values with their unit or
currency. A quantity missing `qudt:numericValue` or its required unit MUST fail publication
validation. Cross-unit aggregation MUST either normalize through a declared conversion or be
rejected.

### Remaining evidence gap

The current `sgsh:QuantityValueShape` and `sgsh:CurrencyUnitShape` identify target resources but do
not fully enforce numeric-value/unit completeness. Per ODR-0017, operational completeness belongs
in SHACL. Add explicit property constraints and positive and negative fixtures before considering
OR-06 fully verified.

## OR-07: Preparedness, response, and service disruption

### Normative requirement

The ontology MUST represent disaster preparedness and response information available in the source
reports, including preemptive evacuation, warnings, assistance, declarations of calamity,
recovery, rescue, and service disruption. These records MUST remain separate resources linked to
their event, location, source, and contributing organization where available.

Full operational rescue-team and mission-planning structures MAY remain out of scope until a source
and stakeholder requirement supports them.

### Paper evidence

- Page 8 excludes full rescue mission structures while retaining responders within available data
  scope.
- Pages 9-11 explain the response/preparedness extension and separate records.
- Pages 19-20 demonstrate service-disruption and assistance queries.

### Supporting source attributes

- Evacuation centers/plans and evacuated persons/families.
- Warning announcement, timestamp, and contributing organization.
- Assistance item/need, quantity, cost/contribution, families assisted, and organization.
- Declaration type, resolution number/date, recovery activity, rescue unit/equipment.
- Port/airport status, flight cancellation, road/bridge status, power/water/communication
  interruption, class/work suspension, and stranded counts.

### Semantic realization

- `:Preparedness`, `:PreemptiveEvacuation`, and `:Warning`.
- `:Response`, `:Assistance`, `:DeclarationOfCalamity`, `:Recovery`, and `:Rescue`.
- Service-disruption subclasses of `:Impact`.
- Event links, `:hasLocation`, and `:contributingOrg`.

### Functional evidence

- Competency questions: CQ8 and CQ12-CQ18.
- SHACL: `sgsh:PreparednessShape`, `sgsh:PreemptiveEvacuationShape`,
  `sgsh:WarningShape`, `sgsh:ResponseShape`, `sgsh:AssistanceShape`,
  `sgsh:DeclarationOfCalamityShape`, `sgsh:RecoveryShape`, `sgsh:RescueShape`, and the
  disruption shapes.
- Current tests: `sakunagraph_etl/tests/test_impact.py` covers selected DROMIC assistance and
  preemptive-evacuation extraction.

### Acceptance condition

Legally redistributable fixtures MUST demonstrate at least one preparedness record, one response
record, and one service-disruption record. Each MUST retain its event and most-granular supported
location, conform to SHACL, and appear in the relevant CQ8/CQ12-CQ18 result. A record with only a
location and no reported detail MUST fail the shared detail constraint.

### Remaining evidence gap

The 20 competency questions are not yet automated as a frozen semantic suite. Full rescue
mission/team workflows remain intentionally unsupported and must not be implied by the presence of
the smaller `:Rescue` reporting class.

## OR-08: IRDR/EM-DAT-aligned SKOS disaster taxonomy

### Normative requirement

Disaster types MUST be modeled as concepts in one SKOS concept scheme rather than as OWL event
classes. The hierarchy MUST follow the adopted IRDR/EM-DAT classification, include technological
disasters required by Philippine records, and retain definitions and labels. Production event
classification SHOULD use leaf concepts; broader concepts support navigation and aggregate
queries.

### Paper evidence

- Page 7 identifies the EM-DAT/IRDR glossary as the taxonomy foundation.
- Page 9 explains the SKOS choice, technological top-level concepts, added working definitions,
  and leaf-only event classification.
- Pages 18-20 demonstrate taxonomy traversal and type-filtered event queries.

### Supporting source attributes

- Source disaster type, subtype, category, event name, and contextual narrative.
- IRDR/EM-DAT preferred labels, definitions, broader/narrower relations, and locally supplied
  definitions for otherwise incomplete categories.

### Semantic realization

- `:DisasterTypeScheme` and `:DisasterType` individuals/concepts.
- `skos:ConceptScheme`, `skos:Concept`, `skos:prefLabel`, `skos:definition`,
  `skos:broader`, and `skos:inScheme`.
- `:hasDisasterType` links events to concepts.
- `sakunagraph_etl/src/sakunagraph_etl/enrichment/disaster_types.py` implements the
  rule-transformer classification path.

### Functional evidence

- Primary competency questions: CQ1-CQ5; most later CQs also filter through the taxonomy.
- SHACL: `sgsh:DisasterTypeShape`, `sgsh:DisasterTypeSchemeShape`, and
  `sgsh:DisasterEventHasDisasterTypePropertyShape`.
- Current tests:
  `sakunagraph_etl/tests/test_disaster_classification_rules.py`,
  `api/tests/test_ask_entity_resolver.py`,
  `api/tests/test_ask_query_compiler.py`, and
  `frontend/tests/unit/map-data.test.js`.

### Acceptance condition

Every disaster-type concept MUST belong to `:DisasterTypeScheme`, have a preferred label and
definition, and form an acyclic broader hierarchy terminating in the intended top-level concept.
CQ1-CQ5 MUST return the expected hierarchy and events. A production event classified directly with
a non-leaf concept MUST fail validation or be explicitly marked as a lower-confidence fallback
under a documented policy.

### Remaining evidence gap

Current SHACL validates the class, scheme, broader links, and optional definition datatype, but does
not enforce a preferred label, a definition, acyclicity, or leaf-only event classification. Per
ODR-0017, add these operational constraints and tests to SHACL while retaining semantic hierarchy
and inference in OWL.

## Cross-cutting semantic validation policy

ODR-0017 assigns domain meaning and inference to OWL and concrete submission/publication checks to
SHACL. Existing OWL cardinality and value restrictions MUST be audited and retained in OWL only
when they are intended logical claims.

ODR-0018 requires shared integrity shapes plus source/profile-specific validation for DROMIC,
NDRRMC, GDA, EM-DAT, and PSGC. Malformed data and impossible semantic combinations are blocking
violations. Legitimate source omissions and unresolved but traceable values are non-blocking
warnings; missing optional enrichment is informational. An unstated value MUST NOT be converted to
false or zero merely to satisfy validation.

## Historical exclusions and change triggers

| Scope item                             | Paper status           | Current interpretation                                                                                    | Change trigger                                                                 |
| -------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Raw climate/environmental sensor data  | Excluded on page 8     | ODR-0010 defines current climate values as report-extracted measurements, not raw sensor ingestion        | Add raw sensor semantics only through a later source-backed decision            |
| Full rescue team and mission structure | Excluded on page 8     | ODR-0011 limits `:Rescue` to reported unit/equipment/organization fields                                  | Add only with a source-backed stakeholder requirement and privacy review        |
| Crowdsourced local incident reports    | Future work on page 22 | Outside the supported source pipeline                                                                     | Add trust, moderation, provenance, consent, and correction requirements first  |
| Climate and socioeconomic enrichment   | Future work on page 22 | Not part of the paper validation baseline                                                                 | Add vocabulary alignment, licensing, freshness, and competency requirements    |

## Requirement change control

Any pull request that changes one of OR-01-OR-08 MUST:

1. identify the affected requirement IDs;
2. update the [paper evidence map](paper-evidence-map.md) when traceability changes;
3. add or update a competency query, SHACL fixture, or reasoner test;
4. state whether the change preserves the paper-era semantics or is a post-paper extension;
5. assess ETL mappings, API schema/query catalogs, and release compatibility; and
6. record unresolved domain decisions instead of silently choosing a meaning.

## Step 1 completion criteria

- [x] OR-01-OR-08 have stable IDs and normative statements.
- [x] Each requirement records paper pages and supporting source attributes.
- [x] Each requirement maps ontology terms, competency questions, and current SHACL shapes.
- [x] Each requirement identifies current automated evidence without overstating it.
- [x] Each requirement has a testable acceptance condition and a named evidence gap.
- [x] Paper exclusions are reconciled with current repository scope.
- [ ] The acceptance conditions are fully automated. This belongs to later steps in the roadmap.
