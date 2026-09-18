# Paper-to-ontology evidence map

## Purpose

This document maps the research evidence in [nej-main.pdf](../../nej-main.pdf) to the normative
[ontology requirements](requirements.md), current ontology terms, validation shapes, competency
questions, and automated checks. Paper page numbers are the printed numbers visible in the PDF.

The map distinguishes:

- **paper evidence** - the study's rationale, method, or reported result;
- **repository evidence** - an artifact currently present in this checkout; and
- **verification gap** - proof that must still be automated or a policy that needs review.

The map does not treat a class, shape, or test fixture as proof that the complete requirement works
end to end.

## Source evidence used to elicit the requirements

| Source                              | Paper characterization                                                                                                                   | Main attributes/concepts contributed                                                                                                                         | Requirements                                           |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------ |
| NDRRMC                              | Major-event situation reports from 2020 onward, primarily PDF, with detailed and spatially disaggregated impacts and response activities | Event metadata, affected population, casualties, housing/infrastructure/agriculture damage, disruption, preparedness, response, location, source/revision    | OR-01, OR-02, OR-03, OR-04, OR-05, OR-06, OR-07, OR-08 |
| DROMIC                              | Reports from 2014 onward, including localized and smaller-scale incidents                                                                | Incident identity, barangay/local location, affected/displaced population, evacuation, assistance, response, fire and other incident types, report revisions | OR-01, OR-02, OR-03, OR-04, OR-05, OR-06, OR-07, OR-08 |
| GDA                                 | Digitized historical spreadsheets derived from archival publications                                                                     | Historical event identity, dates, coarse/textual location, disaster type, aggregate impact and damage                                                        | OR-01, OR-02, OR-04, OR-05, OR-06, OR-08               |
| EM-DAT                              | Historical spreadsheet records, commonly national or coarse in geographic scope; source of the adopted IRDR/EM-DAT classification        | Event identity, dates, disaster hierarchy, aggregate casualties/damage, USD and scaled monetary values, source organization                                  | OR-01, OR-02, OR-04, OR-05, OR-06, OR-08               |
| PSGC                                | Authoritative Philippine administrative reference dataset                                                                                | Stable codes, labels, administrative level, alternate labels, parent hierarchy, municipality income class and related reference attributes                   | OR-01, OR-02, OR-07                                    |
| Domain consultation and concept map | Common, overlapping, and source-specific attributes were reviewed before ontology construction                                           | Candidate concepts, semantic equivalences, source-specific fields, spatial granularity, class/property arrangement                                           | OR-01-OR-08                                            |

The paper therefore has four disaster sources and one geographic reference source. Portfolio
material may say “five integrated sources” only when PSGC's reference role is also made clear.

## OR-01 trace: heterogeneous source integration

### Paper locator

- Pages 1-7: integration problem, RDF/OWL rationale, data collection, and pre-ontology analysis.
- Pages 13-14: concept map derived from common, overlapping, and source-specific attributes.

### Extracted evidence

RDF provides a common semantic layer for differently shaped source records while allowing absent
facts to remain unasserted. The ontology was designed before transformation so source mappings
would share one interpretation.

### Repository mapping

- Terms: `:DisasterEvent`, `:Impact`, `:Preparedness`, `:Response`, `:Source`.
- Implementation: `sakunagraph_etl/src/sakunagraph_etl/sources/*/rdf.py` and deterministic IRI
  helpers.
- CQs: CQ1-CQ20; representative CQ1, CQ6, CQ15, CQ19, CQ20.
- Shapes: `sgsh:DisasterEventShape`, `sgsh:ImpactShape`, `sgsh:PreparednessShape`,
  `sgsh:ResponseShape`, `sgsh:SourceShape`.
- Existing check: `scripts/portfolio_demo.py` plus the five golden N-Triples fixtures.

### Gap

No single acceptance test currently proves omission semantics, SHACL conformance, deterministic
identity, and provenance for all five source fixtures together. ODR-0018 now defines shared plus
source-specific SHACL profiles and severity handling, but that profile system still needs
implementation and fixtures.

## OR-02 trace: PSGC-grounded geography

### Paper locator

- Page 7: PSGC selected as authoritative location reference.
- Page 10: source granularity, most-granular assertion, hierarchy inference, and national fallback.
- Page 11: `:hasLocation` property chains.

### Extracted evidence

NDRRMC/DROMIC may reach barangay level while GDA/EM-DAT may be coarse or textual. The model asserts
the most granular available location and traverses `:isPartOf` for broader locations.

### Repository mapping

- Terms: `:Location`, `:Country`, `:IslandGroup`, `:Region`, `:Province`, `:Municipality`,
  `:City`, `:SubMunicipality`, `:Barangay`, `:hasLocation`, `:isPartOf`.
- Implementation: location matcher and PSGC source modules under `sakunagraph_etl/`.
- CQs: CQ2, CQ4, and all location-filtered CQ5-CQ18.
- Shapes: all PSGC location/hierarchy shapes plus event/impact/preparedness/response location
  properties.
- Existing checks: API query tests assert `:isPartOf*` traversal.

### Current decision and gap

ODR-0012 confirms `:Philippines` as the generic fallback for missing or ambiguous raw locations.
This means no more specific location was resolved and does not alone prove nationwide impact. The
fallback currently has no separate uncertainty marker. GraphDB inference and invalid-parent
fixtures still need one automated acceptance path.

## OR-03 trace: major events and incidents

### Paper locator

- Page 10: event-scope distinction.
- Pages 10-11: source-specific major-event and incident impact granularity.

### Extracted evidence

Large-scale disasters and localized/human-induced incidents remain distinct but use the same
taxonomy. The distinction accommodates NDRRMC and DROMIC reporting scopes.

### Repository mapping

- Terms: `:DisasterEvent`, `:MajorEvent`, `:Incident`, `:hasRelatedIncident`,
  `:isRelatedTo`, `:incidentDerivedFrom`.
- CQs: CQ1, CQ5, CQ19, CQ20.
- Shapes: `sgsh:DisasterEventShape`, `sgsh:MajorEventShape`, `sgsh:IncidentShape`.
- Existing checks: event-detail, Ask compiler, analysis, and frontend regression tests use both
  event types.

### Gap

OWL disjointness between `:MajorEvent` and `:Incident` is not covered by a current negative
reasoner fixture.

## OR-04 trace: impact granularity

### Paper locator

- Page 8: direct `Impact` subclasses replace beAWARE `VulnerableObject` indirection.
- Pages 10-11: impact instances preserve source and spatial/reporting granularity.

### Extracted evidence

The sources report aggregate category values rather than individual damaged assets. DROMIC
incident impacts, NDRRMC major-event impacts, and GDA/EM-DAT aggregates therefore remain separate
impact resources.

### Repository mapping

- Terms: `:Impact` and the population, casualty, damage, and disruption subclasses.
- CQs: CQ6-CQ14.
- Shapes: `sgsh:ImpactShape` and all targeted impact subclass shapes.
- Existing checks: `sakunagraph_etl/tests/test_impact.py` and API analysis/query tests.

### Gap

CQ6-CQ14 now execute against the immutable competency fixture in the offline and GraphDB lanes. A
future mixed-source aggregation fixture must still prove that inferred location ancestors do not
multiply values across independently reported source aggregates.

## OR-05 trace: provenance and alternate records

### Paper locator

- Page 9: PROV-O source, extraction, and counterpart-entity lineage.
- Page 12: canonical alignment IRIs and `prov:alternateOf`.
- Pages 20-21: source comparison and multi-source event queries.

### Extracted evidence

Cross-source resolution must preserve the original source representation. Co-referent records are
discoverable through a canonical alignment rather than destructive triple merging.

### Repository mapping

- Terms: `:Source`, `prov:Entity`, `prov:wasDerivedFrom`, `prov:wasAttributedTo`,
  `prov:alternateOf`, `:incidentDerivedFrom`.
- Implementation: `sakunagraph_etl/src/sakunagraph_etl/resolution/` and source RDF mappings.
- CQs: CQ8, CQ15, CQ19, CQ20.
- Shapes: `sgsh:SourceShape` and organization class constraints.
- Existing checks: API analysis and Ask compiler tests assert alternate-record traversal.

### Current decisions and gap

ODR-0013 defines the canonical IRI as a cluster identifier only; no consolidated event is
materialized. ODR-0014 preserves each report's values without automatic conflict resolution and
leaves consolidation to the consumer. ODR-0016 requires explicit cluster membership and retains
pairwise `prov:alternateOf` only between matched source records. The current resolver and SHACL
still need to implement/enforce that graph contract. Fact-level provenance enforcement and an
end-to-end multi-source fixture also remain to be implemented.

## OR-06 trace: quantities and currencies

### Paper locator

- Page 9: QUDT adoption for heterogeneous PHP and USD values.
- Pages 18-20: quantitative population, damage, production-loss, and assistance queries.

### Extracted evidence

Quantities require explicit units so values from different sources and currencies are not combined
through implicit conventions.

### Repository mapping

- Terms: `qudt:QuantityValue`, `qudt:CurrencyUnit`, `qudt:numericValue`, `unit:CCY_PHP`,
  `unit:CCY_USD`, `:PHP_millions`, `:USD_thousands`, and amount properties.
- CQs: CQ9, CQ11, CQ15.
- Shapes: amount-property class constraints, `sgsh:QuantityValueShape`, and
  `sgsh:CurrencyUnitShape`.
- Existing check: analysis API unit validation.

### Gap

The target node shapes do not yet require a numeric value and compatible unit. Add positive and
negative fixtures and reject undocumented cross-unit aggregation.

## OR-07 trace: preparedness, response, and disruption

### Paper locator

- Page 8: full rescue mission structures excluded from the study's available-data scope.
- Pages 9-11: response/preparedness extensions and separate instances.
- Pages 19-20: service disruption and assistance examples.

### Extracted evidence

The graph covers interventions and preparedness, not only event damage. It represents the fields
actually reported but does not claim to model operational mission planning.

### Repository mapping

- Terms: `:Preparedness`, `:PreemptiveEvacuation`, `:Warning`, `:Response`,
  `:Assistance`, `:DeclarationOfCalamity`, `:Recovery`, `:Rescue`, and service-disruption classes.
- CQs: CQ8 and CQ12-CQ18.
- Shapes: preparedness, response, assistance, declaration, recovery, rescue, and disruption
  shapes.
- Existing check: `sakunagraph_etl/tests/test_impact.py` covers selected assistance and evacuation
  extraction.

### Gap

The relevant CQs need automated expected results. The small `:Rescue` reporting class must not be
presented as full rescue-team or mission-management support.

## OR-08 trace: SKOS disaster taxonomy

### Paper locator

- Page 7: EM-DAT/IRDR glossary selected as classification foundation.
- Page 9: SKOS model, technological disasters, added definitions, and leaf-only classification.
- Pages 18-20: taxonomy traversal and type filtering.

### Extracted evidence

Disaster types are SKOS concepts rather than OWL event classes. Broader concepts organize
navigation and aggregation; leaf concepts classify production events.

### Repository mapping

- Terms: `:DisasterTypeScheme`, `:DisasterType`, `skos:ConceptScheme`, `skos:Concept`,
  `skos:prefLabel`, `skos:definition`, `skos:broader`, `skos:inScheme`,
  `:hasDisasterType`.
- Implementation: `ontology/disaster_type_scheme.ttl` and the ETL disaster classifier.
- CQs: CQ1-CQ5, with taxonomy filtering reused by later CQs.
- Shapes: `sgsh:DisasterTypeShape`, `sgsh:DisasterTypeSchemeShape`,
  `sgsh:DisasterEventHasDisasterTypePropertyShape`.
- Existing checks: disaster classification, Ask entity resolution/compiler, and frontend label
  formatting tests.

### Gap

Current SHACL does not enforce preferred labels, definitions, acyclic hierarchy, or leaf-only
event classification.

## Paper-to-current scope reconciliation

| Topic                 | Paper evidence                                               | Current repository evidence                                                                    | Required explanation                                                                                                         |
| --------------------- | ------------------------------------------------------------ | ---------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Source count          | Four disaster sources plus PSGC reference data, pages 6-7    | README and smoke demo count five integrated sources                                            | Always state PSGC's reference role                                                                                           |
| Climate parameters    | Raw climate/environmental sensor parameters excluded, page 8 | ODR-0010: current values are measurements extracted from reports, not raw sensor ingestion      | Add provenance/CQ fixtures that preserve this evidence boundary                                                              |
| Rescue scope          | Full rescue team/mission structures excluded, page 8         | ODR-0011: rescue remains report-level only                                                      | Do not imply mission execution or command modeling                                                                            |
| Missing location      | Philippines used for absent/ambiguous locations, page 10     | ODR-0012 confirms the generic `:Philippines` fallback                                           | Do not interpret the fallback alone as proof of nationwide impact                                                             |
| Runtime reasoning     | Paper describes OWL-DL reasoning, page 12                    | ODR-0015: GraphDB 11.1.3 with OWL2-RL                                                          | Portability requirements are deferred; pin runtime metadata in semantic reports                                               |
| Functional validation | All 20 CQs reported successful, pages 12-20                  | CQ01-CQ20, an immutable synthetic fixture, exact expected CSVs, and RDFLib/GraphDB execution contracts in `ontology/validation/` | Preserve the paper result as historical evidence; require a GraphDB report for current release acceptance                    |
| CQ categories         | Paper says five but names six, page 18                       | Repository headings name six                                                                   | Use six consistently                                                                                                         |
| Semantic constraints  | Protégé/OOPS!/NEOntometrics described, pages 12 and 15-18    | ODR-0017 assigns semantics/inference to OWL; ODR-0018 assigns source-aware quality gates to SHACL | Audit OWL restrictions and implement versioned SHACL profiles/severities                                                    |

## Evidence file index

- [Project paper](../../nej-main.pdf)
- [Normative ontology requirements](requirements.md)
- [Ontology decision records](modeling-decisions/README.md)
- [Decision-resolution log](modeling-decisions/open-questions.md)
- [Namespace and versioning policy](versioning-policy.md)
- [Import snapshot manifest](../imports/README.md)
- [Current ontology](../sakunagraph.ttl)
- [Disaster taxonomy](../disaster_type_scheme.ttl)
- [Competency questions](../validation/competency_questions.md)
- [Executable competency manifest](../validation/competency-manifest.json)
- [Semantic-validation workflow](../validation/README.md)
- [Disaster SHACL shapes](../shapes/shapes.ttl)
- [PSGC SHACL shapes](../shapes/psgc/shapes.ttl)
- [NEOntometrics export](../validation/neontometrics-2.csv)
- [OOPS! export](../validation/pitfall-scanner-results-2.xml)

## Maintenance rule

When a requirement changes, update its requirement section and this evidence map in the same pull
request. Add a post-paper decision record for new semantics; do not edit the paper's historical
claim to make it match later code. Mark a verification gap closed only when the linked automated
test and reproducible fixture are present.
