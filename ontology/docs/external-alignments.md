# External vocabulary reuse and alignment register

Status: Implemented; paper rationale extracted; alignments reviewed; QUDT 3.5.1 pinned
2026-09-18

## Purpose and evidence boundary

This register documents SakunaGraPH's reuse of GeoSPARQL, PROV-O, SKOS, QUDT, and beAWARE.
It separates three kinds of evidence:

- **paper evidence** explains why a vocabulary was selected and, where stated, why a relation was
  chosen;
- **core-ontology evidence** records what is asserted in
  [`sakunagraph.ttl`](../sakunagraph.ttl); and
- **upstream evidence** identifies the latest stable public version, availability, and license as
  checked on 2026-09-18.

The paper does not contain pinned import versions, licenses, or a complete term-by-term alignment
table. Those facts are therefore not attributed to the paper. The current upstream version is also
not automatically the version used for reproducible validation: the immutable local files and
checksums in the [import snapshot manifest](../imports/README.md) remain the build inputs.

The term inventory below is deliberately scoped to `ontology/sakunagraph.ttl`. Terms used only in
the separate disaster-type scheme, data graphs, SHACL shapes, queries, or ETL output are outside
this inventory. The alignments below describe the reviewed core ontology after the 2026-09-18
semantic migration.

Prefix key: `:` is `https://sakuna.ph/`; `geo:` is the GeoSPARQL ontology namespace; `prov:` is
the PROV namespace; `skos:` is the SKOS Core namespace; `qudt:` is the QUDT schema namespace;
`qkind:`, `qprefix:`, and `unit:` are the QUDT quantity-kind, prefix, and unit namespaces; and
`baw:` is the beAWARE ontology namespace used by the core file.

## Paper-backed reuse rationale

Paper page numbers are the printed page numbers visible in
[`nej-main.pdf`](../../nej-main.pdf).

| Vocabulary | Paper evidence | Pages |
| --- | --- | ---: |
| beAWARE | Supplies the structural starting point. SakunaGraPH excludes unsupported raw sensor and full rescue-mission structures, collapses `VulnerableObject` indirection for aggregate impacts, and extends the model for Philippine reporting needs. | 8-10 |
| SKOS | Represents the IRDR/EM-DAT-aligned disaster taxonomy as a concept scheme rather than OWL classes; only leaf concepts classify events. | 9 |
| PROV-O | Records source-document and extraction lineage and relationships between corresponding source records. | 9, 11 |
| QUDT | Attaches explicit units to heterogeneous quantities and to PHP and USD monetary values instead of relying on implicit conventions. | 9 |
| GeoSPARQL | `:Location` is a subclass of `geo:Feature`, not an equivalent class, because GeoSPARQL features include phenomena beyond SakunaGraPH administrative locations. | 17 |

## Upstream version, repository artifact, license, and availability

"Latest" means the latest stable upstream publication found on the check date, or the latest
public artifact when a vocabulary has no release. Drafts and in-development branches are not
treated as releases.

| Vocabulary | Latest stable upstream publication | SakunaGraPH artifact or reference | License and availability |
| --- | --- | --- | --- |
| GeoSPARQL | OGC GeoSPARQL 1.1, OGC 22-047r1. GeoSPARQL Next is still in development and is not substituted for 1.1. | Imported as `http://www.opengis.net/ont/geosparql/1.1`; local `imports/geosparql-1.1.ttl` reports ontology artifact 1.1.1 and is checksum-pinned. | The [standard and model files](https://www.ogc.org/standards/geosparql/) are public. The [official repository](https://github.com/opengeospatial/ogc-geosparql) states that software and data, including the ontology, use Apache-2.0. |
| PROV-O | W3C Recommendation, 30 April 2013; the W3C page still identifies this as the latest published version. | Imported by dated IRI `http://www.w3.org/ns/prov-o-20130430`; local `imports/prov-o-20130430.owl` is checksum-pinned. | The [Recommendation and OWL encoding](https://www.w3.org/TR/prov-o/) are public under W3C document-use terms; the local redistributed snapshot records the W3C Document License. |
| SKOS | W3C Recommendation, 18 August 2009; the W3C page still identifies this as the latest published version. | Imported as `http://www.w3.org/2004/02/skos/core`; local `imports/skos-2009.rdf` is checksum-pinned. | The [Recommendation](https://www.w3.org/TR/skos-reference/) and namespace document are public under W3C document-use terms; the local redistributed snapshot records the W3C Document License. |
| QUDT | QUDT 3.5.1, published 29 August 2026. | Imported by version IRI `http://qudt.org/3.5.1/qudt-all`; local `imports/qudt-3.5.1-all.ttl` is checksum-pinned. Its VAEM 2.0 transitive import is pinned as `imports/vaem-2.0.rdf`. | The [3.5.1 catalog](https://www.qudt.org/catalog/qudt-catalog.html) provides versioned downloads. QUDT uses CC BY 4.0 with attribution to QUDT.org. The imported VAEM artifact declares CC BY-SA 3.0 US and attribution to TopQuadrant, Inc. |
| beAWARE | Latest public ontology artifact is the `master`-branch `beAWARE_ontology.owl`, with embedded `owl:versionInfo` 1.0. The project page does not publish a GitHub release. | Imported through the moving `master` URL. The local `imports/beAWARE_ontology.owl` is an immutable checksum-pinned snapshot whose original upstream commit and retrieval date are unknown; the manifest records that current upstream differs. | The [official public repository](https://github.com/beAWARE-project/ontology) is Apache-2.0. Availability is GitHub branch-head availability, not a versioned release guarantee. |

The exact local filenames, retrieval records, and SHA-256 values are authoritative in the
[import snapshot manifest](../imports/README.md). This document does not replace that manifest.

## Directly reused terms in the core ontology

### GeoSPARQL

| Term | Use in `sakunagraph.ttl` |
| --- | --- |
| `geo:Feature` | External superclass of `:Location`; also locally restated as an `owl:Class` with its definition. |

### PROV-O

| Term | Use in `sakunagraph.ttl` |
| --- | --- |
| `prov:Entity` | Superclass of `:DisasterEvent` and `:Source`. |
| `prov:Organization` | Range of `:contributingOrg`. |
| `prov:wasDerivedFrom` | Superproperty of `:incidentDerivedFrom` and a member of that property's chain. |

The paper also discusses or queries other PROV-O terms, including `prov:alternateOf` and
`prov:wasAttributedTo`, but they are not referenced by the core ontology file and are therefore
not added to this core-file inventory.

### SKOS

| Term | Use in `sakunagraph.ttl` |
| --- | --- |
| `skos:definition` | Definitions on local and restated external terms. |
| `skos:example` | Usage examples on selected terms. |
| `skos:altLabel` | Alternate labels on selected local terms. |

The paper establishes the disaster taxonomy's SKOS design. This inventory does not claim that the
three SKOS terms above are the complete taxonomy vocabulary because the separate
`disaster_type_scheme.ttl` artifact is outside the stated core-file scope.

### QUDT

| Term | Use in `sakunagraph.ttl` |
| --- | --- |
| `qudt:QuantityValue` | Range of the 13 amount/cost properties listed below. |
| `qudt:CurrencyUnit`, `qudt:DerivedUnit`, `qudt:Unit` | Types of `:PHP_millions` and `:USD_thousands`. |
| `qudt:conversionMultiplier` | Decimal scale factors `1000000.0` and `1000.0` for the two local currency units. |
| `qudt:hasQuantityKind` | Assigns `qkind:Currency` to both local currency units. |
| `qudt:prefix` | Assigns `qprefix:Mega` and `qprefix:Kilo` to the local scaled units. |
| `qudt:scalingOf` | Relates the local scaled units to `unit:CCY_PHP` and `unit:CCY_USD`. |
| `qkind:Currency` | Quantity kind of both local currency units. |
| `qprefix:Mega`, `qprefix:Kilo` | QUDT decimal prefixes applied to the local units. |
| `unit:CCY_PHP`, `unit:CCY_USD` | Current QUDT base currency units scaled by the local units. |

The 13 properties whose range is `qudt:QuantityValue` are:

- `:agriDamageAmount`, `:commercialDamageAmount`, `:contributionAmount`,
  `:crossSectoralDamageAmount`, and `:generalDamageAmount`;
- `:housingDamageAmount`, `:infraDamageAmount`, `:insuredDamageAmount`, and `:itemCost`;
- `:itemCostPerUnit`, `:postStructureCost`, `:productionLossCost`, and
  `:socialDamageAmount`.

### beAWARE

The core ontology directly references 24 beAWARE terms:

| Kind | Terms |
| --- | --- |
| Explicitly declared classes | `baw:ClimateParameter`, `baw:ClimateParameterType`, `baw:Impact`, `baw:Incident`, `baw:Location`, `baw:NaturalDisaster` |
| Imported class used in class position | `baw:NaturalDisasterType` is used as an `rdfs:domain` and `rdfs:range` target. The imported beAWARE artifact declares it `owl:Class`; the former local `owl:NamedIndividual` declaration was removed. |
| Object properties | `baw:hasClimateParameterMeasurement`, `baw:hasDisasterLocation`, `baw:hasDisasterOccurrence`, `baw:hasIncidentClimateParameter`, `baw:hasIncidentLocation`, `baw:hasIncidentOccurrence`, `baw:hasMeasurementLocation`, `baw:isClimateParameterOfIncident`, `baw:isLocationOfDisaster`, `baw:isLocationOfIncident`, `baw:isLocationOfMeasurement`, `baw:isOfClimateParameterType`, `baw:isOfDisasterType` |
| Datatype properties | `baw:hasDisasterEnd`, `baw:hasDisasterStart`, `baw:hasUnit`, `baw:hasValue` |

Eleven local individuals are typed directly as `baw:ClimateParameterType`:
`:AtmosphericPressure`, `:Depth`, `:FloodDepth`, `:Humidity`, `:Intensity`, `:Magnitude`,
`:MagnitudeScale`, `:Precipitation`, `:Temperature`, `:WaveHeight`, and `:WindSpeed`.
This is current repository behavior, not evidence that the paper ingested raw sensors: page 8 says
raw sensor and climate measurements were excluded from the study's data scope.

## Alignment assertions and semantic justification

### GeoSPARQL and PROV-O

| Assertion in `sakunagraph.ttl` | Intended meaning and evidence | Review status |
| --- | --- | --- |
| `:Location rdfs:subClassOf geo:Feature` | Every SakunaGraPH administrative location is a spatial feature, while not every spatial feature is a SakunaGraPH administrative location. The direction and non-equivalence are explicitly justified on paper page 17. | Paper-supported. |
| `:DisasterEvent rdfs:subClassOf prov:Entity` | Treats the represented disaster-event resource as a provenance-bearing entity. This is consistent with the paper's entity-lineage requirement on page 9, but the paper does not discuss this exact subclass axiom. | Consistent, not paper-explicit. |
| `:Source rdfs:subClassOf prov:Entity` | A source resource is the entity from which graph entities are derived. This directly supports the paper's source-document lineage on page 9. | Consistent with paper rationale. |
| `:incidentDerivedFrom rdfs:subPropertyOf prov:wasDerivedFrom` | Narrows general PROV derivation to the local incident/major-event source-linking relation described by the property's comment and chain. | Semantically directional; not paper-explicit. |

`prov:Organization` is reused as a range rather than aligned to a local organization class, so no
class-equivalence claim is made.

### QUDT scaled-currency links

| Assertion in `sakunagraph.ttl` | Intended meaning | Review status |
| --- | --- | --- |
| `:PHP_millions a qudt:CurrencyUnit, qudt:DerivedUnit, qudt:Unit`; `qudt:scalingOf unit:CCY_PHP`; `qudt:prefix qprefix:Mega`; multiplier `1000000.0`; quantity kind `qkind:Currency` | Represents values reported in millions of Philippine pesos while keeping currency and scale explicit. | Uses QUDT 3.5.1's current base-currency IRI and the same scaled-unit pattern used by its published currency units. Reviewed and adopted. |
| `:USD_thousands a qudt:CurrencyUnit, qudt:DerivedUnit, qudt:Unit`; `qudt:scalingOf unit:CCY_USD`; `qudt:prefix qprefix:Kilo`; multiplier `1000.0`; quantity kind `qkind:Currency` | Represents values reported in thousands of US dollars while keeping currency and scale explicit. | Uses QUDT 3.5.1's current base-currency IRI and scaled-unit pattern. Reviewed and adopted. |

The local public unit IRIs remain stable, so ETL output and stored data do not need to be reminted.
Only their external semantic mapping changes. The legacy `cur:PHP`/`cur:USD`, `skos:broader`, and
zero-offset assertions are no longer used.

### beAWARE mappings

The paper establishes selective adaptation rather than wholesale identity with beAWARE. Strong
OWL equivalence therefore needs term-level evidence. The reviewed model retains only relations
supported by the paper and the property definitions.

| Assertion in `sakunagraph.ttl` | Comparison with paper and current definitions | Review status |
| --- | --- | --- |
| `baw:NaturalDisaster rdfs:subClassOf :DisasterEvent` | A beAWARE natural-disaster occurrence is one kind of the broader SakunaGraPH event scope, which also covers localized and human-induced incidents. This matches paper page 10. | Paper-consistent. |
| `baw:hasDisasterStart rdfs:subPropertyOf :startDate` | Both denote an event start, while the beAWARE property's `baw:NaturalDisaster` domain is narrower than the local property's `:DisasterEvent` domain. | Directionally justified specialization. |
| `baw:hasDisasterEnd rdfs:subPropertyOf :endDate` | Both denote an event end, with the same narrower-to-broader domain relation. | Directionally justified specialization. |

The former equivalences between the beAWARE and local `Impact`, `Incident`, and `Location` classes
were removed because the paper documents changed impact structure and different incident/location
scope without establishing extension equality. The former disaster-type-property equivalence and
the duplicate inverse link were removed because beAWARE's class-based `NaturalDisasterType` target
is not the paper's local IRDR/EM-DAT-aligned SKOS taxonomy. No weaker relation is asserted where
the paper and term definitions do not establish a safe direction.

There are no `owl:sameAs`, `skos:exactMatch`, `skos:closeMatch`, `skos:broadMatch`,
`skos:narrowMatch`, or `skos:relatedMatch` assertions in `sakunagraph.ttl`.

## Review conclusions

1. `:Location rdfs:subClassOf geo:Feature` is retained in the paper-supported direction.
2. Unsupported beAWARE class and disaster-type-property equivalences are removed; the two temporal
   properties use directional `rdfs:subPropertyOf` relations.
3. The local scaled-currency IRIs are preserved but now use QUDT 3.5.1's current base units and
   derived-unit predicates.
4. QUDT 3.5.1 and its VAEM 2.0 transitive dependency are checksum-pinned and catalog-resolvable
   offline.
5. The import-catalog regression tests the complete closure, reviewed beAWARE relations, rejected
   equivalences, and QUDT scaled-unit pattern.
