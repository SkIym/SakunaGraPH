# ODR-0009: Correct or contextually retain every reported OOPS! finding

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirements: OR-01, OR-02, OR-08

## Context

The paper reports six OOPS! pitfall categories. Because the scanner is not context-aware, a finding
can require correction or a documented retention based on source semantics and the application
domain. An ignored warning without rationale would make later audits ambiguous.

## Options considered

For each finding the project could correct the ontology, retain it with domain justification, or
defer it without a decision. The paper chose correction or justified retention for every reported
category; it did not leave any as unexplained deferral.

## Decision

**Paper decisions:**

| Paper finding | Cases | Disposition | Rationale |
| --- | ---: | --- | --- |
| 1. Merging different concepts in one class | 1 | Retained with justification | Roads and bridges are grouped together by the source reports, so the combined concept preserves source semantics. |
| 2. Missing annotations | 266 | Corrected | Add `skos:definition` and `rdfs:comment` to affected ontology elements. |
| 3. Inverse relationships not explicitly declared | 28 | Retained selectively | Declaring every inverse would add complexity and redundant RDF; explicit inverses are kept for frequent query paths such as event/type. |
| 4. Miscellaneous classes | 5 | Retained with justification | `FireMiscellaneous`, `CollapseMiscellaneous`, `ExplosionMiscellaneous`, `MiscellaneousAccident`, and `MiscellaneousAccidentGeneral` mirror the EM-DAT schema. |
| 5. Wrong transitive relationship | 1 | Retained with a domain restriction | `:isPartOf` represents a strictly nested, acyclic PSGC administrative hierarchy, where transitivity is intentional. |
| 6. Wrong equivalent classes | 1 | Corrected | Replace `:Location owl:equivalentClass geo:Feature` with `:Location rdfs:subClassOf geo:Feature` so non-location GeoSPARQL features are not inferred as SakunaGraPH administrative locations. |

No retained item is a universal ontology-design rule. Each waiver is limited to the source and
domain conditions stated above.

## Consequences

- The ontology preserves source-aligned categories where splitting them would invent unsupported
  distinctions.
- Contextual waivers are reviewable instead of being lost in a scanner report.
- The PSGC hierarchy must remain acyclic for the transitivity waiver to stay valid.
- Selective inverses reduce schema size but require clients to know canonical query directions.
- Annotation completeness and the `Location` subclass relationship must be rechecked after changes.

## Validation

- Run the OOPS! scan against a versioned ontology artifact and compare findings with this register.
- Check that `:Location rdfs:subClassOf geo:Feature` and not `owl:equivalentClass` is present.
- Validate PSGC parent levels and acyclicity before relying on transitive `:isPartOf`.
- Lint public classes/properties for a human-readable label plus definition/comment.
- Treat a new occurrence or changed affected element as a new review, not automatically covered by
  this paper-era waiver.

## Current reconciliation

**Repository observation:** The current ontology uses `:RoadAndBridgesDamage`, while the retained
scanner export refers to the paper-era `:RoadAndBridges` name. The XML is evidence of that scan, not
proof that the current ontology was rescanned. The future validation baseline should record tool
version, ontology checksum, date, and accepted waiver identifiers.

## Evidence

- Project paper, page 15, OOPS! table and dispositions for findings 1-2.
- Project paper, page 17, dispositions for findings 3-6.
- `ontology/validation/pitfall-scanner-results-2.xml`.
- `ontology/sakunagraph.ttl` (`:isPartOf`, `:Location`, annotations, and retained taxonomy terms).
