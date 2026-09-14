# ODR-0005: Assert the most granular PSGC location and infer ancestors

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirement: OR-02

## Context

The sources report geography at different levels, from national or free-text locations to
barangays. Explicitly attaching every ancestor to every record would duplicate triples and could
introduce inconsistent location paths.

## Options considered

The paper contrasts explicit redundant ancestor assertions with one granular assertion backed by a
fixed hierarchy. A third operational option—query-time traversal without declaring transitivity or
property chains—is reconstructed from the implementation trade-off.

## Decision

**Paper decision:** Ground administrative locations in PSGC. Assert only the most granular
source-supported location on each event or related record. Represent PSGC containment with
`:isPartOf`, declare it transitive within the restricted administrative hierarchy, and use
`:hasLocation` property chains so an event can inherit locations linked through its impacts,
preparedness, response, and related incidents.

The paper also assigns absent or ambiguous locations to the Philippines. ODR-0012 confirms this as
current behavior and documents the interpretation and limitation of the generic fallback.

## Consequences

- One location assertion supports region/province/national queries through inference or property
  paths.
- The graph avoids copying aggregate values to descendant locations.
- Correct answers depend on a complete, acyclic PSGC hierarchy and a documented inference regime.
- A consumer without the configured entailment must use paths such as `:isPartOf*` and explicit
  event-to-record joins.
- A generic Philippines fallback is indistinguishable from an explicitly nationwide location based
  on `:hasLocation` alone; provenance/report context must be retained.

## Validation

- CQ2 and CQ4 directly test location and ancestor traversal; CQ5-CQ18 also depend on it.
- PSGC SHACL shapes check administrative types and parent relationships.
- A positive fixture should assert only a barangay/city and still return the correct region.
- A negative fixture should reject cycles or invalid parent levels.

## Current subdecision

ODR-0012 selects `:Philippines` as the generic location for missing or ambiguous raw locations. No
separate uncertainty marker is currently required.

## Evidence

- Project paper, pages 10-11, “Location granularity” and the `:hasLocation` property-chain text.
- Project paper, pages 15 and 17, OOPS! dispositions for `:isPartOf` and `:Location`.
- `ontology/sakunagraph.ttl` (`:hasLocation`, `:isPartOf`, and PSGC location classes).
- `ontology/shapes/psgc/shapes.ttl`.
