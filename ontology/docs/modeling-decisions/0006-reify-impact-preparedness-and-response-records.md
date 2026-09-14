# ODR-0006: Keep impact, preparedness, and response as separate records

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirements: OR-02, OR-04, OR-07

## Context

DROMIC commonly reports incident- and barangay-level details, NDRRMC reports impacts at major-event
scope with spatial disaggregation, and EM-DAT/GDA provide coarser aggregates. Impact,
preparedness, and response facts may each have their own location, source, time, organization, and
reported values.

## Options considered

The paper implies two alternatives:

1. place all impact/preparedness/response values directly on the event, collapsing their scope; or
2. create separate records linked to the event, location, and source.

## Decision

**Paper decision:** Represent each reported impact as a distinct `:Impact` instance at the
source-supported scope. Apply the same pattern to `:Preparedness` and `:Response` records. Link
specialized records to events using properties such as `:hasImpact`, `:hasPreparedness`, and
`:hasResponse`; use location property chains to support event-level geographic discovery.

## Consequences

- Source-specific values and geographic scopes can coexist without forcing a false single value.
- Provenance and revisions can attach to the record they qualify.
- Queries can distinguish impact facts from actions and preparedness.
- Aggregations require care: co-referent or differently scoped records must not be blindly summed.
- More RDF nodes and joins are required than in a flat event model.

## Validation

- CQ6-CQ14 cover impacts and disruptions; CQ8 and CQ15-CQ18 cover preparedness/response.
- `sgsh:ImpactShape`, `sgsh:PreparednessShape`, `sgsh:ResponseShape`, and their subclass shapes
  validate the record families.
- An OR-04 fixture should retain separate DROMIC and NDRRMC impact IRIs and prove that a regional
  aggregate is not multiplied across inferred descendant locations.

## Evidence

- Project paper, pages 10-11, “Impact granularity” and the following preparation/response text.
- `ontology/sakunagraph.ttl` (event-to-record properties and `:hasLocation` chains).
- `ontology/shapes/shapes.ttl` (impact, preparedness, and response shapes).
