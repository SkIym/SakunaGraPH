# ODR-0004: Keep `MajorEvent` and `Incident` distinct

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirements: OR-03, OR-04

## Context

Philippine records span large or aggregated calamities and smaller localized or human-induced
events. Treating all records at one scope would hide the difference between a major event and an
incident that may occur within or alongside it.

## Options considered

The alternatives reconstructed from paper page 10 are:

1. use one undifferentiated event class;
2. encode scale in the disaster-type taxonomy; or
3. use two event subclasses while sharing the same disaster-type taxonomy.

## Decision

**Paper decision:** Model both scopes under `:DisasterEvent`, keep `:MajorEvent` and `:Incident`
distinct, and allow both to use the same disaster-type taxonomy. Relate an incident to a major
event only when the evidence supports that relationship.

**Repository observation:** The current ontology makes both classes subclasses of
`:DisasterEvent` and declares them disjoint.

## Consequences

- Shared queries can retrieve both event scopes while scope-specific fields remain expressible.
- A DROMIC incident and an NDRRMC major event need not be collapsed merely because they concern the
  same disaster period.
- Consumers must choose whether a query groups by major event, incident, or all disaster events.
- The disjointness rule depends on OWL reasoning or an equivalent closed-world validation test.

## Validation

- CQ1 checks major events and explicitly related incidents; CQ5 and CQ19 cover event/incident
  classification; CQ20 covers cross-source identity.
- `sgsh:DisasterEventShape`, `sgsh:MajorEventShape`, and `sgsh:IncidentShape` validate their
  respective properties.
- An OR-03 negative fixture should fail when one IRI is typed as both disjoint classes.

## Evidence

- Project paper, page 10, “Event scope” and “Impact granularity.”
- `ontology/sakunagraph.ttl` (`:DisasterEvent`, `:MajorEvent`, `:Incident`,
  `:hasRelatedIncident`, and `:isRelatedTo`).
