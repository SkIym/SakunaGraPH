# ODR-0003: Model disaster types as SKOS concepts and classify with leaves

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirement: OR-08

## Context

SakunaGraPH needs one navigable classification for major natural disasters and localized or
human-induced incidents. The hierarchy follows IRDR/EM-DAT, adds technological disasters required
by Philippine records, and includes locally supplied working definitions where the source glossary
is incomplete.

## Options considered

The paper explicitly chooses between disaster types as OWL classes and as members of a SKOS
concept scheme. It also distinguishes classification with terminal concepts from classification at
arbitrary broader levels.

## Decision

**Paper decision:** Model disaster types as `skos:Concept` resources in one concept scheme rather
than as OWL event classes. Use leaf concepts to classify event data. Use `skos:broader` ancestors
for navigation and aggregate queries, not as routine production classification targets.

## Consequences

- The taxonomy can evolve as a controlled vocabulary without changing the event class hierarchy.
- `MajorEvent` and `Incident` use the same type system.
- Broader-category queries depend on SKOS path traversal or materialized hierarchy inference.
- Classifiers must know which concepts are leaves; labels and definitions become operational data.
- A fallback to a broader concept, if desired, requires an explicit confidence/status policy.

## Validation

- CQ1-CQ5 exercise event typing and SKOS hierarchy traversal.
- `sgsh:DisasterTypeShape` and `sgsh:DisasterTypeSchemeShape` cover basic type/scheme structure.
- A future negative fixture must reject or flag direct production classification with a non-leaf
  concept. Current SHACL does not enforce that paper rule.

## Evidence

- Project paper, page 9, section 2.2.3.
- `ontology/disaster_type_scheme.ttl`.
- `ontology/sakunagraph.ttl` (`:DisasterType`, `:DisasterTypeScheme`, and
  `:hasDisasterType`).
- `sakunagraph_etl/src/sakunagraph_etl/enrichment/disaster_types.py`.
