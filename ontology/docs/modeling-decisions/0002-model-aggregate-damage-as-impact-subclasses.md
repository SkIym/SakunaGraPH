# ODR-0002: Model aggregate damage as direct `Impact` subclasses

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirement: OR-04

## Context

beAWARE models physical assets through `VulnerableObject` instances connected to impacts. The
SakunaGraPH sources usually report totals or categories such as damaged infrastructure, not
identifiable bridges, buildings, or other asset instances.

## Options considered

The paper explicitly contrasts:

1. preserving the `VulnerableObject` indirection and inventing asset nodes for aggregate rows; and
2. collapsing that indirection and representing reported damage categories as `Impact` subclasses.

## Decision

**Paper decision:** Represent aggregate damage categories directly as subclasses of `:Impact`, for
example `:InfrastructureDamage rdfs:subClassOf :Impact`. Do not create `VulnerableObject`
instances when the source does not identify individual vulnerable assets.

## Consequences

- RDF mirrors the actual reporting granularity and avoids fabricated asset identity.
- Damage-category queries require fewer joins.
- The model cannot directly answer asset-inventory questions unless an explicit asset layer is
  introduced later.
- If asset-level data is added, aggregate damage records and identified assets must coexist without
  interpreting an aggregate as one physical object.

## Validation

- CQ9-CQ14 exercise category-specific damage and disruption records.
- `sgsh:ImpactShape` and subclass shapes validate the direct impact model.
- An OR-04 fixture should show an event linked to a direct impact subclass with reported details
  and no synthetic `VulnerableObject`.

## Evidence

- Project paper, page 8, section 2.2.2.
- `ontology/sakunagraph.ttl` (`:Impact`, `:InfrastructureDamage`, and other impact subclasses).
- `ontology/shapes/shapes.ttl` (impact node shapes).
