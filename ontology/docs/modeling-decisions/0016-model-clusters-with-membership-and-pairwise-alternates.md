# ODR-0016: Model clusters with membership and pairwise alternates

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirement: OR-05

## Context

ODR-0013 defines the canonical IRI as a cluster identifier, not a consolidated event. The current
resolver uses `prov:alternateOf` both from the cluster to its members and directly between matched
source event records. A grouping relationship and an alternate-representation relationship have
different meanings and should not be conflated.

## Options considered

1. retain canonical-to-member and pairwise `prov:alternateOf` links;
2. use cluster membership only and derive source-to-source relationships through the cluster; or
3. use proper cluster membership and retain pairwise `prov:alternateOf` between source records.

## Decision

**Maintainer decision:** Use option 3. Represent the canonical IRI as a collection/cluster and link
it to source event records through a membership property. Retain direct pairwise
`prov:alternateOf` links between matched source event records. Do not use `prov:alternateOf` as the
cluster-to-member predicate.

The implementation may use `prov:Collection` with `prov:hadMember` directly or a SakunaGraPH
cluster class/property aligned to those PROV-O terms. Either representation must preserve the
cluster-only meaning from ODR-0013.

## Consequences

- Cluster membership and event co-reference have explicit, different semantics.
- CQ20 can continue to discover matched source records through direct `prov:alternateOf` links.
- Pairwise links grow approximately with the square of cluster size; this is accepted for query
  convenience and must be measured if clusters become large.
- The cluster resource must not receive merged event facts.

## Validation

- A two-source fixture must contain one cluster, two membership edges, and bidirectional pairwise
  `prov:alternateOf` links.
- No cluster-to-member edge may use `prov:alternateOf`.
- CQ20 must continue to return the two source records directly.
- A cluster resource must not contain source-derived event names, dates, locations, or impacts.

## Current implementation gap

The resolver currently types the canonical IRI as `:DisasterEvent` and uses
`canonical prov:alternateOf member`. Implementation work must replace those edges with explicit
membership while retaining source-to-source alternates. This ODR records the target behavior; it
does not claim that the resolver already conforms.

## Evidence

- Maintainer selection Q6-C recorded on 2026-09-14.
- ODR-0007 and ODR-0013.
- `sakunagraph_etl/src/sakunagraph_etl/resolution/_resolver.py`.
- `ontology/validation/competency_questions.md` (CQ20).
