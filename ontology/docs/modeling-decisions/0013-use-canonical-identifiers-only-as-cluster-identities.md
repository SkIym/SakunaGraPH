# ODR-0013: Use canonical identifiers only as cluster identities

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirement: OR-05

## Context

Entity resolution groups source records believed to describe the same occurrence and mints a
deterministic canonical IRI. It was unclear whether that IRI should become a consolidated event
carrying selected source facts.

## Options considered

1. use the canonical IRI only to identify an alignment cluster;
2. materialize a consolidated event under the canonical IRI; or
3. replace source records through strong identity/merge semantics.

## Decision

**Maintainer decision:** The canonical IRI is merely a cluster identifier. SakunaGraPH does not yet
materialize or publish a consolidated canonical event. This boundary follows advice from the
project's domain experts.

Source event IRIs remain the records that carry report-derived event facts.

## Consequences

- Matching records are discoverable as a group without choosing a preferred source truth.
- The cluster IRI must not acquire merged dates, labels, locations, impacts, or response facts.
- Applications that need one consolidated view must construct it themselves and disclose their
  consolidation policy.
- The current resolver's `rdf:type :DisasterEvent` assertion on the canonical IRI can make the
  boundary less obvious and should be reviewed when implementation work resumes.
- ODR-0016 resolves the graph shape: the cluster uses membership links, while pairwise
  `prov:alternateOf` is reserved for matched source event records.

## Validation

- Alignment output must retain all source event IRIs and one deterministic cluster IRI.
- The cluster IRI must not contain materialized source facts.
- Canonical merge/materialization must remain disabled unless a later ODR supersedes this decision.

## Evidence

- Maintainer answer and domain-expert advice recorded on 2026-09-14.
- ODR-0007.
- `sakunagraph_etl/src/sakunagraph_etl/resolution/_resolver.py`.
- `sakunagraph_etl/src/sakunagraph_etl/resolution/job.py`.
