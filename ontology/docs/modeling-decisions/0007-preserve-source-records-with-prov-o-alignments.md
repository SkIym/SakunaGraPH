# ODR-0007: Preserve source records and connect co-referent events with PROV-O

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirement: OR-05

## Context

Different agencies may report the same real-world disaster with different identifiers, labels,
dates, locations, and impact figures. Users need cross-source discovery without losing the exact
record and report from which a claim came.

## Options considered

The paper contrasts destructive triple merging with source-preserving alignment. A third possible
choice—using `owl:sameAs` for all matches—is not selected by the paper and would assert stronger
identity semantics than `prov:alternateOf`.

## Decision

**Paper decision:** Use PROV-O to record source document, extraction lineage, attribution, and
counterpart relationships. Keep each source record's IRI and representation. For resolved
cross-source clusters, mint a canonical alignment identity and connect counterparts with
`prov:alternateOf` instead of destructively replacing the source records.

## Consequences

- Evidence from each publisher remains independently inspectable.
- CQ19 can compare sources and CQ20 can find events reported by more than one source.
- Alignment says that records are alternatives about the same occurrence; it does not authorize
  copying every property between them as strict logical identity would.
- SakunaGraPH does not choose consolidated display values; consuming applications must disclose
  any consolidation rule they apply.

## Validation

- CQ8, CQ15, CQ19, and CQ20 exercise provenance and cross-source discovery.
- A fixture with two matched source records should retain both IRIs, both source paths, one stable
  cluster identifier, and `prov:alternateOf` links.
- A rerun with the same cluster must mint the same identifier; adding a member must follow the
  documented cluster-stability policy.

## Current reconciliation

**Repository observation:** The resolver currently emits a canonical `:DisasterEvent`,
`prov:alternateOf` links to members, pairwise cross-source `prov:alternateOf` links, and a registry.
Its `write_alignments` docstring and job overview still mention `owl:sameAs`, although the emitted
triples use PROV-O. Canonical materialization/merge is disabled in the current job.

ODR-0013 defines the canonical IRI as a cluster identifier only, with no materialized consolidated
event. ODR-0014 preserves conflicting values as statements made by specific reports and leaves
consolidation to the consumer. ODR-0016 requires explicit cluster membership plus pairwise
`prov:alternateOf` links between source event records.

## Evidence

- Project paper, page 9, “Provenance and source tracking.”
- Project paper, pages 11-12, entity-resolution and canonical-alignment description.
- `sakunagraph_etl/src/sakunagraph_etl/resolution/_resolver.py`.
- `sakunagraph_etl/src/sakunagraph_etl/resolution/job.py`.
- `sakunagraph_etl/src/sakunagraph_etl/rdf/iris.py`.
