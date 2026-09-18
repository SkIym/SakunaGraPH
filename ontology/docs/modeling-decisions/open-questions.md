# Ontology decision log and backlog

## Status

There are no unanswered Step 2 ontology decisions as of 2026-09-18. Cross-store portability is an
explicitly deferred scope item in ODR-0015, not an assumed requirement.

## Resolved questions

| Question | Resolution |
| --- | --- |
| Q1: climate evidence type | [ODR-0010](0010-model-climate-values-as-report-extracted-measurements.md) |
| Q2: rescue scope | [ODR-0011](0011-limit-rescue-modeling-to-report-level-data.md) |
| Q3: missing/ambiguous location | [ODR-0012](0012-use-philippines-as-the-unresolved-location-fallback.md) |
| Q4: canonical IRI role | [ODR-0013](0013-use-canonical-identifiers-only-as-cluster-identities.md) |
| Q5: conflicting values | [ODR-0014](0014-preserve-conflicting-values-as-source-report-statements.md) |
| Q6: cluster membership and pairwise alternates | [ODR-0016](0016-model-clusters-with-membership-and-pairwise-alternates.md) |
| Q7: GraphDB runtime and ruleset | [ODR-0015](0015-run-graphdb-11-1-3-with-owl2-rl-reasoning.md) |
| Q8: OWL/SHACL responsibility boundary | [ODR-0017](0017-use-owl-for-semantics-and-shacl-for-operational-validation.md) |
| Q9: open-world and publication-quality policy | [ODR-0018](0018-use-source-aware-shacl-severity-profiles.md) |
| Q10: external alignment strength and QUDT pinning | [ODR-0019](0019-use-directional-external-alignments.md) |

## Deferred or excluded scope

- Cross-store portability requirements are deferred by ODR-0015.

## Reopening a decision

If source coverage, domain advice, or runtime behavior changes, add a numbered ODR and mark the
affected record superseded. Do not rewrite the historical decision without preserving its prior
rationale.
