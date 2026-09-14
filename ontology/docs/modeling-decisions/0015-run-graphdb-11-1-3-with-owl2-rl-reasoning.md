# ODR-0015: Run GraphDB 11.1.3 with OWL2-RL reasoning

Status: Accepted (maintainer decision); portability deferred

Decision date: 2026-09-14

Requirements: OR-02, OR-03, OR-08

## Context

The paper described the evaluated repository broadly as using OWL-DL reasoning, which is not
specific enough to reproduce current inference behavior. Runtime version and ruleset affect class,
property-chain, transitive, symmetric, and hierarchy inferences.

## Options considered

1. no inference or query-time traversal only;
2. RDFS reasoning;
3. GraphDB OWL2-RL reasoning; or
4. another GraphDB/custom ruleset.

## Decision

**Maintainer decision:** The current target runtime is GraphDB 11.1.3 using OWL2-RL reasoning.

Cross-store portability requirements have not yet been decided and are explicitly deferred rather
than assumed.

## Consequences

- Production acceptance results should be evaluated against GraphDB 11.1.3 with OWL2-RL enabled.
- RDFLib smoke tests remain useful for syntax and explicit SPARQL behavior, but are not evidence of
  identical entailment.
- Results may differ in stores that use no inference, RDFS, OWL-Horst, another OWL profile, or a
  differently configured import closure.
- Any future GraphDB/ruleset upgrade requires semantic regression results before adoption.

## Validation

- Record GraphDB version and ruleset in semantic-test reports and deployment metadata.
- Run hierarchy, property-chain, transitivity, disjointness, and type-inference fixtures in the
  target repository.
- Treat portability testing as a future requirement, not a current acceptance claim.

## Evidence

- Maintainer answer recorded on 2026-09-14.
- Project paper, page 12, historical GraphDB reasoning description.
- `ontology/sakunagraph.ttl` (OWL axioms and property chains).
