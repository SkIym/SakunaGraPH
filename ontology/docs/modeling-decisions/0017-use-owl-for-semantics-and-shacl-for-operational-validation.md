# ODR-0017: Use OWL for semantics and SHACL for operational validation

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirements: OR-01-OR-08

## Context

OWL's open-world semantics and SHACL's graph-validation model serve different purposes. OWL
restrictions such as `owl:someValuesFrom` express logical meaning but do not reliably reject a
submitted RDF file merely because a value is not explicitly present.

## Options considered

1. use OWL for meaning/inference and SHACL for submission/publication completeness;
2. duplicate operational constraints in OWL and SHACL; or
3. rely on OWL alone for semantics and data-quality validation.

## Decision

**Maintainer decision:** Use option 1. OWL owns domain meaning and inference, including class
hierarchies, disjointness, domains/ranges, transitivity, and property chains. SHACL owns concrete
submission and publication checks, including required counts, datatypes, classes, patterns, and
validation severity.

Existing OWL cardinality and value restrictions must be reviewed. Retain a restriction in OWL only
when it is an intended logical claim; express operational completeness requirements in SHACL.

## Consequences

- Missing-field validation has explicit closed-world behavior through SHACL.
- OWL remains usable for OWL2-RL inference without being mistaken for a form validator.
- Some current restrictions may need removal from OWL, translation to SHACL, or both after a
  term-by-term semantic review.
- A constraint may intentionally exist in both layers only when its logical and operational
  meanings are separately documented and tested.

## Validation

- OWL reasoner tests must cover inference and inconsistency cases.
- SHACL positive/negative fixtures must cover required fields, counts, datatypes, and classes.
- Every OWL cardinality/value restriction should be classified as logical, operational, or both
  before the current ontology is declared conformant with this decision.
- Tests must run with GraphDB 11.1.3 OWL2-RL for production entailment and the selected SHACL engine
  for publication validation.

## Current implementation gap

The repository has OWL restrictions and SHACL shapes, but no completed audit records which
restrictions are genuine logical claims and which were intended as completeness checks. That audit
belongs to roadmap Steps 5 and 6.

## Evidence

- Maintainer selection Q8-A recorded on 2026-09-14.
- ODR-0015.
- `ontology/sakunagraph.ttl`.
- `ontology/shapes/shapes.ttl` and `ontology/shapes/psgc/shapes.ttl`.
