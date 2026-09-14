# ODR-0014: Preserve conflicting values as source-report statements

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirements: OR-04, OR-05, OR-06

## Context

Different reports—or successive reports—may give different dates, locations, counts, or damage
values for records aligned to the same occurrence. SakunaGraPH needs to avoid presenting an
undocumented system-selected value as authoritative.

## Options considered

1. choose a preferred source or latest value automatically;
2. merge or reconcile values into one system-generated value; or
3. preserve what each specific report says and leave consolidation to the consumer.

## Decision

**Maintainer decision:** SakunaGraPH does not resolve conflicting reported values. The data records
what each specific report states, with its source provenance. Consolidation, precedence, or truth
selection is the responsibility of the user or consuming application.

## Consequences

- Multiple conflicting values may be correct representations of different source statements.
- The graph and APIs must preserve enough report provenance to identify which report supplied each
  value.
- Consumers must not blindly sum or select values across aligned source records.
- A UI may present values side by side, but must not label one “canonical” without an external,
  disclosed policy.
- A later report does not silently rewrite the historical statement made by an earlier report.

## Validation

- A fixture with two aligned reports and different impact values must retain both values and both
  provenance paths.
- Alignment must not copy either value onto the cluster identifier.
- Query results must expose source/report identity alongside potentially conflicting values.

## Evidence

- Maintainer answer recorded on 2026-09-14.
- ODR-0006, ODR-0007, and ODR-0013.
- Project paper, pages 9-12, provenance, granularity, and alignment design.
