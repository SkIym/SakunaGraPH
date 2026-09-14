# ODR-0012: Use the Philippines as the unresolved-location fallback

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirement: OR-02

## Context

Some raw source records omit location or provide text that cannot be resolved to a more specific
PSGC unit. The paper and current EM-DAT transform use a generic Philippines location in this case.

## Options considered

1. attach the generic `:Philippines` location;
2. mint a dedicated unknown-location resource; or
3. omit `:hasLocation`.

## Decision

**Maintainer decision:** When a raw location is missing or ambiguous, supply the generic
`:Philippines` location tag, consistent with the EM-DAT handling. Do not invent a subnational PSGC
location.

No separate uncertainty marker was selected in this decision.

## Consequences

- All published records retain a broad location usable by existing national-scope queries.
- The fallback means “no more specific location was resolved,” not necessarily “the entire country
  was affected.”
- Without an additional marker, fallback records and records explicitly reported as nationwide may
  be indistinguishable from location alone. Consumers must inspect provenance/report context and
  avoid interpreting every `:Philippines` link as nationwide impact.

## Validation

- A missing and an ambiguous raw location must deterministically map to `:Philippines`.
- A resolvable subnational location must map to the most granular PSGC IRI instead.
- A regression fixture should document that no subnational location is inferred for the fallback.

## Evidence

- Maintainer answer recorded on 2026-09-14.
- ODR-0005 and project paper, page 10.
- `sakunagraph_etl/src/sakunagraph_etl/sources/emdat/transform.py`.
- `sakunagraph_etl/src/sakunagraph_etl/sources/psgc/rdf.py`.
