# ODR-0018: Use source-aware SHACL severity profiles

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirements: OR-01-OR-08

## Context

SakunaGraPH integrates sources with intentionally different fields and granularity. Open-world
semantics treat an unstated fact as unknown rather than false, while publication still needs to
reject malformed RDF. One strict completeness profile would incorrectly reject legitimate coarse
or sparse records.

## Options considered

1. use source-aware validation profiles and severity levels;
2. require the same fields from every source through one strict global profile; or
3. validate only basic RDF structure and allow all domain omissions.

## Decision

**Maintainer decision:** Use option 1. Apply a shared core of semantic integrity shapes plus
source/profile-specific SHACL requirements for DROMIC, NDRRMC, GDA, EM-DAT, and PSGC where their
contracts differ.

Use the following operational severities:

- **Violation:** malformed datatype, impossible class combination, invalid PSGC parent, or a
  record object with none of its defining reported details. Violations block publication.
- **Warning:** an expected field is absent because a source does not provide it, or a value is
  broad/unresolved but remains traceable to its report. Warnings do not block publication.
- **Information:** optional enrichment or confidence metadata is unavailable. Informational
  findings do not block publication.

Never convert an unstated value into a false assertion or numeric zero merely to satisfy a shape.

## Consequences

- Legitimate source omissions remain publishable and visible as quality metadata.
- Malformed records are quarantined before GraphDB publication.
- Profiles need shared identifiers, versions, and a documented selection rule in each source job.
- Common constraints should live in a reusable core to avoid profile drift.
- The generic Philippines fallback from ODR-0012 can be reported as a warning when ingest context
  distinguishes fallback use from an explicitly nationwide source location.

## Validation

- Each source needs positive, violation, warning, and informational fixtures appropriate to its
  supported fields.
- The publication gate must block on any `sh:Violation` and retain/report non-blocking findings.
- An EM-DAT record without barangay detail must remain valid under its profile.
- A casualty count with the wrong datatype and an invalid PSGC parent must fail.
- A missing casualty statement must never be transformed into zero casualties.

## Current implementation gap

The current shared shapes do not yet provide a complete, versioned source-profile system or the
full severity contract above. This ODR defines the target policy for roadmap Step 5; it does not
claim existing publication validation already implements it.

## Evidence

- Maintainer selection Q9-A recorded on 2026-09-14.
- ODR-0017 and OR-01.
- `ontology/shapes/shapes.ttl` and `ontology/shapes/psgc/shapes.ttl`.
- Package-owned SHACL publication gates under `sakunagraph_etl`.
