# ODR-0011: Limit rescue modeling to report-level data

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirement: OR-07

## Context

The paper omitted beAWARE's complete rescue-team and mission structures while the current model
contains a smaller `:Rescue` record with fields found in disaster reports.

## Options considered

1. model full mission planning, command, team structure, and execution;
2. model only rescue facts explicitly reported by the integrated sources; or
3. omit rescue data entirely.

## Decision

**Maintainer decision:** Rescue modeling remains report-level only. SakunaGraPH may represent
reported rescue units, equipment, contributing organizations, locations, and related remarks, but
does not model a complete rescue mission or operational command structure.

## Consequences

- `:Rescue` describes report content, not a mission-management system.
- Missing mission roles, assignments, status transitions, or command links are out of scope rather
  than incomplete data.
- Future operational rescue data requires a new decision record and source-supported extension.

## Validation

- CQ17 may return reported units and equipment only.
- Fixtures must not invent mission, command, deployment, or team-membership entities.
- Public documentation must use “reported rescue operations/data,” not “rescue mission tracking.”

## Evidence

- Maintainer answer recorded on 2026-09-14.
- ODR-0001 and project paper, page 8.
- `ontology/sakunagraph.ttl` (`:Rescue`, `:rescueUnit`, and `:rescueEquipment`).
