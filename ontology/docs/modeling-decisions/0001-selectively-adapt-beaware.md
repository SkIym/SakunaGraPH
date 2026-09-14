# ODR-0001: Selectively adapt beAWARE to the available report data

Status: Historical; current scope recorded separately

Recorded in repository: 2026-09-14

Requirements: OR-01, OR-07

## Context

beAWARE offered a useful structural disaster ontology, but SakunaGraPH's paper-era sources did not
contain every kind of evidence represented by beAWARE. DROMIC situation reports, EM-DAT, and GDA
did not provide raw environmental sensor measurements. The sources represented responders, but
not the complete rescue-team and mission structures modeled by beAWARE.

## Options considered

The alternatives below reconstruct the contrast made on paper page 8:

1. import or reproduce the complete beAWARE model whether or not source data can populate it;
2. reject beAWARE and create an unrelated ontology; or
3. reuse the relevant structure and omit unsupported modules until richer data exists.

## Decision

**Paper decision:** Use beAWARE as a structural reference, while excluding raw climate and
environmental sensor parameters and full rescue-team/mission representation from the evaluated
scope. These are data-scope boundaries, not claims that the concepts are fundamentally
incompatible. They may be introduced when supported by sources and requirements.

## Consequences

- The ontology remains aligned with evidence actually present in the evaluated sources.
- Users should not infer operational command, mission planning, or sensor-observation coverage
  from the smaller responder and rescue-reporting vocabulary.
- Reintroducing a module requires explicit semantics, provenance, competency questions, and
  fixtures rather than only adding vocabulary terms.

## Validation

- OR-01 fixtures should not require absent climate or mission fields.
- OR-07 and CQ17 should demonstrate reported rescue units/equipment without claiming full mission
  execution semantics.
- ODR-0010 fixtures must distinguish report-extracted measurements from raw sensor data.
- ODR-0011 fixtures must keep rescue coverage at report level.

## Current reconciliation

**Repository observation:** `ontology/sakunagraph.ttl`, the SHACL shapes, and the NDRRMC climate
extractor now contain climate-parameter support. This partially diverges from the paper-era
exclusion.

ODR-0010 defines these values as report-extracted measurements, not raw sensor data. ODR-0011
confirms that rescue modeling remains limited to facts reported by the source documents and does
not cover full mission/command structures.

## Evidence

- Project paper, page 8, sections 2.2.1 and 2.2.2.
- `ontology/sakunagraph.ttl` (beAWARE and climate terms).
- `ontology/shapes/shapes.ttl` (`sgsh:ClimateParameterShape` and rescue shapes).
- `sakunagraph_etl/src/sakunagraph_etl/enrichment/climate_parameters.py`.
