# ODR-0010: Model climate values as report-extracted measurements

Status: Accepted (maintainer decision)

Decision date: 2026-09-14

Requirement: OR-01

## Context

ODR-0001 records that the paper excluded raw climate and environmental sensor data. The current
ontology and NDRRMC pipeline nevertheless contain climate-parameter nodes extracted from narrative
reports. Their evidence type needed to be distinguished from raw sensor observations.

## Options considered

1. represent the values as raw sensor observations;
2. represent them as measurements extracted from source reports; or
3. remove climate-parameter output until a sensor pipeline exists.

## Decision

**Maintainer decision:** Climate-parameter nodes represent report-extracted measurements. They
record what the source report states; they do not claim direct ingestion from, or independent
verification against, a sensor feed.

Warnings remain report-extracted warning records rather than climate measurements merely because
they appear in the same narrative.

## Consequences

- Every measurement must retain provenance to the report from which it was extracted.
- The graph can answer what measurement a report states, but not whether the measurement matches
  an authoritative sensor observation.
- Documentation and UI labels must not describe this pipeline as real-time sensor integration.
- Extraction confidence or uncertainty may be added later without changing the evidence source.

## Validation

- A fixture must trace each climate-parameter node to an NDRRMC source/report.
- Measurement values and units must conform to `sgsh:ClimateParameterShape`.
- A regression test should keep warnings and measurements in their respective record types.

## Evidence

- Maintainer answer recorded on 2026-09-14.
- ODR-0001 and project paper, page 8.
- `sakunagraph_etl/src/sakunagraph_etl/enrichment/climate_parameters.py`.
- `ontology/shapes/shapes.ttl` (`sgsh:ClimateParameterShape`).
