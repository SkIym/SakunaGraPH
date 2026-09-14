# ODR-0008: Represent quantities and currencies with QUDT

Status: Accepted (paper evidence)

Recorded in repository: 2026-09-14

Requirement: OR-06

## Context

SakunaGraPH integrates monetary and quantitative values reported with different units and scales.
DROMIC and NDRRMC commonly report Philippine pesos, while EM-DAT reports US dollars. A bare number
cannot safely convey currency, scale, or measurement unit.

## Options considered

The paper contrasts implicit per-property or application conventions with values that carry
explicit unit metadata. QUDT was selected because it supplies a principled vocabulary with
available documentation.

## Decision

**Paper decision:** Represent monetary and other applicable quantities as QUDT quantity values and
attach the unit or currency explicitly. Preserve distinctions such as PHP, USD, millions of PHP,
and thousands of USD rather than normalizing silently.

## Consequences

- A numeric literal remains interpretable outside the original ETL code.
- Consumers can reject incompatible aggregation or apply a documented conversion.
- Queries require an additional hop through `qudt:QuantityValue`.
- Scale and conversion policy must be explicit; QUDT use alone does not make different currencies
  or scales comparable.

## Validation

- CQ9, CQ11, and CQ15 exercise monetary/quantitative values.
- Amount property shapes require QUDT value nodes, but current `sgsh:QuantityValueShape` does not
  yet enforce both numeric value and unit completeness.
- Positive/negative OR-06 fixtures should cover PHP millions, USD thousands, a non-currency unit,
  missing units, and prohibited cross-unit aggregation.

## Evidence

- Project paper, page 9, “Monetary and quantitative modeling.”
- `ontology/sakunagraph.ttl` (`qudt:QuantityValue`, currencies, scaled units, and amount
  properties).
- `ontology/shapes/shapes.ttl` (`sgsh:QuantityValueShape` and QUDT-valued properties).
