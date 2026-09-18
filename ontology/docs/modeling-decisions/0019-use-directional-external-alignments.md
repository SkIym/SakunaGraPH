# ODR-0019: Use only justified directional external alignments

Status: Accepted (maintainer decision)

Decision date: 2026-09-18

Requirements: OR-01, OR-06, OR-08

## Context

The paper describes SakunaGraPH as a selective adaptation of beAWARE, gives the local event,
location, impact, and taxonomy scope, and explicitly makes `:Location` a subclass rather than an
equivalent of `geo:Feature`. The core ontology nevertheless asserted several two-way beAWARE
equivalences that the paper did not establish. It also mapped scaled currencies through legacy
QUDT currency resources that are absent from QUDT 3.5.1.

## Options considered

1. retain every existing equivalence and legacy currency link;
2. replace only those relations whose direction follows from the paper and the referenced term
   definitions, removing relations for which no safe direction is established; or
3. remove all external alignments and use the vocabularies only as unrelated term sources.

## Decision

**Maintainer decision:** Use option 2.

- Retain `:Location rdfs:subClassOf geo:Feature` and
  `baw:NaturalDisaster rdfs:subClassOf :DisasterEvent`.
- Replace the beAWARE start/end equivalences with `rdfs:subPropertyOf` in the
  beAWARE-to-SakunaGraPH direction.
- Remove the class equivalences for `Impact`, `Incident`, and `Location`, and remove the
  disaster-type-property equivalence and its duplicate inverse link. Do not invent weaker links
  where the paper and definitions do not establish a safe direction.
- Keep the public local unit IRIs `:PHP_millions` and `:USD_thousands`, but model them with QUDT
  3.5.1's `qudt:scalingOf`, `qudt:prefix`, current base-currency units, and decimal conversion
  multipliers.
- Import and checksum-pin QUDT 3.5.1 and its VAEM 2.0 transitive dependency.

## Consequences

- Reasoning no longer treats distinct beAWARE and SakunaGraPH scopes as extensionally identical.
- beAWARE natural-disaster dates still entail the corresponding local event dates.
- The local SKOS disaster taxonomy remains separate from beAWARE's class-based disaster types.
- Existing RDF using the local scaled-unit IRIs remains valid; their external definitions become
  current and explicit.
- The offline import closure grows by QUDT and VAEM snapshots and their license notices.

## Validation

- `test_ontology_import_catalog.py` checks the full offline import closure and immutable checksums.
- The same regression asserts retained subrelations, absence of rejected equivalences and the
  duplicate inverse, and the exact QUDT 3.5.1 scaled-unit pattern.
- RDF parsing, competency-query, and SHACL regressions must remain green.

## Evidence

- Project paper, pages 8-10 and 17.
- [`external-alignments.md`](../external-alignments.md).
- `ontology/sakunagraph.ttl` and the pinned beAWARE and QUDT artifacts.
- Maintainer approval on 2026-09-18 to apply the documented semantic migration and pinning.
