# SakunaGraPH ontology decision records

## Purpose

These records explain why SakunaGraPH uses its current ontology patterns. They separate decisions
stated by the project paper from later maintainer decisions, repository behavior, deferred scope,
and implementation gaps.

The project paper (`nej-main.pdf`) is historical evidence, not a living specification. A record
marked **Accepted (paper evidence)** reports a decision made in the paper. It does not imply that
the current implementation has complete automated validation.

## Status vocabulary

| Status | Meaning |
| --- | --- |
| Accepted (paper evidence) | The paper states the choice and its rationale. |
| Accepted (maintainer decision) | The maintainer explicitly approved the choice after the paper. |
| Accepted (maintainer decision); portability deferred | The runtime choice is approved, but cross-store behavior is not yet specified. |
| Historical; current review required | The paper states the choice, but current code or scope has diverged. |
| Historical; current scope recorded separately | The paper-era boundary remains historical evidence and later ODRs define current behavior. |
| Accepted with an open subdecision | The main pattern is established, but one named policy still needs approval. |
| Accepted with open operational details | The semantic direction is established, but its runtime graph contract still needs approval. |
| Proposed | A later choice is documented for review but is not yet authoritative. |
| Superseded | A newer decision record replaces the record. |

## Decision index

| ID | Decision | Status | Requirements |
| --- | --- | --- | --- |
| [ODR-0001](0001-selectively-adapt-beaware.md) | Selectively adapt beAWARE to the available report data | Historical; current scope recorded separately | OR-01, OR-07 |
| [ODR-0002](0002-model-aggregate-damage-as-impact-subclasses.md) | Model aggregate damage as direct `Impact` subclasses | Accepted (paper evidence) | OR-04 |
| [ODR-0003](0003-model-disaster-types-as-skos-leaf-concepts.md) | Model disaster types as SKOS concepts and classify with leaves | Accepted (paper evidence) | OR-08 |
| [ODR-0004](0004-distinguish-major-events-and-incidents.md) | Keep `MajorEvent` and `Incident` distinct | Accepted (paper evidence) | OR-03, OR-04 |
| [ODR-0005](0005-ground-locations-in-psgc-and-infer-ancestors.md) | Assert the most granular PSGC location and infer ancestors | Accepted (paper evidence) | OR-02 |
| [ODR-0006](0006-reify-impact-preparedness-and-response-records.md) | Keep impact, preparedness, and response as separate records | Accepted (paper evidence) | OR-02, OR-04, OR-07 |
| [ODR-0007](0007-preserve-source-records-with-prov-o-alignments.md) | Preserve source records and connect co-referent events with PROV-O | Accepted (paper evidence) | OR-05 |
| [ODR-0008](0008-represent-quantities-with-qudt.md) | Represent quantities and currencies with QUDT | Accepted (paper evidence) | OR-06 |
| [ODR-0009](0009-disposition-oops-findings.md) | Correct or contextually retain every reported OOPS! finding | Accepted (paper evidence) | OR-01, OR-02, OR-08 |
| [ODR-0010](0010-model-climate-values-as-report-extracted-measurements.md) | Model climate values as report-extracted measurements | Accepted (maintainer decision) | OR-01 |
| [ODR-0011](0011-limit-rescue-modeling-to-report-level-data.md) | Limit rescue modeling to report-level data | Accepted (maintainer decision) | OR-07 |
| [ODR-0012](0012-use-philippines-as-the-unresolved-location-fallback.md) | Use the Philippines as the unresolved-location fallback | Accepted (maintainer decision) | OR-02 |
| [ODR-0013](0013-use-canonical-identifiers-only-as-cluster-identities.md) | Use canonical identifiers only as cluster identities | Accepted (maintainer decision) | OR-05 |
| [ODR-0014](0014-preserve-conflicting-values-as-source-report-statements.md) | Preserve conflicting values as source-report statements | Accepted (maintainer decision) | OR-04, OR-05, OR-06 |
| [ODR-0015](0015-run-graphdb-11-1-3-with-owl2-rl-reasoning.md) | Run GraphDB 11.1.3 with OWL2-RL reasoning | Accepted (maintainer decision); portability deferred | OR-02, OR-03, OR-08 |
| [ODR-0016](0016-model-clusters-with-membership-and-pairwise-alternates.md) | Model clusters with membership and pairwise alternates | Accepted (maintainer decision) | OR-05 |
| [ODR-0017](0017-use-owl-for-semantics-and-shacl-for-operational-validation.md) | Use OWL for semantics and SHACL for operational validation | Accepted (maintainer decision) | OR-01-OR-08 |
| [ODR-0018](0018-use-source-aware-shacl-severity-profiles.md) | Use source-aware SHACL severity profiles | Accepted (maintainer decision) | OR-01-OR-08 |

## Evidence boundary

Each record labels claims as one of:

- **Paper decision**: directly supported by the cited paper page.
- **Maintainer decision**: explicitly supplied by the project maintainer after the paper.
- **Repository observation**: verified in the current files but not necessarily discussed in the
  paper.
- **Reconstructed alternative**: an option implied by the paper's comparison; it is included to
  explain the trade-off, not presented as minutes from a formal design meeting.
- **Open question**: no owner decision is claimed.

The decision-resolution log and any future backlog are maintained in
[open-questions.md](open-questions.md). No Step 2 questions are currently unanswered.

## Record maintenance

Do not rewrite a paper-era decision to make it agree with later code. Add a new decision record and
mark the older record superseded or historically scoped. A semantic change should update the
affected OR requirement, decision record, ontology/shape files, fixtures, and competency-query
tests in the same pull request.
