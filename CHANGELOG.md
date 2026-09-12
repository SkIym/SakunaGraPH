# Changelog

Notable SakunaGraPH changes are documented here. The production ETL, API, frontend, and ontology
currently have independent internal versions; repository release notes describe the exact artifact
set being published.

## Unreleased

### Added

- Recruiter-facing project overview with architecture, evidence, reproducible setup, limitations,
  and screenshots.
- System architecture and engineering trade-off record.
- Data-source access, attribution, and redistribution policy.
- Contribution and release guidance.
- Infrastructure-free RDF portfolio demo using checksum-verified synthetic fixtures.
- Five-minute portfolio walkthrough and a CI job that executes the public smoke demo.
- GitHub Actions workflows for ETL quality, dependency and infrastructure checks, API contracts,
  frontend quality, accessibility, browser tests, performance, and deployment verification.

### Clarified

- The current Ask implementation is graph-grounded retrieval and does not yet maintain a vector
  index or text-chunk retrieval layer.
- The application Compose stack requires separately configured GraphDB and local model endpoints.
- Repository licensing does not relicense third-party datasets or imported ontology material.

## [sakunagraphv.1.0] - 2026-06-19

### Added

- Published the v1.0 SakunaGraPH knowledge graph and ontology baseline.
- Added a property-chain axiom connecting related events and source derivation so source evidence
  can be followed from incidents through their related major events.

[sakunagraphv.1.0]: https://github.com/SkIym/SakunaGraPH/releases/tag/sakunagraphv.1.0
