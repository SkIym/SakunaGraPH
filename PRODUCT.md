# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The primary users are members of the general public who need Philippine disaster data presented in a form they can understand and use without specialist knowledge.

The secondary users are disaster-data researchers and analysts who need to inspect the same information in greater depth, including its structure, source evidence, and provenance.

## Product Purpose

SakunaGraPH integrates disaster records from Philippine and international sources into a unified knowledge graph. It exists to make disaster information easier for the public to discover and understand while preserving the structured detail researchers need for analysis.

Success means the available data is kept up to date, accurate, and easy for an average user to consume.

## Positioning

SakunaGraPH combines an OWL 2 disaster ontology, cross-source entity resolution, and a GraphDB-backed evidence model to provide one coherent view of records from NDRRMC, GDA, EM-DAT, PSGC, and DROMIC. Public-facing questions, maps, and analysis views are grounded in the same graph that supports direct SPARQL inspection and provenance-aware research.

## Operating Context

People use the web application to:

- ask natural-language questions about Philippine disasters;
- explore events geographically through the map;
- browse and export filtered event records;
- compare disaster metrics and temporal patterns;
- inspect the ontology, taxonomy, and Philippine administrative hierarchy; and
- run read-only SPARQL queries when direct graph access is appropriate.

The underlying workflow ingests raw PDF, spreadsheet, and web data; normalizes and enriches it; maps it to RDF; resolves related records across sources; validates the result; and loads it into GraphDB.

## Capabilities and Constraints

- GraphDB is the system of record for graph entities and retrieved facts.
- The integrated sources are NDRRMC, GDA, EM-DAT, PSGC, and DROMIC.
- The product models disaster events, incidents, impacts, response activity, organizations, provenance, and Philippine administrative geography using RDF/OWL.
- The public application provides Home/SPARQL, Map, Ontology, Ask, and Analysis surfaces.
- Ask uses graph-grounded retrieval. Its local language model may interpret questions and word answers, but answers must remain constrained by validated graph evidence.
- Direct and generated SPARQL are read-only.
- Researchers must be able to reach supporting query details, rows, citations, source records, and provenance when those are available.
- Data freshness and accuracy depend on the source-specific ETL and validation workflows; the interface must not imply stronger coverage or certainty than the loaded evidence supports.

## Brand Commitments

- The product name is **SakunaGraPH**.
- The established description is **An Ontology-Based Knowledge Graph for Disaster Data Integration**.
- Public-facing language should be understandable without requiring familiarity with RDF, OWL, GraphDB, or SPARQL. Specialist terminology remains available where it helps researchers inspect the graph precisely.

## Evidence on Hand

- The ontology and disaster-type scheme are maintained in `ontology/sakunagraph.ttl` and `ontology/disaster_type_scheme.ttl`.
- Ontology validation artifacts and competency questions are maintained under `ontology/validation/` and `ontology/shapes/`.
- The production ETL package, source integrations, entity resolution, quality checks, and deployment workflows are maintained under `sakunagraph_etl/`.
- The FastAPI graph, analysis, map, ontology, disaster-detail, SPARQL, and Ask services are maintained under `api/`.
- The SvelteKit public interface and its unit, contract, accessibility, end-to-end, performance, and visual regression tests are maintained under `frontend/`.
- Repository evidence demonstrates implemented capabilities and test coverage; it does not establish testimonials, customer adoption, external validation, complete historical coverage, or guaranteed real-time freshness. Future work must not fabricate those claims.

## Product Principles

1. **Make disaster data legible to everyone.** Lead with plain-language understanding and progressive disclosure, while retaining expert depth.
2. **Keep every answer accountable to evidence.** Preserve sources, query details, provenance, and uncertainty instead of presenting unsupported certainty.
3. **Treat freshness and accuracy as product outcomes.** Make data state and coverage honest, and support the workflows that keep the graph current and validated.
4. **Unify without erasing source meaning.** Resolve related records across datasets while retaining alternate records and source identity.
5. **Support exploration at multiple levels.** Let people move from a simple question or map view to detailed records, analysis, ontology structure, and SPARQL without changing the underlying truth.

## Accessibility & Inclusion

The product must work for non-specialists as well as technical users. Use plain language for primary tasks, expose specialist detail progressively, support keyboard and touch interaction, preserve readable contrast, respect reduced-motion preferences, and keep responsive layouts usable on mobile web.
