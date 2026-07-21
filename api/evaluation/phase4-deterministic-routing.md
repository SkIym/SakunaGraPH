# Phase 4 Deterministic Routing Audit

## Scope

Phase 4 routes compatible structured Ask plans to the existing analysis and
disaster-detail services. Supported plans that need grouping or entity scopes
not accepted by those services are compiled into deterministic read-only
SPARQL. Only `open_graph_query` retains the model-generated fallback.

## Service routes

| Intent | Existing API service |
| --- | --- |
| `list_events` | `analysis.events.get_analysis_events` |
| `event_count` | `analysis.events.get_analysis_events` total |
| `impact_summary` | `analysis.metrics.get_summary` |
| `victim_trend` | `analysis.metrics.get_victim_trends` |
| `region_ranking` | `analysis.metrics.get_region_rankings` |
| `disaster_ranking` | `analysis.metrics.get_disaster_rankings` |
| `event_details` | `disasters.details.get_event_details` |
| `source_lookup` | `disasters.details.get_disaster_sources` |

The router uses a service only when its filter and metric semantics match the
resolved plan. Other supported combinations use the deterministic compiler.

## Graph-shape audit

The compiler reuses `AnalysisFilters` and `event_filter_where`, including the
existing alternate-event deduplication behavior. Compiler templates use the
same graph shapes already consumed by API services:

- location filtering through `:hasLocation/:isPartOf*`;
- disaster taxonomy filtering through
  `(:hasDisasterType|:hasDisasterSubtype)/skos:broader*`;
- controlled casualty resources `:Dead`, `:Injured`, and `:Missing`;
- affected-population properties on `:hasAffectedPopulation` records;
- damage measures through `qudt:numericValue` and `qudt:unit`; and
- provenance through `prov:wasDerivedFrom+` and `prov:wasAttributedTo`.

No new graph-shape mismatch was identified during the Phase 4 static and
contract test audit. If a deployed GraphDB dataset exposes a mismatch, it must
be recorded under `api/evaluation/` and handled in the API; ontology, RDF, and
ETL artifacts remain outside this plan's write scope.

## Verification

- Exact compiler output is locked by a SPARQL snapshot.
- All supported compiler variants are parsed and checked as read-only.
- Golden service-result contracts cover event counts, casualty totals, and
  multi-unit damage totals.
- Integration tests prove common service and compiler paths do not invoke
  model-generated SPARQL.
