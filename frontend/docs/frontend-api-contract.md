# Frontend/API contract freeze

The generated source of truth is FastAPI's `app.openapi()`. The normalized snapshot at
`tests/contracts/openapi.snapshot.json` contains every parameter, request schema, response status,
response field, and nested model consumed by the frontend. `npm run test:contract:live` regenerates
that view from the API and fails on an incompatible difference; `npm run test:contract` validates
the committed inventory without requiring a local Python environment.

## Consumed operations

The common analysis filter parameters are `event_type`, `start_date`, `end_date`, repeated
`location_ids`, repeated `disaster_types`, and `q`.

| Method and path                              | Additional input                                    | Successful response / download                                                 |
| -------------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------ |
| `POST /api/sparql`                           | JSON `query`                                        | SPARQL `head`, `results`, or `boolean`                                         |
| `POST /api/ask`                              | JSON `query`                                        | `sparql`, `answer`, `rows`                                                     |
| `POST /api/ask/preview`                      | JSON `query`                                        | `sparql` (protected, not yet used by the page)                                 |
| `POST /api/ask/stream`                       | JSON `query`                                        | SSE stream (protected, feature-flag candidate)                                 |
| `GET /api/map/events`                        | `scope`, `id`, `mode`, `page`                       | `events`, `majorCount`, `incidentCount`                                        |
| `GET /api/disasters/details`                 | `uri`                                               | Event overview, remarks, locations, types, related events, alternates, sources |
| `GET /api/ontology/graph`                    | None                                                | `nodes`, `links`                                                               |
| `GET /api/ontology/taxonomy`                 | None                                                | Recursive taxonomy node                                                        |
| `GET /api/ontology/psgc`                     | None                                                | `nodes`, `links`                                                               |
| `GET /api/analysis/filter-options`           | None                                                | `locations`, `disasterTypes`                                                   |
| `GET /api/analysis/events`                   | Common + `page`, `page_size`, `sort_by`, `sort_dir` | `items`, pagination, sort fields                                               |
| `GET /api/analysis/events/export.csv`        | Common + `sort_by`, `sort_dir`                      | CSV blob; filename from `Content-Disposition` or `sakunagraph-events.csv`      |
| `GET /api/analysis/summary`                  | Common                                              | Counts and damage totals                                                       |
| `GET /api/analysis/disaster-counts`          | Common + `group_by`                                 | `group_by`, `items`                                                            |
| `GET /api/analysis/victim-trends`            | Common + `disaster_type`                            | `items`                                                                        |
| `GET /api/analysis/region-rankings`          | Common + `disaster_type`                            | `items`                                                                        |
| `GET /api/analysis/disaster-rankings`        | Common + `location_id`                              | `items`                                                                        |
| `GET /api/analysis/damage-histogram`         | Common + `bins`, `unit`                             | `bins`                                                                         |
| `GET /api/analysis/damage-vs-affected`       | Common                                              | `items`                                                                        |
| `GET /api/analysis/calendar/years`           | Common + `include_impacts`                          | `items`                                                                        |
| `GET /api/analysis/calendar/months`          | Common + `year`, `include_impacts`                  | `items`                                                                        |
| `GET /api/analysis/calendar/days`            | Common + `year`, `month`, `include_impacts`         | `items`                                                                        |
| `GET /api/analysis/timeline/category-stacks` | Common + `bucket`                                   | `bucket`, `items`                                                              |
| `GET /api/analysis/timeline/date-events`     | Common + `date_prefix`                              | `date_prefix`, `items`                                                         |

The map also downloads the versioned same-origin static asset `/data/regions.geojson`; it is not an
API operation and remains the active geographic source.

## Shared client boundary

All backend traffic is owned by `src/lib/api/`: `client.js` handles base URLs, query strings,
timeouts, caller cancellation, response parsing, request IDs, and normalized errors. Domain modules
cover analysis, ask, disasters, map, ontology, and SPARQL operations. `src/lib/api.js` remains as a
compatibility export; route and component code imports domain modules directly.

Requests default to same-origin paths. A trailing slash is removed when
`PUBLIC_API_BASE_URL=https://api.example.test/` is configured, producing URLs such as
`https://api.example.test/api/ask`. The unit suite verifies both forms. API errors expose a stable
`kind` (`http`, `graphdb`, `response`, `timeout`, `cancelled`, or `network`), HTTP status, and request
ID where the response provides one.

`PUBLIC_ASK_STREAMING_ENABLED=false` keeps legacy `POST /api/ask` as the default. When set to a
truthy value, the ask feature consumes `POST /api/ask/stream`. A transport or protocol failure
before the first valid `meta` event retries once through the legacy endpoint and labels the answer
as fallback. Once metadata has arrived, an error or disconnect is shown on that answer instead of
duplicating upstream work. Cancellation, replacement questions, and route destruction abort the
active request and close its response body.

## Additive GraphRAG contract v1

The legacy non-streaming fields `sparql`, `answer`, and `rows` remain required. GraphRAG may add
these optional fields without changing current rendering:

```json
{
	"citations": [
		{
			"id": "citation-1",
			"label": "Source label",
			"uri": "https://example.test/source",
			"sourceRecord": "optional record IRI",
			"excerpt": "optional bounded excerpt"
		}
	],
	"retrieval": {
		"mode": "legacy | graphrag | fallback",
		"indexVersion": "optional immutable index version",
		"sourceCount": 1
	}
}
```

All citation fields except `id`, `label`, and `uri` are optional. Citation IDs are unique within one
answer and never reused to identify a different source in that answer.

`POST /api/ask/stream` continues to use SSE messages whose `data` is one JSON object:

- `meta`: required `type`, `sparql`, and `rows`; optional `citations`, `retrieval`, `requestId`.
- `token`: required `type` and non-empty `text`.
- `citation`: required `type` and `citation` using the shape above.
- `done`: required `type`; optional final `citations` and `retrieval`.
- `error`: required `type`, integer `status`, and user-safe `detail`; optional `requestId`.

Ordering is one `meta`, zero or more `token`/`citation` messages, then exactly one terminal `done` or
`error`. Unknown additive event types are ignored. A client disconnect or replacement request must
cancel upstream work. Citation versions such as `citation.v1` use the same citation shape and stay
associated with the assistant answer that received them. The API response includes
`Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`; the deployment proxy must honor
those headers and avoid response buffering.

Streamed prose is rendered as ordinary text, not an `aria-live` region. A separate polite, atomic
status announces only generation start, completion, cancellation, and failure. Citations and
retrieval provenance are rendered after the prose in a distinct answer-sources section. The legacy
endpoint remains supported for rollout and rollback while the feature flag defaults to off.
