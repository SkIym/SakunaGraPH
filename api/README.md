# SakunaGraPH API

The SakunaGraPH API is a FastAPI application that exposes the disaster
knowledge graph through analytics, map, ontology, event-detail, SPARQL, and
natural-language question-answering endpoints. GraphDB is the system of record.
A configurable local language model helps interpret questions and, when
necessary, word answers from already validated graph evidence.

## Current GraphRAG scope

In this repository, **GraphRAG** currently means graph-grounded retrieval and
answer generation over GraphDB. The runtime does not currently build embeddings,
retrieve text chunks, use a vector database, or maintain a separate retrieval
index. The current Ask workflow combines:

- A structured LLM planner
- GraphDB-backed entity resolution
- Existing deterministic API services or compiled SPARQL
- SPARQL structure and ontology-schema validation for compiler/model queries
- Read-only graph retrieval
- Result validation, provenance extraction, and evidence IDs
- Deterministic answers or an evidence-constrained LLM answer

## Runtime architecture

```text
Browser / frontend
        |
        v
Caddy gateway (/api/*)
        |
        v
FastAPI application
  |-- request and response schemas
  |-- analysis router ------> analysis services -----+
  |-- map router -----------> map services ----------|
  |-- ontology router ------> ontology services -----|--> SPARQL executor --> GraphDB
  |-- disasters router -----> disaster services -----|
  |-- SPARQL router ---------------------------------+
  `-- Ask router
        |-- structured planning --------------------------> local LLM
        |-- entity resolution ----------------------------> GraphDB
        |-- service routing / deterministic compilation
        |-- SPARQL validation when query-backed; result validation for all paths
        |-- graph retrieval ------------------------------> GraphDB
        `-- conditional grounded answer generation ------> local LLM
```

The application is a layered modular monolith:

| Layer | Location | Responsibility |
| --- | --- | --- |
| Application | `src/main.py`, `src/config.py` | Construct FastAPI, register middleware and routers, load settings |
| HTTP | `src/routers/` | Parse HTTP inputs, call services, translate service errors |
| Contracts | `src/schemas/` | Pydantic request, response, planning, execution, and evidence models |
| Domain services | `src/services/analysis/`, `disasters/`, `map/`, `ontology/`, `ask/` | Implement feature behavior and shape graph results |
| GraphDB adapter | `src/services/sparql/` | Validate and execute read-only SPARQL |
| LLM adapter | `src/services/llm.py` | Non-streaming and streaming calls to the configured local model |
| Shared errors | `src/services/common/` | Carry HTTP-safe status codes and error details across layers |

All feature routers are mounted under `/api`. The API also exposes `/health` and
the gateway-compatible `/api/health` alias.

## Request paths

The principal endpoint groups are:

| Group | Representative endpoints | Purpose |
| --- | --- | --- |
| Ask | `POST /api/ask`, `/api/ask/preview`, `/api/ask/stream` | Natural-language graph questions |
| Analysis | `GET /api/analysis/events`, `/summary`, `/victim-trends`, rankings and timeline routes | Dashboard data and CSV export |
| Disasters | `GET /api/disasters/details`, event impact, organization, and source routes | Detailed disaster records and provenance |
| Map | `GET /api/map/events` | Geographic event lists and counts |
| Ontology | `GET /api/ontology/graph`, `/taxonomy`, `/psgc` | Ontology and PSGC visualizations |
| SPARQL | `POST /api/sparql` | Direct read-only SELECT queries |
| Meta | `GET /health`, `GET /api/health` | Process liveness only |

FastAPI generates the complete interactive contract at `/docs` and the OpenAPI
document at `/openapi.json`.

The map endpoint accepts `region`, `province`, `city`, and `municipality` scopes. Its `id` is the
10-digit PSGC code for the selected geography, including NCR cities and Pateros.

## GraphRAG / Ask deep dive

The Ask feature treats LLM output as a proposal that must pass deterministic
validation. The model never communicates with GraphDB directly and never decides
whether a query is safe to run.

### Data objects through the pipeline

```text
AskRequest
  query: original user text
        |
        v
AskPlan
  intent, metric, dates, grouping, limit, entity mentions
        |
        v
ResolvedAskPlan
  canonical entity IRIs, match confidence, warnings, ambiguities
        |
        v
QueryArtifact
  execution origin, optional SPARQL, service route, expected entities and columns
        |
        v
SPARQL JSON results + ResultValidationReport
  validated columns, RDF terms, row count, truncation
        |
        v
AskAnswerContext
  typed rows, units, evidence IDs, provenance, warnings
        |
        v
AskResponse or SSE events
```

### 1. HTTP input validation

`src/routers/ask.py` accepts a JSON body such as:

```json
{
  "query": "How many deaths were reported for floods in 2023?"
}
```

`AskRequest` trims the question, rejects whitespace-only input, and enforces a
2,000-character maximum. Invalid request bodies are rejected by FastAPI before
the Ask service or LLM is called.

**LLM involvement:** none.

### 2. Structured planning

`src/services/ask/planner.py` sends the question, the current date, planning
rules, examples, and the JSON Schema for `AskPlan` to the local model. The model
must return one JSON object rather than SPARQL.

The plan records:

- Intent, such as `event_count`, `impact_summary`, `event_details`, or
  `open_graph_query`
- Major event, incident, or combined scope
- Location, disaster-type, event, and organization mentions copied from the
  question
- Start and end dates
- Metric, such as deaths, affected persons, or damage
- Grouping and sort direction
- A bounded result limit from 1 to 100

The parser rejects duplicate JSON keys, nonstandard constants, unknown fields,
SPARQL-looking entity mentions, invalid dates, and output over 12,000
characters. If the first plan is invalid, the model receives exactly one repair
attempt. Failure after that attempt ends the request with `validation_failed`.

**LLM involvement:** mandatory. This is the first LLM call for every Ask,
preview, or streaming request.

### 3. Deterministic normalization

`src/services/ask/normalization.py` normalizes values that should not depend on
model reasoning. It expands calendar phrases such as `in 2023`, `last year`, or
`January through March 2024`; maps aliases such as `fatalities` to `dead`; and
corrects a narrow disaster-type-listing intent ambiguity.

The result is a strongly typed, query-language-free `AskPlan`. The original
question is not inserted into a graph query.

**LLM involvement:** none. This step checks and corrects the planner proposal
with ordinary Python code.

### 4. GraphDB-backed entity resolution

`src/services/ask/entity_resolver.py` loads only the catalogs required by the
plan. Locations, disaster types, named events, organizations, and casualty
types are read from GraphDB in parallel and cached in-process for 15 minutes.

Each surface mention is matched in this order:

1. Exact graph identifier or label
2. Known alias
3. Geographic hierarchy alias
4. Fuzzy string match with a minimum similarity of 0.86

The result includes the canonical IRI, identifier, graph label, match method,
and confidence. Casualty language such as `deaths` is resolved to controlled
resources such as `:Dead`, preventing ad hoc string comparisons in SPARQL.

**LLM involvement:** none. Entity identity comes from approved GraphDB catalogs,
not from model memory.

### 5. Ambiguity and unresolved-entity gate

A unique match is added to `ResolvedAskPlan`. Multiple close matches produce an
`EntityAmbiguity` with candidate entities. The request then returns
`needs_disambiguation` before query construction or execution.

A mention with no match produces a warning. A shared readiness guard requires
all requested entities to resolve before either service dispatch or query
generation, so an unmatched filter cannot be silently dropped from the
question.

**LLM involvement:** none. The API asks the user to clarify rather than asking
the model to guess.

### 6. Route selection

`src/services/ask/service_router.py` chooses the narrowest deterministic
execution path supported by the resolved plan:

- Existing analysis services handle common event lists, counts, summaries,
  trends, and rankings.
- Existing disaster services handle event details and source lookup.
- `src/services/ask/query_compiler.py` handles other supported intents with
  fixed SPARQL templates.
- Only `open_graph_query` uses model-generated SPARQL as a constrained fallback.

The service route is preferred because it reuses tested domain logic and stable
response semantics. The compiler uses resolved IRIs, normalized dates, fixed
graph patterns, deterministic ordering, and a bounded `LIMIT`; it does not
interpolate the original question.

For `open_graph_query`, the local model receives a fixed SakunaGraPH ontology
summary from `src/services/ask/context.py` and proposes a SELECT query. That
proposal still has to pass every validation step below.

**LLM involvement:** conditional. It generates SPARQL only for the
`open_graph_query` fallback. Common and supported graph questions use domain
services or deterministic templates.

### 7. Query artifact construction

Every path produces a `QueryArtifact` containing:

- SPARQL for compiler and model-fallback paths; an empty string for service paths
- Its origin: `service`, `compiler`, or `model_fallback`
- An optional domain-service route
- Projected and expected columns
- Expected resolved entity IRIs
- Expected metric and grouping
- Compilation warnings

For a service-backed request, the artifact is execution metadata rather than a
query. It records the selected service, resolved entities, requested metric and
grouping, and expected output columns. It does not manufacture unused SPARQL and
does not pass through Ask SPARQL validation. The top-level `sparql` response
field remains an empty string for contract compatibility.

The artifact later becomes part of response diagnostics, result validation, and
evidence provenance.

**LLM involvement:** none, except that an `open_graph_query` artifact contains
the model proposal from the previous step.

### 8. SPARQL safety and semantic validation

For compiler and `model_fallback` artifacts,
`src/services/ask/query_validator.py` and `src/services/sparql/policy.py` parse
the exact query that will be sent to GraphDB using RDFLib and enforce:

- SELECT-only queries and explicit projected variables
- Approved prefixes and external vocabulary terms
- No `BASE`, `SERVICE`, `GRAPH`, `MINUS`, or dataset clauses
- Configured limits on query length, triples, OPTIONALs, UNIONs, and subqueries
- A bounded top-level limit where required
- Terms that exist in the live GraphDB schema catalog
- Compatible known property domains and ranges
- Presence of every resolved entity and requested date
- Required aggregation, grouping, ordering, and exact projected columns

The schema catalog is retrieved from GraphDB and cached for 15 minutes. A query
can be valid SPARQL yet still fail here if it uses the wrong SakunaGraPH term or
does not match the interpreted plan.

Service-backed artifacts skip this Ask-level validator because they contain no
query. `select_service_route()` already restricts supported intent, metric,
grouping, and entity combinations. A shared readiness guard still rejects
ambiguous entities, unmatched requested entities, and invalid resolved IRIs
before dispatch. The domain service's actual internal SPARQL continues through
the shared SELECT-only SPARQL executor before reaching GraphDB.

**LLM involvement:** none. The model cannot override this gate.

### 9. Retrieval and execution

`src/services/ask/service.py` executes the prepared artifact in one of two ways:

- A service-backed artifact calls the selected analysis or disaster service.
  Its typed response is flattened into a common SPARQL JSON-result shape.
- A compiler or model-fallback artifact is sent to GraphDB through
  `src/services/sparql/executor.py`.

`execute_service_route()` is an in-process dispatcher; it does not call a
FastAPI route or merely hand a query to one. It converts the resolved plan into
the filters and arguments expected by the selected domain service. That service
constructs and executes its own GraphDB query and returns typed data, which the
dispatcher converts into Ask rows.

The GraphDB adapter independently rejects write operations, validates SELECT
syntax again, applies the configured timeout, optionally uses read-only
credentials, and truncates results to the configured Ask row limit.

**LLM involvement:** none. The API, not the model, owns retrieval and GraphDB
credentials.

### 10. Result validation

`src/services/ask/result_validator.py` verifies that the response has a valid
SPARQL SELECT shape, contains every expected column, stays within the row limit,
uses finite numeric aggregate values, and includes grouping values when the plan
requires them. Truncation is preserved as an explicit warning.

Malformed or semantically incompatible results stop the request before answer
generation.

**LLM involvement:** none.

### 11. Answer context, evidence, and provenance

`src/services/ask/answer.py` converts validated RDF bindings into
`AskAnswerContext`. Each result term receives its raw value, display value,
datatype, language, and unit where applicable. Each row receives an evidence ID
such as `E1` or `E2`.

Evidence provenance can include:

- Query origin and domain-service route
- SHA-256 execution fingerprint: the exact SPARQL for query-backed paths, or the
  normalized plan and service metadata for service-backed paths
- Source organization IRIs and labels
- Source-record IRIs
- Report links

Fuzzy entity matches mark the context as approximate and add a confidence
warning. Truncated results remain marked as truncated. Even a zero-row result
gets an `E1` evidence record stating that the validated query returned no rows.

The answer context is included for successful `POST /api/ask` responses,
including `no_data`, and in the successful stream's `meta` event. It is absent
from preview, disambiguation, and error responses because those paths stop
before context construction.

**LLM involvement:** none.

### 12. Answer generation

The API first attempts a deterministic answer. Current deterministic renderers
cover zero-row results, ungrouped counts and summaries, and event,
disaster-type, or source listings.

When no deterministic renderer fits, the local model receives only the
validated structured answer context. The grounding prompt requires the model to
cite factual statements with evidence IDs, preserve units, mention truncation
or approximation, and avoid inventing facts, identifiers, provenance, or
evidence. Missing evidence references are appended after generation.

**LLM involvement:** conditional. The LLM words complex answers but does not
retrieve data or receive unvalidated GraphDB results.

### 13. Response transport

`POST /api/ask` returns one `AskResponse` with the interpreted plan, SPARQL,
answer, display rows, artifact, warnings, evidence, and answer context.

`POST /api/ask/preview` stops after preparing the execution artifact. Compiler
and model-fallback queries are fully validated; service-backed previews return a
ready service artifact with empty `sparql`. Preview never executes the service
or query, builds answer context, or generates an answer.

`POST /api/ask/stream` emits server-sent events in this successful order:

```text
meta -> results -> zero or more warning events -> token events -> done
```

The `meta` event contains the execution artifact, interpretation, rows,
evidence, and answer context. A disambiguation stream emits `meta`, a clarification
`token`, and `done`. A service failure is emitted as an `error` event.

### Where the LLM is and is not involved

| Phase | LLM used? | Model responsibility |
| --- | --- | --- |
| Request validation | No | None |
| Structured planning | Always | Propose a constrained JSON interpretation |
| Date/metric normalization | No | None |
| Entity resolution | No | None; GraphDB catalogs are authoritative |
| Ambiguity handling | No | None; the user must clarify |
| Service routing | No | None |
| Deterministic compilation | No | None |
| Open graph fallback | Sometimes | Propose SPARQL from fixed ontology context |
| Query validation | No | Compiler/model SPARQL only; deterministic parser and schema checks decide |
| GraphDB execution | No | None |
| Result validation | No | None |
| Evidence construction | No | None |
| Simple answer rendering | No | None |
| Complex answer rendering | Sometimes | Word an answer from validated evidence only |

## Ask statuses and failures

| Status | Meaning |
| --- | --- |
| `query_ready` | Preview produced a ready service artifact or validated SPARQL artifact |
| `answered` | The validated execution returned data |
| `no_data` | The validated execution returned no matching rows |
| `needs_disambiguation` | A mention matched multiple graph entities |
| `validation_failed` | Planning, compilation, query, or result validation failed |
| `execution_failed` | GraphDB, schema-catalog, or deterministic-service execution failed |
| `generation_failed` | Another LLM-generation failure occurred |

## Configuration

Settings are loaded from environment variables and an optional `.env` file in
the working directory. Run local commands from `api/` so its `.env` file is
found.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CORS_ORIGINS` | `["*"]` | Allowed browser origins; restrict outside development |
| `GRAPHDB_ENDPOINT` | `http://localhost:7200/repositories/sakunagraph` | GraphDB repository query endpoint |
| `GRAPHDB_READ_ONLY_USERNAME` | unset | Optional read-only GraphDB user |
| `GRAPHDB_READ_ONLY_PASSWORD` | unset | Optional read-only GraphDB password |
| `GRAPHDB_QUERY_TIMEOUT_SECONDS` | `30` | GraphDB query timeout |
| `LOCAL_LLM_BASE_URL` | `http://127.0.0.1:1234` | Local model server |
| `LOCAL_LLM_CHAT_PATH` | `/api/v1/chat` | Chat endpoint path |
| `LOCAL_LLM_MODEL` | `google/gemma-4-e4b` | Requested model identifier |
| `LOCAL_LLM_TIMEOUT` | `120` | Model request timeout |
| `LOCAL_LLM_STORE` | `false` | Whether the model server may store requests |
| `ASK_SPARQL_MAX_LENGTH` | `30000` | Maximum validated Ask query length |
| `ASK_SPARQL_MAX_TRIPLES` | `80` | Maximum triple patterns |
| `ASK_SPARQL_MAX_OPTIONALS` | `30` | Maximum OPTIONAL blocks |
| `ASK_SPARQL_MAX_UNIONS` | `12` | Maximum UNION groups |
| `ASK_SPARQL_MAX_SUBQUERIES` | `12` | Maximum subqueries |
| `ASK_RESULT_ROW_LIMIT` | `100` | Maximum Ask result rows |

Both GraphDB credential variables must be set together. The liveness route does
not check GraphDB or the LLM.

## Local development

From `api/`:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --requirement requirements.txt
uvicorn src.main:app --reload --host 127.0.0.1 --port 8000
```

Then open:

- API documentation: `http://127.0.0.1:8000/docs`
- OpenAPI schema: `http://127.0.0.1:8000/openapi.json`
- Liveness: `http://127.0.0.1:8000/health`

Graph-backed endpoints require the configured GraphDB repository. Ask endpoints
also require the local model for structured planning, even when the final answer
can be rendered deterministically.

## Tests and evaluation

Run the unit and regression suite from `api/`:

```powershell
python -m unittest discover -s tests -t . -v
```

Ask fixtures cover planning, entity resolution, compilation, grounding,
streaming, and validation. The evaluation harness and recorded baseline are
documented in `evaluation/README.md`.

Dry-run the Ask evaluation without contacting the model or GraphDB:

```powershell
python scripts/evaluate_ask.py --dry-run
```

The planner evaluation contacts the configured model but not GraphDB:

```powershell
python scripts/evaluate_ask_planner.py
```

## Container deployment

The repository-root `docker-compose.yml` starts Caddy, the frontend, and this
API. GraphDB and the local model remain configurable upstream services.

From the repository root:

```powershell
Copy-Item .env.compose.example .env
docker compose up --build --wait
```

Open `http://localhost:8080`. Caddy forwards `/api/*` to Uvicorn and disables
response buffering for Ask streams. The API container runs as an unprivileged
user and exposes port 8000 only to the internal Compose network.

## Important operational boundaries

- GraphDB is authoritative for graph entities and retrieved facts.
- The local model is required for Ask planning but never receives GraphDB
  credentials.
- Direct and generated SPARQL are restricted to SELECT operations by application
  policy; deployments should also use a read-only GraphDB identity.
- In-memory caches are local to one API process and are neither persistent nor
  shared between workers.
- The current `/health` endpoint reports process liveness, not GraphDB or LLM
  readiness.
- A future vector or embedding index should be built and versioned outside API
  startup.
