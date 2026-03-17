# Backend Architecture Comparison: REST vs GraphQL & AWS Deployment

## 1. REST vs GraphQL as the Service Layer

Currently on the `backend` branch with no API layer yet — just GraphDB accessed directly at `localhost:7200`. Here's how the two options compare for this use case:

### RESTful API (e.g., FastAPI + SPARQL)

**Scalability strengths:**
- Stateless by design — horizontally scales trivially behind a load balancer (ALB/NLB on AWS)
- HTTP caching (`ETag`, `Cache-Control`) works natively — disaster data that doesn't change frequently benefits hugely from this
- Each endpoint maps to a specific SPARQL query, so you can optimize and cache per-route
- Mature ecosystem: rate limiting, auth middleware, API gateways (API Gateway + Lambda or ECS)

**Weaknesses for this case:**
- The ontology has deeply nested relationships (Event → Incident → Casualties/Damage/Relief → QuantityValue → Unit). REST requires either many round-trips or custom "fat" endpoints that embed related data
- As the ontology grows, there will be a proliferation of endpoints or complex query parameter schemes
- Clients can't control response shape — they get whatever the endpoint returns

### GraphQL

**Scalability strengths:**
- Single endpoint, client-driven queries — perfect for a graph/ontology with deep, variable-depth relationships
- Clients fetch exactly what they need in one request (e.g., "give me this event's casualties and relief amounts but not infrastructure damage")
- Schema is self-documenting and maps naturally to the OWL class hierarchy
- Reduces over-fetching and under-fetching, which matters when triples span 15+ entity types

**Weaknesses for this case:**
- No native HTTP caching (single POST endpoint) — needs application-level caching (Redis, or query-result caching)
- Complex queries can produce expensive SPARQL under the hood — needs query complexity analysis/depth limiting to prevent abuse
- Horizontally scalable but requires more thought around query cost bounding
- Resolver N+1 problem: naive resolvers can generate many SPARQL queries per GraphQL request. Needs DataLoader-style batching

### Recommendation

**GraphQL is the stronger fit** for these reasons:

1. The ontology is inherently a graph with variable-depth traversal (Event → Incident → Location → Province → Region, or Event → Relief → QuantityValue → Unit). GraphQL's query model mirrors this naturally.
2. There are multiple data consumers with different needs (researchers wanting full event details vs. dashboards wanting aggregates). GraphQL lets each client shape its own query.
3. The data is relatively read-heavy and write-infrequent (ETL batch loads), so the caching disadvantage is manageable.
4. Libraries like **Strawberry** (Python, async, type-safe) or **Ariadne** integrate well with FastAPI and can translate GraphQL queries into SPARQL against GraphDB.

A practical hybrid: expose GraphQL as the primary query interface, but keep a few REST endpoints for simple operations (health checks, ETL triggers, bulk data exports).

---

## 2. EC2 (Co-located) vs Amazon Neptune

### Option A: EC2 with GraphDB co-located

| Aspect | Details |
|--------|---------|
| **Cost** | Cheapest at small scale. A single `t3.large` or `m5.xlarge` can run both GraphDB and the API |
| **Ops burden** | You manage everything: OS patches, GraphDB upgrades, backups, disk management, failover |
| **Scalability** | Vertical only (bigger instance). No native HA — you'd need to set up replication yourself |
| **Data model** | Full RDF/SPARQL 1.1 + GeoSPARQL + OWL reasoning — GraphDB supports all current features |
| **Migration** | Zero effort, it's what is already run locally |

### Option B: Amazon Neptune

| Aspect | Details |
|--------|---------|
| **Cost** | More expensive baseline (~$0.348/hr for `db.r5.large` ≈ $253/mo minimum), but includes managed HA, backups, encryption |
| **Ops burden** | Fully managed — automated backups, patching, multi-AZ failover, encryption at rest |
| **Scalability** | Up to 15 read replicas, storage auto-scales to 128 TB, supports Neptune Serverless for variable workloads |
| **Data model** | Supports SPARQL 1.1 over RDF — compatible with Turtle data. But: **no OWL reasoning, limited GeoSPARQL support** |
| **Migration** | Bulk load via S3 (Neptune Loader). Existing `.ttl` files work directly. `load_graphdb.py` would need rewriting to use Neptune's loader API |

### Option C (worth considering): EC2 for GraphDB + separate API on ECS/Lambda

Best of both worlds for moderate scale:
- GraphDB on a dedicated EC2 instance (or EBS-backed, with snapshots for backup)
- API layer (FastAPI + GraphQL) on ECS Fargate or Lambda — auto-scales independently
- Keeps the full GraphDB feature set (reasoning, GeoSPARQL) while scaling the API tier

### Recommendation

**Start with EC2 (co-located), plan for Option C as you grow.** Here's why:

1. **Neptune lacks OWL reasoning.** The ontology imports beAWARE, uses SKOS hierarchies, and has class inheritance (DisasterEvent → MajorEvent). If any inferencing is relied upon, Neptune can't do it.

2. **GeoSPARQL support in Neptune is limited.** The ontology imports GeoSPARQL 1.1 and PSGC data includes geometries. GraphDB has full GeoSPARQL support; Neptune's is partial.

3. **Data volume is modest.** Philippine disaster records across NDRRMC, GDA, EM-DAT, and PSGC are likely in the low millions of triples — well within what a single EC2 instance handles.

4. **Cost matters for a research project.** A `t3.medium` (~$30/mo) can handle the current scale. Neptune's minimum is ~$253/mo.

5. **When to reconsider Neptune:** If you hit >100M triples, need multi-AZ availability, or have multiple concurrent heavy query users, Neptune Serverless becomes compelling for its auto-scaling and zero-ops maintenance.

---

## Suggested AWS Architecture (near-term)

```
                    ┌─────────────┐
  Users ──────────► │  API Gateway │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   EC2       │
                    │ ┌─────────┐ │
                    │ │ FastAPI  │ │
                    │ │ GraphQL  │ │
                    │ └────┬────┘ │
                    │      │      │
                    │ ┌────▼────┐ │
                    │ │ GraphDB │ │
                    │ │ :7200   │ │
                    │ └─────────┘ │
                    └─────────────┘
                           │
                    ┌──────▼──────┐
                    │  S3 Bucket  │
                    │ (TTL files, │
                    │  ETL output)│
                    └─────────────┘
```

When scaling is needed, split the API into ECS Fargate and keep GraphDB on its own EC2 with EBS snapshots for backup.
