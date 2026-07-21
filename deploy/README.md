# Frontend and API deployment

The root `docker-compose.yml` runs three services:

- `gateway`: the only published port; Caddy routes `/api/*` to FastAPI and all other requests to
  the adapter-node frontend. `flush_interval -1` prevents proxy buffering of GraphRAG SSE events.
- `frontend`: the production SvelteKit adapter-node build, running as the unprivileged `node` user.
- `api`: the FastAPI application, running as the unprivileged `sakuna` user.

Copy `.env.compose.example` to `.env` and adjust the GraphDB and local-model addresses when they
are not running on the Docker host. Then build and start the application from the repository root:

```bash
docker compose up --build --wait
```

Open `http://localhost:8080`. Stop it with `docker compose down`. Neither the frontend nor API port
is published directly; this is important because adapter-node trusts the forwarded host and
protocol headers supplied by the internal Caddy service. `ORIGIN` must match the URL users open.

The liveness endpoints are:

- Gateway/frontend: `GET /health`
- API through the gateway: `GET /api/health`

These checks report process availability only. They intentionally do not fail when GraphDB or the
local model server is unavailable.

## Deployment contract

From `frontend/`, run:

```bash
npm run test:deployment
```

The command builds the real images with a deterministic test-only GraphDB/model upstream, starts
the same Caddy topology on port 4176, and verifies direct routes, same-origin API routing, GeoJSON,
and unbuffered GraphRAG streaming. It always removes its containers and network afterward.
