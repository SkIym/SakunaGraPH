import { json } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';

// Default points at a local GraphDB instance — override via GRAPHDB_ENDPOINT env var
const GRAPHDB_ENDPOINT =
	env.GRAPHDB_ENDPOINT ?? 'http://localhost:7200/repositories/SakunaGraph';

// Block all SPARQL Update operations — this endpoint is strictly read-only
const WRITE_PATTERNS = [
	/\bINSERT\b/i,
	/\bDELETE\b/i,
	/\bCLEAR\b/i,
	/\bDROP\b/i,
	/\bCREATE\s+GRAPH\b/i,
	/\bLOAD\b/i,
	/\bCOPY\s+GRAPH\b/i,
	/\bMOVE\s+GRAPH\b/i
];

function isWriteOperation(query) {
	return WRITE_PATTERNS.some((p) => p.test(query));
}

export async function POST({ request }) {
	let body;
	try {
		body = await request.json();
	} catch {
		return json({ error: 'Invalid JSON body.' }, { status: 400 });
	}

	const { query } = body;

	if (!query || typeof query !== 'string' || !query.trim()) {
		return json({ error: 'A non-empty SPARQL query is required.' }, { status: 400 });
	}

	if (isWriteOperation(query)) {
		return json(
			{ error: 'Write operations (INSERT, DELETE, CLEAR, DROP, LOAD, etc.) are not permitted.' },
			{ status: 403 }
		);
	}

	try {
		const res = await fetch(GRAPHDB_ENDPOINT, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/sparql-query',
				Accept: 'application/sparql-results+json'
			},
			body: query
		});

		if (!res.ok) {
			const text = await res.text();
			return json({ error: `GraphDB returned ${res.status}: ${text}` }, { status: res.status });
		}

		const data = await res.json();
		return json(data);
	} catch (err) {
		return json(
			{ error: 'Could not reach GraphDB. Check that GRAPHDB_ENDPOINT is configured.' },
			{ status: 502 }
		);
	}
}
