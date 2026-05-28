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

/**
 * Execute a read-only SPARQL SELECT query against GraphDB.
 * Returns the parsed SPARQL JSON response ({ results: { bindings: [...] } }).
 * Throws an Error on write attempts, non-OK responses, or network failures.
 *
 * @param {string} query - A SPARQL SELECT or ASK query string
 * @returns {Promise<object>} Parsed SPARQL results JSON
 */
export async function executeSparql(query) {
	if (!query || typeof query !== 'string' || !query.trim()) {
		throw new Error('A non-empty SPARQL query is required.');
	}

	if (isWriteOperation(query)) {
		throw new Error('Write operations (INSERT, DELETE, CLEAR, DROP, LOAD, etc.) are not permitted.');
	}

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
		throw new Error(`GraphDB returned ${res.status}: ${text}`);
	}

	return res.json();
}