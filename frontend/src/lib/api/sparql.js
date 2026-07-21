import { apiJson } from './client.js';

export function runSparql(query, options = {}) {
	return apiJson('/api/sparql', {
		method: 'POST',
		json: { query },
		timeoutMs: 60_000,
		...options,
	});
}
