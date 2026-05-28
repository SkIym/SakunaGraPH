import { json } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

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

	try {
		const data = await executeSparql(query);
		return json(data);
	} catch (err) {
		const msg = err.message ?? '';
		if (msg.includes('Write operations')) {
			return json({ error: msg }, { status: 403 });
		}
		if (msg.startsWith('GraphDB returned')) {
			// Extract the original status code GraphDB sent back
			const match = msg.match(/GraphDB returned (\d+)/);
			const status = match ? parseInt(match[1]) : 502;
			return json({ error: msg }, { status });
		}
		return json(
			{ error: 'Could not reach GraphDB. Check that GRAPHDB_ENDPOINT is configured.' },
			{ status: 502 }
		);
	}
}