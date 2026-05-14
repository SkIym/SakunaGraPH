export async function POST({ request, fetch }) {
	const query = await request.text();

	const response = await fetch('http://localhost:7878/query', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/sparql-query',
			'Accept': 'application/sparql-results+json'
		},
		body: query
	});

	const data = await response.text();

	return new Response(data, {
		status: response.status,
		headers: {
			'Content-Type': 'application/sparql-results+json'
		}
	});
}