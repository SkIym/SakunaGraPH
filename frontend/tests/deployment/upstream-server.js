import { createServer } from 'node:http';

const HOST = '0.0.0.0';
const PORT = 9090;

function json(response, status, body) {
	response.writeHead(status, { 'content-type': 'application/json' });
	response.end(JSON.stringify(body));
}

async function requestText(request) {
	let body = '';
	for await (const chunk of request) body += chunk;
	return body;
}

async function requestBody(request) {
	const body = await requestText(request);
	return body ? JSON.parse(body) : {};
}

const server = createServer(async (request, response) => {
	const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);

	if (request.method === 'GET' && url.pathname === '/health') {
		json(response, 200, { status: 'ok' });
		return;
	}

	if (request.method === 'POST' && url.pathname === '/graphdb') {
		const body = await requestText(request);
		const query = request.headers['content-type']?.includes('application/sparql-query')
			? body
			: (new URLSearchParams(body).get('query') ?? '');
		if (query.includes('SELECT ?region ?label (COUNT(DISTINCT ?event) AS ?count)')) {
			json(response, 200, {
				head: { vars: ['region', 'label', 'count'] },
				results: {
					bindings: [
						{
							region: { type: 'uri', value: 'https://sakuna.ph/psgc/0100000000' },
							label: { type: 'literal', value: 'Ilocos Region' },
							count: { type: 'literal', value: '1' },
						},
					],
				},
			});
			return;
		}
		if (query.includes('SELECT (COUNT(DISTINCT ?event) AS ?count)')) {
			json(response, 200, {
				head: { vars: ['count'] },
				results: {
					bindings: [{ count: { type: 'literal', value: '1' } }],
				},
			});
			return;
		}
		if (query.includes('SELECT DISTINCT ?event ?eventName ?eventClass ?startDate ?endDate')) {
			json(response, 200, {
				head: { vars: ['event', 'eventName', 'eventClass', 'startDate', 'endDate'] },
				results: {
					bindings: [
						{
							event: { type: 'uri', value: 'https://sakuna.ph/ndrrmc-event/deployment-test' },
							eventName: { type: 'literal', value: 'Compose Deployment Test Event' },
							eventClass: { type: 'uri', value: 'https://sakuna.ph/MajorEvent' },
							startDate: { type: 'literal', value: '2026-01-01' },
							endDate: { type: 'literal', value: '2026-01-02' },
						},
					],
				},
			});
			return;
		}
		json(response, 200, {
			head: { vars: [] },
			results: { bindings: [] },
		});
		return;
	}

	if (
		request.method === 'POST' &&
		url.pathname.startsWith('/model/') &&
		url.pathname.endsWith('/converse')
	) {
		const body = await requestBody(request);
		const prompt = body.messages?.[0]?.content?.[0]?.text ?? '';
		const isGroundingRequest = prompt.includes('Structured answer context:');
		if (isGroundingRequest) await new Promise((resolve) => setTimeout(resolve, 500));
		json(response, 200, {
			output: {
				message: {
					role: 'assistant',
					content: [
						{
							text: isGroundingRequest
								? 'Compose stream reached the browser [E1].'
								: '{"intent":"region_ranking","metric":"events","group_by":"region","limit":25}',
						},
					],
				},
			},
			stopReason: 'end_turn',
			usage: { inputTokens: 5, outputTokens: 6, totalTokens: 11 },
			metrics: { latencyMs: 500 },
		});
		return;
	}

	json(response, 404, { detail: 'Not found' });
});

server.listen(PORT, HOST, () => {
	console.log(`Deployment upstream listening on http://${HOST}:${PORT}`);
});

function shutdown() {
	server.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
