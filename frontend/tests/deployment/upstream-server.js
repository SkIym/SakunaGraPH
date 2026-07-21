import { createServer } from 'node:http';

const HOST = '0.0.0.0';
const PORT = 9090;

function json(response, status, body) {
	response.writeHead(status, { 'content-type': 'application/json' });
	response.end(JSON.stringify(body));
}

async function requestBody(request) {
	let body = '';
	for await (const chunk of request) body += chunk;
	return body ? JSON.parse(body) : {};
}

const server = createServer(async (request, response) => {
	const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);

	if (request.method === 'GET' && url.pathname === '/health') {
		json(response, 200, { status: 'ok' });
		return;
	}

	if (request.method === 'POST' && url.pathname === '/graphdb') {
		json(response, 200, {
			head: { vars: ['eventName'] },
			results: {
				bindings: [{ eventName: { type: 'literal', value: 'Compose Deployment Test Event' } }],
			},
		});
		return;
	}

	if (request.method === 'POST' && url.pathname === '/api/v1/chat') {
		const body = await requestBody(request);
		if (!body.stream) {
			json(response, 200, {
				output: [
					{
						type: 'message',
						content:
							'```sparql\nSELECT ?eventName WHERE { ?event <https://sakuna.ph/eventName> ?eventName } LIMIT 1\n```',
					},
				],
			});
			return;
		}

		response.writeHead(200, {
			'content-type': 'application/x-ndjson',
			'cache-control': 'no-store',
		});
		setTimeout(() => {
			response.write(
				`${JSON.stringify({ output: [{ type: 'message', content: 'Compose stream reached the browser.' }] })}\n`,
			);
			setTimeout(() => response.end(`${JSON.stringify({ done: true })}\n`), 100);
		}, 500);
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
