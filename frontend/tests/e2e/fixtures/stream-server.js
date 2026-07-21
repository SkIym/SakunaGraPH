import { createServer } from 'node:http';

const host = '127.0.0.1';
const port = 4180;
let activeStreams = 0;
let cancelledStreams = 0;

function json(response, status, body) {
	response.writeHead(status, {
		'Content-Type': 'application/json',
		'Cache-Control': 'no-store',
	});
	response.end(JSON.stringify(body));
}

async function requestJson(request) {
	let body = '';
	for await (const chunk of request) body += chunk;
	return body ? JSON.parse(body) : {};
}

function wait(milliseconds) {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function event(response, payload) {
	response.write(`data: ${JSON.stringify(payload)}\n\n`);
}

async function streamAnswer(response, query) {
	activeStreams += 1;
	let terminal = false;
	response.on('close', () => {
		activeStreams -= 1;
		if (!terminal) cancelledStreams += 1;
	});
	response.writeHead(200, {
		'Content-Type': 'text/event-stream; charset=utf-8',
		'Cache-Control': 'no-cache, no-transform',
		Connection: 'keep-alive',
		'X-Accel-Buffering': 'no',
	});
	event(response, {
		type: 'meta',
		sparql: 'SELECT ?event WHERE { ?event a <https://sakuna.ph/DisasterEvent> } LIMIT 1',
		rows: [{ event: 'https://sakuna.ph/test/event-1' }],
		retrieval: { mode: 'graphrag', indexVersion: 'fixture-v1', sourceCount: 1 },
		requestId: 'stream-request-1',
	});
	await wait(120);
	if (response.destroyed) return;
	event(response, { type: 'token', text: /cancel/i.test(query) ? 'Partial ' : 'One ' });

	if (/cancel/i.test(query)) {
		await wait(5_000);
		if (response.destroyed) return;
	}

	await wait(120);
	if (response.destroyed) return;
	event(response, {
		type: 'citation.v1',
		citation: {
			id: 'source-1',
			label: 'NDRRMC Situation Report',
			uri: 'https://example.test/reports/1',
			excerpt: 'A browser fixture source.',
		},
	});
	event(response, { type: 'token', text: 'streamed event.' });
	await wait(120);
	if (response.destroyed) return;
	event(response, {
		type: 'done',
		retrieval: { mode: 'graphrag', indexVersion: 'fixture-v1', sourceCount: 1 },
	});
	terminal = true;
	response.end();
}

const server = createServer(async (request, response) => {
	const url = new URL(request.url ?? '/', `http://${host}:${port}`);
	if (url.pathname === '/health') return json(response, 200, { status: 'ok' });
	if (url.pathname === '/api/test/stream-stats') {
		return json(response, 200, { activeStreams, cancelledStreams });
	}
	if (request.method !== 'POST') return json(response, 404, { detail: 'Not found' });

	const { query = '' } = await requestJson(request);
	if (url.pathname === '/api/ask/stream') {
		if (/fallback/i.test(query)) {
			return json(response, 404, { detail: 'Streaming is unavailable.' });
		}
		void streamAnswer(response, query);
		return;
	}
	if (url.pathname === '/api/ask') {
		return json(response, 200, {
			sparql: 'SELECT * WHERE {}',
			answer: 'Legacy rollout fallback.',
			rows: [],
		});
	}
	return json(response, 404, { detail: 'Not found' });
});

server.listen(port, host, () => {
	process.stdout.write(`Streaming fixture listening on http://${host}:${port}\n`);
});
