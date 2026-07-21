import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

const repositoryRoot = resolve(import.meta.dirname, '../..');
const projectName = 'sakunagraph-deployment-test';
const port = '4176';
const baseUrl = `http://127.0.0.1:${port}`;
const composeFiles = [
	resolve(repositoryRoot, 'docker-compose.yml'),
	resolve(repositoryRoot, 'frontend/tests/deployment/docker-compose.test.yml'),
];
const composeArguments = [
	'compose',
	'--ansi',
	'never',
	'--project-name',
	projectName,
	...composeFiles.flatMap((file) => ['--file', file]),
];
const environment = {
	...process.env,
	SAKUNAGRAPH_PORT: port,
	ORIGIN: baseUrl,
	PUBLIC_ASK_STREAMING_ENABLED: 'true',
};

function docker(args, { allowFailure = false } = {}) {
	const result = spawnSync('docker', [...composeArguments, ...args], {
		cwd: repositoryRoot,
		env: environment,
		stdio: 'inherit',
	});
	if (result.error) {
		if (allowFailure) {
			console.error(result.error);
			return 1;
		}
		throw result.error;
	}
	if (!allowFailure && result.status !== 0) {
		throw new Error(`docker ${[...composeArguments, ...args].join(' ')} exited ${result.status}.`);
	}
	return result.status ?? 1;
}

function assert(condition, message) {
	if (!condition) throw new Error(message);
}

async function get(path, init = {}) {
	return fetch(`${baseUrl}${path}`, {
		signal: AbortSignal.timeout(10_000),
		...init,
	});
}

async function checkRoutes() {
	const routes = [
		'/',
		'/map',
		'/ontology',
		'/ask',
		'/analysis',
		'/analysis/events',
		'/analysis/metrics',
		'/analysis/timeline',
	];
	for (const route of routes) {
		const response = await get(route);
		const body = await response.text();
		assert(response.ok, `${route} returned ${response.status}.`);
		assert(response.headers.get('content-type')?.includes('text/html'), `${route} was not HTML.`);
		assert(body.includes('SakunaGraPH'), `${route} did not render the application shell.`);
		console.log(`✓ direct route ${route}`);
	}
}

function checkRuntimeBoundaries() {
	docker([
		'exec',
		'-T',
		'frontend',
		'node',
		'-e',
		"const fs=require('node:fs'); if(process.getuid?.()===0 || fs.existsSync('/app/src') || fs.existsSync('/app/tests') || !fs.existsSync('/app/build/index.js')) process.exit(1)",
	]);
	docker([
		'exec',
		'-T',
		'api',
		'python',
		'-c',
		"import os; from pathlib import Path; raise SystemExit(os.geteuid() == 0 or Path('/app/tests').exists() or not Path('/app/src/main.py').exists())",
	]);
	console.log('✓ minimal non-root runtime images');
}

async function checkApiProxy() {
	const response = await get('/api/health');
	assert(response.ok, `/api/health returned ${response.status}.`);
	const body = await response.json();
	assert(body.status === 'ok', '/api/health did not reach FastAPI.');
	console.log('✓ same-origin /api proxy');
}

async function checkGeoJson() {
	const response = await get('/data/regions.geojson?v=deployment-test');
	assert(response.ok, `GeoJSON returned ${response.status}.`);
	const body = await response.json();
	assert(body.type === 'FeatureCollection', 'GeoJSON was not a FeatureCollection.');
	assert(
		Array.isArray(body.features) && body.features.length > 0,
		'GeoJSON contained no features.',
	);
	console.log(`✓ static GeoJSON (${body.features.length} features)`);
}

async function readWithTimeout(reader, timeoutMs) {
	let timeout;
	try {
		return await Promise.race([
			reader.read(),
			new Promise((_, reject) => {
				timeout = setTimeout(
					() => reject(new Error('Timed out waiting for an SSE chunk.')),
					timeoutMs,
				);
			}),
		]);
	} finally {
		clearTimeout(timeout);
	}
}

async function checkStream() {
	const response = await get('/api/ask/stream', {
		method: 'POST',
		headers: { accept: 'text/event-stream', 'content-type': 'application/json' },
		body: JSON.stringify({ query: 'Return the deployment test event.' }),
		signal: AbortSignal.timeout(15_000),
	});
	assert(response.ok, `/api/ask/stream returned ${response.status}.`);
	assert(
		response.headers.get('content-type')?.includes('text/event-stream'),
		'/api/ask/stream did not preserve the SSE content type.',
	);
	assert(response.body, '/api/ask/stream returned no response body.');

	const reader = response.body.getReader();
	const decoder = new TextDecoder();
	const first = await readWithTimeout(reader, 2_000);
	const firstText = decoder.decode(first.value, { stream: true });
	assert(firstText.includes('"type": "meta"'), 'The first SSE chunk did not contain metadata.');
	assert(!firstText.includes('"type": "done"'), 'The proxy buffered the complete stream.');

	let remaining = '';
	while (true) {
		const chunk = await readWithTimeout(reader, 5_000);
		if (chunk.done) break;
		remaining += decoder.decode(chunk.value, { stream: true });
	}
	remaining += decoder.decode();
	assert(
		remaining.includes('Compose stream reached the browser.'),
		'The SSE token was not forwarded.',
	);
	assert(remaining.includes('"type": "done"'), 'The SSE completion event was not forwarded.');
	console.log('✓ unbuffered GraphRAG SSE stream');
}

let failed = false;
try {
	docker(['up', '--build', '--wait', '--wait-timeout', '240']);
	checkRuntimeBoundaries();
	await checkRoutes();
	await checkApiProxy();
	await checkGeoJson();
	await checkStream();
	console.log('Deployment contract passed.');
} catch (error) {
	failed = true;
	console.error(error);
	docker(['logs', '--no-color'], { allowFailure: true });
} finally {
	docker(['down', '--volumes', '--remove-orphans'], { allowFailure: true });
}

if (failed) process.exit(1);
