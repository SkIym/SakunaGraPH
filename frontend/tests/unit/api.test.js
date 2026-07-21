import { describe, expect, it, vi } from 'vitest';
import {
	ApiHttpError,
	ApiNetworkError,
	ApiResponseError,
	ApiTimeoutError,
	GraphDbError,
	apiUrl,
	createApiClient,
	withQuery,
} from '../../src/lib/api.js';

describe('API URL helpers', () => {
	it('keeps same-origin API paths by default', () => {
		expect(apiUrl('/api/ask')).toBe('/api/ask');
		expect(apiUrl('api/sparql')).toBe('/api/sparql');
	});

	it('serializes scalar and repeated query parameters', () => {
		expect(withQuery('/api/map/events', { scope: 'region', id: '130000000' })).toBe(
			'/api/map/events?scope=region&id=130000000',
		);
		expect(withQuery('/api/analysis/summary', {})).toBe('/api/analysis/summary');
		expect(withQuery('/api/analysis/events', { location_ids: ['01', '02'] })).toBe(
			'/api/analysis/events?location_ids=01&location_ids=02',
		);
	});

	it('normalizes a configured separate API origin', async () => {
		const fetchImpl = vi.fn(
			async () =>
				new Response(JSON.stringify({ answer: 'ok' }), {
					status: 200,
					headers: { 'content-type': 'application/json' },
				}),
		);
		const client = createApiClient({ baseUrl: 'https://api.example.test/', fetchImpl });

		await client.json('/api/ask');

		expect(client.url('api/sparql')).toBe('https://api.example.test/api/sparql');
		expect(fetchImpl).toHaveBeenCalledWith(
			'https://api.example.test/api/ask',
			expect.objectContaining({ credentials: 'same-origin' }),
		);
	});
});

describe('API error normalization', () => {
	it('preserves HTTP detail, status, body, and request ID', async () => {
		const client = createApiClient({
			fetchImpl: async () =>
				new Response(JSON.stringify({ detail: 'Not available' }), {
					status: 404,
					headers: { 'x-request-id': 'request-123' },
				}),
		});

		const error = await client.json('/api/missing').catch((caught) => caught);

		expect(error).toBeInstanceOf(ApiHttpError);
		expect(error).toMatchObject({
			message: 'Not available',
			status: 404,
			requestId: 'request-123',
			kind: 'http',
		});
	});

	it('distinguishes GraphDB failures', async () => {
		const client = createApiClient({
			fetchImpl: async () =>
				new Response(JSON.stringify({ error: 'Could not reach GraphDB' }), { status: 502 }),
		});

		await expect(client.json('/api/sparql')).rejects.toBeInstanceOf(GraphDbError);
	});

	it('distinguishes malformed successful JSON', async () => {
		const client = createApiClient({
			fetchImpl: async () => new Response('{not-json', { status: 200 }),
		});

		await expect(client.json('/api/ontology/graph')).rejects.toBeInstanceOf(ApiResponseError);
	});

	it('distinguishes network failures', async () => {
		const client = createApiClient({
			fetchImpl: async () => {
				throw new TypeError('connection refused');
			},
		});

		await expect(client.json('/api/map/events')).rejects.toBeInstanceOf(ApiNetworkError);
	});

	it('distinguishes bounded timeouts from caller cancellation', async () => {
		const fetchImpl = vi.fn(
			(_url, { signal }) =>
				new Promise((_resolve, reject) => {
					signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
				}),
		);
		const client = createApiClient({ fetchImpl, defaultTimeoutMs: 5 });

		await expect(client.json('/api/slow')).rejects.toBeInstanceOf(ApiTimeoutError);

		const controller = new AbortController();
		controller.abort();
		const cancelled = await client
			.json('/api/cancelled', { signal: controller.signal })
			.catch((error) => error);
		expect(cancelled).toMatchObject({ name: 'AbortError', kind: 'cancelled' });
	});

	it('forwards cancellation while a request is in flight', async () => {
		let receivedSignal;
		const client = createApiClient({
			fetchImpl: (_url, { signal }) => {
				receivedSignal = signal;
				return new Promise((_resolve, reject) => {
					signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')));
				});
			},
		});
		const controller = new AbortController();
		const request = client.json('/api/analysis/events', { signal: controller.signal });

		controller.abort();

		await expect(request).rejects.toMatchObject({ name: 'AbortError', kind: 'cancelled' });
		expect(receivedSignal.aborted).toBe(true);
	});
});
