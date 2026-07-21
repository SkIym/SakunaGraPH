import { describe, expect, it, vi } from 'vitest';
import {
	AskStreamProtocolError,
	AskStreamUpstreamError,
	consumeAskStream,
} from '../../src/lib/features/ask/stream.js';

const encoder = new TextEncoder();

function responseFromChunks(chunks) {
	return new Response(
		new ReadableStream({
			start(controller) {
				for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
				controller.close();
			},
		}),
		{ headers: { 'Content-Type': 'text/event-stream' } },
	);
}

describe('GraphRAG answer stream', () => {
	it('parses partial SSE chunks, additive events, tokens, and versioned citations', async () => {
		const response = responseFromChunks([
			'data: {"type":"meta","sparql":"SELECT * WHERE {}","rows":[{"event":"one"}],',
			'"retrieval":{"mode":"graphrag","indexVersion":"2026-07"}}\r\n\r\n',
			'data: {"type":"future.v2","value":true}\n\n',
			'data: {"type":"token","text":"One "}\n\ndata: {"type":"token","text":"event."}\n\n',
			'event: citation.v1\ndata: {"id":"source-1","label":"Situation Report","uri":"https://example.test/report","excerpt":"Verified source"}\n\n',
			'data: {"type":"done"}\n\n',
		]);
		const onToken = vi.fn();

		const result = await consumeAskStream(response, { onToken });

		expect(result).toEqual({
			sparql: 'SELECT * WHERE {}',
			answer: 'One event.',
			rows: [{ event: 'one' }],
			citations: [
				{
					id: 'source-1',
					label: 'Situation Report',
					uri: 'https://example.test/report',
					excerpt: 'Verified source',
				},
			],
			retrieval: { mode: 'graphrag', indexVersion: '2026-07' },
			requestId: null,
		});
		expect(onToken).toHaveBeenCalledTimes(2);
	});

	it('rejects malformed events', async () => {
		const response = responseFromChunks(['data: {not-json}\n\n']);
		await expect(consumeAskStream(response)).rejects.toBeInstanceOf(AskStreamProtocolError);
	});

	it('surfaces user-safe upstream error events', async () => {
		const response = responseFromChunks([
			'data: {"type":"error","status":503,"detail":"Retrieval unavailable","requestId":"req-1"}\n\n',
		]);
		await expect(consumeAskStream(response)).rejects.toMatchObject({
			name: AskStreamUpstreamError.name,
			status: 503,
			message: 'Retrieval unavailable',
			requestId: 'req-1',
		});
	});

	it('detects a disconnect after partial output', async () => {
		const response = responseFromChunks([
			'data: {"type":"meta","sparql":"","rows":[]}\n\n',
			'data: {"type":"token","text":"Partial"}\n\n',
		]);
		await expect(consumeAskStream(response)).rejects.toThrow(
			'The answer stream disconnected before completion.',
		);
	});

	it('normalizes an abrupt response-body failure', async () => {
		const response = new Response(
			new ReadableStream({
				start(controller) {
					controller.error(new Error('socket closed'));
				},
			}),
		);

		await expect(consumeAskStream(response)).rejects.toMatchObject({
			name: 'AskStreamProtocolError',
			message: 'The response stream disconnected unexpectedly.',
		});
	});

	it('accepts an empty retrieval that completes normally', async () => {
		const response = responseFromChunks([
			'data: {"type":"meta","sparql":"SELECT * WHERE {}","rows":[]}\n\n',
			'data: {"type":"done","citations":[],"retrieval":{"mode":"graphrag","sourceCount":0}}\n\n',
		]);
		await expect(consumeAskStream(response)).resolves.toMatchObject({
			answer: '',
			rows: [],
			citations: [],
			retrieval: { mode: 'graphrag', sourceCount: 0 },
		});
	});

	it('cancels the stream reader when the caller aborts', async () => {
		let streamController;
		const cancel = vi.fn();
		const response = new Response(
			new ReadableStream({
				start(controller) {
					streamController = controller;
				},
				cancel,
			}),
		);
		const controller = new AbortController();
		const consuming = consumeAskStream(response, { signal: controller.signal });
		streamController.enqueue(encoder.encode('data: {"type":"meta","sparql":"","rows":[]}\n\n'));
		controller.abort();

		await expect(consuming).rejects.toMatchObject({ name: 'AbortError' });
		expect(cancel).toHaveBeenCalledOnce();
	});
});
