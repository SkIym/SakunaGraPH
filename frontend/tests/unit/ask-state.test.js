import { describe, expect, it, vi } from 'vitest';
import { ASK_MODES } from '../../src/lib/api/ask.js';
import { createAskState } from '../../src/lib/features/ask/state.svelte.js';

const encoder = new TextEncoder();

function streamResponse(events) {
	return new Response(
		new ReadableStream({
			start(controller) {
				for (const event of events) {
					controller.enqueue(encoder.encode(`data: ${JSON.stringify(event)}\n\n`));
				}
				controller.close();
			},
		}),
	);
}

describe('ask state', () => {
	it('transitions from user/loading messages to the legacy response shape', async () => {
		const onUpdated = vi.fn();
		const submit = vi.fn().mockResolvedValue({
			answer: 'One event was found.',
			sparql: 'SELECT * WHERE {}',
			rows: [{ event: 'Event 1' }],
		});
		const ask = createAskState({ submit, onUpdated });
		ask.input = 'Show one event';

		await ask.send();

		expect(submit).toHaveBeenCalledWith('Show one event', {
			signal: expect.any(AbortSignal),
		});
		expect(ask.messages).toEqual([
			{ role: 'user', text: 'Show one event' },
			{
				role: 'assistant',
				loading: false,
				streaming: false,
				text: 'One event was found.',
				sparql: 'SELECT * WHERE {}',
				rows: [{ event: 'Event 1' }],
				citations: [],
				retrieval: null,
				requestId: null,
			},
		]);
		expect(ask.sending).toBe(false);
		expect(onUpdated).toHaveBeenCalledTimes(2);
	});

	it('normalizes a network error into the existing user message', async () => {
		const submit = vi.fn().mockRejectedValue({ name: 'ApiNetworkError', kind: 'network' });
		const ask = createAskState({ submit });

		await ask.send('Question');

		expect(ask.messages.at(-1)).toEqual({
			role: 'assistant',
			loading: false,
			streaming: false,
			error: 'Could not reach server.',
		});
	});

	it('updates one assistant message with streamed prose, citations, and provenance', async () => {
		const submit = vi.fn();
		const openStream = vi.fn().mockResolvedValue(
			streamResponse([
				{
					type: 'meta',
					sparql: 'SELECT * WHERE {}',
					rows: [{ event: 'Event 1' }],
					retrieval: { mode: 'graphrag', indexVersion: 'v1' },
				},
				{ type: 'token', text: 'One ' },
				{ type: 'token', text: 'event.' },
				{
					type: 'citation',
					citation: {
						id: 'source-1',
						label: 'Report',
						uri: 'https://example.test/report',
					},
				},
				{ type: 'done' },
			]),
		);
		const ask = createAskState({ mode: ASK_MODES.STREAM, submit, openStream });

		await ask.send('Stream this answer');

		expect(submit).not.toHaveBeenCalled();
		expect(ask.messages.at(-1)).toMatchObject({
			role: 'assistant',
			loading: false,
			streaming: false,
			text: 'One event.',
			sparql: 'SELECT * WHERE {}',
			rows: [{ event: 'Event 1' }],
			citations: [{ id: 'source-1', label: 'Report', uri: 'https://example.test/report' }],
			retrieval: { mode: 'graphrag', indexVersion: 'v1' },
		});
		expect(ask.announcement).toBe('Answer complete.');
	});

	it('falls back to the legacy endpoint when streaming fails before metadata', async () => {
		const submit = vi.fn().mockResolvedValue({
			answer: 'Legacy answer',
			sparql: 'SELECT * WHERE {}',
			rows: [],
		});
		const openStream = vi.fn().mockRejectedValue(new Error('Endpoint unavailable'));
		const ask = createAskState({ mode: ASK_MODES.STREAM, submit, openStream });

		await ask.send('Use rollout fallback');

		expect(submit).toHaveBeenCalledWith('Use rollout fallback', {
			signal: expect.any(AbortSignal),
		});
		expect(ask.messages.at(-1)).toMatchObject({
			text: 'Legacy answer',
			retrieval: { mode: 'fallback' },
		});
	});

	it('does not duplicate work after a stream has started', async () => {
		const submit = vi.fn();
		const openStream = vi.fn().mockResolvedValue(
			streamResponse([
				{ type: 'meta', sparql: '', rows: [] },
				{ type: 'error', status: 503, detail: 'Retrieval unavailable' },
			]),
		);
		const ask = createAskState({ mode: ASK_MODES.STREAM, submit, openStream });

		await ask.send('Fail after metadata');

		expect(submit).not.toHaveBeenCalled();
		expect(ask.messages.at(-1)).toMatchObject({ error: 'Retrieval unavailable' });
	});

	it('cancels active work without announcing every partial token', async () => {
		const openStream = vi.fn(
			(_question, { signal }) =>
				new Promise((_resolve, reject) => {
					signal.addEventListener('abort', () => {
						const error = new Error('cancelled');
						error.name = 'AbortError';
						reject(error);
					});
				}),
		);
		const ask = createAskState({ mode: ASK_MODES.STREAM, openStream });
		const sending = ask.send('Cancel this');
		await vi.waitFor(() => expect(openStream).toHaveBeenCalledOnce());

		ask.cancel();
		await sending;

		expect(ask.messages.at(-1)).toMatchObject({
			role: 'assistant',
			loading: false,
			streaming: false,
			cancelled: true,
		});
		expect(ask.sending).toBe(false);
		expect(ask.announcement).toBe('Request cancelled.');
	});

	it('aborts an active request when a replacement question is sent', async () => {
		let call = 0;
		const submit = vi.fn((_question, { signal }) => {
			call += 1;
			if (call === 2) {
				return Promise.resolve({ answer: 'Second answer', sparql: '', rows: [] });
			}
			return new Promise((_resolve, reject) => {
				signal.addEventListener('abort', () => {
					const error = new Error('cancelled');
					error.name = 'AbortError';
					reject(error);
				});
			});
		});
		const ask = createAskState({ submit });
		const first = ask.send('First question');
		await vi.waitFor(() => expect(submit).toHaveBeenCalledOnce());

		const second = ask.send('Second question');
		await Promise.all([first, second]);

		expect(ask.messages[1]).toMatchObject({ cancelled: true });
		expect(ask.messages.at(-1)).toMatchObject({ text: 'Second answer' });
	});
});
