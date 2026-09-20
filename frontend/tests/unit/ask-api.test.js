import { describe, expect, it, vi } from 'vitest';

vi.mock('../../src/lib/api/client.js', () => ({
	apiJson: vi.fn(),
	apiResponse: vi.fn(),
}));

import { apiJson, apiResponse } from '../../src/lib/api/client.js';
import {
	ASK_MODES,
	askQuestion,
	enabledFeatureFlag,
	openAskStream,
	preferredAskMode,
} from '../../src/lib/api/ask.js';

describe('ask transport feature flag', () => {
	it('keeps legacy mode as the default', () => {
		expect(preferredAskMode()).toBe(ASK_MODES.LEGACY);
	});

	it('accepts explicit public boolean flag values', () => {
		for (const value of ['1', 'true', 'YES', 'on']) expect(enabledFeatureFlag(value)).toBe(true);
		for (const value of [undefined, '', '0', 'false', 'off']) {
			expect(enabledFeatureFlag(value)).toBe(false);
		}
	});

	it('serializes the selected query mode without leaking it into fetch options', () => {
		const signal = new AbortController().signal;

		askQuestion('Count events', { queryMode: 'llm', signal });
		openAskStream('Count events', { queryMode: 'llm', signal });

		expect(apiJson).toHaveBeenCalledWith('/api/ask', {
			method: 'POST',
			json: { query: 'Count events', query_mode: 'llm' },
			timeoutMs: 120_000,
			signal,
		});
		expect(apiResponse).toHaveBeenCalledWith('/api/ask/stream', {
			method: 'POST',
			json: { query: 'Count events', query_mode: 'llm' },
			timeoutMs: 120_000,
			signal,
			headers: { Accept: 'text/event-stream' },
		});
	});
});
