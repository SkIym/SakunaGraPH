import { env } from '$env/dynamic/public';
import { apiJson, apiResponse } from './client.js';

export const ASK_MODES = Object.freeze({ LEGACY: 'legacy', STREAM: 'stream' });

export function enabledFeatureFlag(value) {
	return /^(1|true|yes|on)$/i.test(String(value ?? '').trim());
}

export function preferredAskMode() {
	return enabledFeatureFlag(env.PUBLIC_ASK_STREAMING_ENABLED) ? ASK_MODES.STREAM : ASK_MODES.LEGACY;
}

// This shape-compatible operation remains the default and the streaming rollout fallback.
export function askQuestion(query, options = {}) {
	const { queryMode = 'auto', ...requestOptions } = options;
	return apiJson('/api/ask', {
		method: 'POST',
		json: { query, query_mode: queryMode },
		timeoutMs: 120_000,
		...requestOptions,
	});
}

export function previewAsk(query, options = {}) {
	const { queryMode = 'auto', ...requestOptions } = options;
	return apiJson('/api/ask/preview', {
		method: 'POST',
		json: { query, query_mode: queryMode },
		timeoutMs: 60_000,
		...requestOptions,
	});
}

// Event parsing stays in the ask feature so this API module remains transport-only.
export function openAskStream(query, options = {}) {
	const { queryMode = 'auto', ...requestOptions } = options;
	return apiResponse('/api/ask/stream', {
		method: 'POST',
		json: { query, query_mode: queryMode },
		timeoutMs: 120_000,
		...requestOptions,
		headers: { Accept: 'text/event-stream', ...requestOptions.headers },
	});
}
