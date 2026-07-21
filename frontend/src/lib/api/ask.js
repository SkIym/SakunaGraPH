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
	return apiJson('/api/ask', {
		method: 'POST',
		json: { query },
		timeoutMs: 120_000,
		...options,
	});
}

export function previewAsk(query, options = {}) {
	return apiJson('/api/ask/preview', {
		method: 'POST',
		json: { query },
		timeoutMs: 60_000,
		...options,
	});
}

// Event parsing stays in the ask feature so this API module remains transport-only.
export function openAskStream(query, options = {}) {
	return apiResponse('/api/ask/stream', {
		method: 'POST',
		json: { query },
		timeoutMs: 120_000,
		...options,
		headers: { Accept: 'text/event-stream', ...options.headers },
	});
}
