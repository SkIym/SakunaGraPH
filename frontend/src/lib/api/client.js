import { env } from '$env/dynamic/public';
import {
	ApiCancellationError,
	ApiHttpError,
	ApiNetworkError,
	ApiResponseError,
	ApiTimeoutError,
	GraphDbError,
	isCancellationError,
} from './errors.js';

export const DEFAULT_API_TIMEOUT_MS = 30_000;

export function normalizeApiBaseUrl(value = '') {
	return String(value).trim().replace(/\/+$/, '');
}

export function buildApiUrl(path, baseUrl = '') {
	const normalizedPath = path.startsWith('/') ? path : `/${path}`;
	return `${normalizeApiBaseUrl(baseUrl)}${normalizedPath}`;
}

function appendParam(searchParams, key, value) {
	if (value === undefined || value === null || value === '') return;
	if (Array.isArray(value)) {
		for (const item of value) appendParam(searchParams, key, item);
		return;
	}
	searchParams.append(key, String(value));
}

export function withQuery(path, params = {}) {
	let query;
	if (params instanceof URLSearchParams) {
		query = params.toString();
	} else if (typeof params === 'string') {
		query = params.replace(/^\?/, '');
	} else {
		const searchParams = new URLSearchParams();
		for (const [key, value] of Object.entries(params)) appendParam(searchParams, key, value);
		query = searchParams.toString();
	}
	if (!query) return path;
	return `${path}${path.includes('?') ? '&' : '?'}${query}`;
}

function requestIdFrom(response, body) {
	return (
		response.headers.get('x-request-id') ??
		response.headers.get('x-correlation-id') ??
		body?.requestId ??
		body?.request_id ??
		null
	);
}

function validationMessage(detail) {
	if (!Array.isArray(detail)) return null;
	const messages = detail
		.map((item) => {
			if (typeof item === 'string') return item;
			if (!item || typeof item !== 'object') return null;
			const location = Array.isArray(item.loc) ? item.loc.join('.') : '';
			return [location, item.msg].filter(Boolean).join(': ');
		})
		.filter(Boolean);
	return messages.length ? messages.join('; ') : null;
}

function errorMessage(body, response, fallbackText) {
	if (typeof body?.detail === 'string') return body.detail;
	const validation = validationMessage(body?.detail);
	if (validation) return validation;
	if (typeof body?.error === 'string') return body.error;
	if (typeof body?.message === 'string') return body.message;
	if (typeof body?.detail?.message === 'string') return body.detail.message;
	if (fallbackText && !fallbackText.trimStart().startsWith('<')) return fallbackText;
	return response.statusText || `Request failed with status ${response.status}.`;
}

async function readText(response) {
	return response.text();
}

function parseJson(text, response, url) {
	if (!text) return null;
	try {
		return JSON.parse(text);
	} catch (cause) {
		throw new ApiResponseError('The server returned invalid JSON.', {
			status: response.status,
			url,
			requestId: requestIdFrom(response),
			cause,
		});
	}
}

function httpError(response, body, text, url) {
	const message = errorMessage(body, response, text);
	const ErrorType = /\bgraphdb\b/i.test(message) ? GraphDbError : ApiHttpError;
	return new ErrorType(message, {
		status: response.status,
		url,
		requestId: requestIdFrom(response, body),
		body,
	});
}

function cancellationError(url, cause) {
	return new ApiCancellationError('The request was cancelled.', { url, cause });
}

export function createApiClient({
	baseUrl = env.PUBLIC_API_BASE_URL ?? '',
	fetchImpl = globalThis.fetch,
	defaultTimeoutMs = DEFAULT_API_TIMEOUT_MS,
	defaultHeaders = {},
} = {}) {
	const normalizedBaseUrl = normalizeApiBaseUrl(baseUrl);

	function url(path) {
		return buildApiUrl(path, normalizedBaseUrl);
	}

	async function requestWithMeta(path, options = {}) {
		const {
			timeoutMs = defaultTimeoutMs,
			signal: callerSignal,
			responseType = 'json',
			json,
			headers: requestHeaders,
			...fetchOptions
		} = options;
		const requestUrl = url(path);
		let keepAbortForwarding = false;

		if (callerSignal?.aborted) throw cancellationError(requestUrl, callerSignal.reason);
		if (typeof fetchImpl !== 'function') {
			throw new ApiNetworkError('The browser fetch API is unavailable.', { url: requestUrl });
		}

		const controller = new AbortController();
		let timedOut = false;
		const forwardAbort = () => controller.abort(callerSignal?.reason);
		callerSignal?.addEventListener('abort', forwardAbort, { once: true });
		const timer =
			Number.isFinite(timeoutMs) && timeoutMs > 0
				? globalThis.setTimeout(() => {
						timedOut = true;
						controller.abort();
					}, timeoutMs)
				: null;

		const headers = new Headers(defaultHeaders);
		for (const [key, value] of new Headers(requestHeaders)) headers.set(key, value);
		if (!headers.has('Accept')) {
			headers.set(
				'Accept',
				responseType === 'json'
					? 'application/json'
					: responseType === 'blob'
						? '*/*'
						: 'text/plain',
			);
		}
		if (json !== undefined) headers.set('Content-Type', 'application/json');

		try {
			const response = await fetchImpl(requestUrl, {
				credentials: 'same-origin',
				...fetchOptions,
				headers,
				body: json === undefined ? fetchOptions.body : JSON.stringify(json),
				signal: controller.signal,
			});

			if (!response.ok) {
				const text = await readText(response);
				let body = null;
				if (text) {
					try {
						body = JSON.parse(text);
					} catch {
						body = null;
					}
				}
				throw httpError(response, body, text, requestUrl);
			}

			let data;
			switch (responseType) {
				case 'response':
					data = response;
					// Keep forwarding caller cancellation so a future stream reader can stop the body after
					// the response headers have arrived. The one-shot listener removes itself on cancellation.
					keepAbortForwarding = Boolean(callerSignal);
					break;
				case 'blob':
					data = await response.blob();
					break;
				case 'text':
					data = await response.text();
					break;
				case 'json': {
					const text = await readText(response);
					data = parseJson(text, response, requestUrl);
					break;
				}
				default:
					throw new TypeError(`Unsupported API response type: ${responseType}`);
			}
			return { data, response };
		} catch (error) {
			if (error instanceof ApiHttpError || error instanceof ApiResponseError) throw error;
			if (timedOut) {
				throw new ApiTimeoutError(`The request timed out after ${timeoutMs} ms.`, {
					url: requestUrl,
					cause: error,
				});
			}
			if (callerSignal?.aborted || isCancellationError(error)) {
				throw cancellationError(requestUrl, error);
			}
			throw new ApiNetworkError('Could not reach the server.', { url: requestUrl, cause: error });
		} finally {
			if (timer !== null) globalThis.clearTimeout(timer);
			if (!keepAbortForwarding) callerSignal?.removeEventListener('abort', forwardAbort);
		}
	}

	async function request(path, options) {
		return (await requestWithMeta(path, options)).data;
	}

	return Object.freeze({
		baseUrl: normalizedBaseUrl,
		url,
		request,
		requestWithMeta,
		json: (path, options) => request(path, { ...options, responseType: 'json' }),
		text: (path, options) => request(path, { ...options, responseType: 'text' }),
		blob: (path, options) => request(path, { ...options, responseType: 'blob' }),
		response: (path, options) => request(path, { ...options, responseType: 'response' }),
	});
}

export const apiClient = createApiClient();
export const apiUrl = (path) => apiClient.url(path);
export const apiJson = (path, options) => apiClient.json(path, options);
export const apiText = (path, options) => apiClient.text(path, options);
export const apiBlob = (path, options) => apiClient.blob(path, options);
export const apiResponse = (path, options) => apiClient.response(path, options);
export const apiRequestWithMeta = (path, options) => apiClient.requestWithMeta(path, options);
