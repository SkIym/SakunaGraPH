import { env } from '$env/dynamic/public';

const API_BASE_URL = (env.PUBLIC_API_BASE_URL ?? '').replace(/\/$/, '');

export function apiUrl(path) {
	const normalized = path.startsWith('/') ? path : `/${path}`;
	return `${API_BASE_URL}${normalized}`;
}

export function withQuery(path, params) {
	const query = params instanceof URLSearchParams ? params : new URLSearchParams(params);
	const suffix = query.toString();
	return suffix ? `${path}?${suffix}` : path;
}

export async function apiJson(path, options) {
	const response = await fetch(apiUrl(path), options);
	const data = await response.json().catch(() => ({}));
	if (!response.ok) {
		const detail = typeof data.detail === 'string' ? data.detail : null;
		throw new Error(detail ?? data.error ?? 'Request failed.');
	}
	return data;
}
