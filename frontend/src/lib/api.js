import { env } from '$env/dynamic/public';

const API_BASE_URL = (env.PUBLIC_API_BASE_URL ?? '').replace(/\/$/, '');

export function apiUrl(path) {
	const normalized = path.startsWith('/') ? path : `/${path}`;
	return `${API_BASE_URL}${normalized}`;
}
