import { apiJson, withQuery } from './client.js';

export function getMapEvents(params, options) {
	return apiJson(withQuery('/api/map/events', params), options);
}
