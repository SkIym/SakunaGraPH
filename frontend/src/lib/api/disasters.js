import { apiJson, withQuery } from './client.js';

export function getDisasterDetails(uri, options) {
	return apiJson(withQuery('/api/disasters/details', { uri }), options);
}
