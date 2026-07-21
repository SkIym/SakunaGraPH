import { getMapEvents } from '../../api/map.js';
import { groupEventsByAlternates } from './groupEvents.js';

export const MAP_PAGE_SIZE = 10;

export function createMapEventQuery({ fetchEvents = getMapEvents } = {}) {
	let results = $state(null);
	let majorCount = $state(0);
	let incidentCount = $state(0);
	let loading = $state(false);
	let error = $state('');
	let groupedResults = $state(null);

	async function load({ selected, mode, page, signal }) {
		if (!selected) return;
		loading = true;
		error = '';
		const scope = selected.type === 'region' ? 'region' : 'province';
		const id = selected.type === 'region' ? selected.psgc : selected.id;

		try {
			const data = await fetchEvents({ scope, id, mode, page: String(page) }, { signal });
			results = data.events ?? [];
			majorCount = data.majorCount;
			incidentCount = data.incidentCount;
			groupedResults = groupEventsByAlternates(results);
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			error =
				requestError.kind === 'network'
					? 'Could not reach server.'
					: requestError.message || 'Query failed.';
		} finally {
			if (!signal.aborted) loading = false;
		}
	}

	function reset() {
		results = null;
		majorCount = 0;
		incidentCount = 0;
		groupedResults = null;
		error = '';
	}

	return {
		get results() {
			return results;
		},
		get majorCount() {
			return majorCount;
		},
		get incidentCount() {
			return incidentCount;
		},
		get loading() {
			return loading;
		},
		get error() {
			return error;
		},
		get groupedResults() {
			return groupedResults;
		},
		countFor(mode) {
			return mode === 'major' ? majorCount : incidentCount;
		},
		clearResults() {
			results = null;
			groupedResults = null;
		},
		reset,
		load,
	};
}
