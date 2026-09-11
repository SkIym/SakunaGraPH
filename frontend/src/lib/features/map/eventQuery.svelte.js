import { getMapEvents } from '../../api/map.js';
import { groupEventsByAlternates } from './groupEvents.js';

export const MAP_PAGE_SIZE = 10;

function safeCount(value) {
	const count = Number(value);
	return Number.isFinite(count) && count >= 0 ? Math.floor(count) : 0;
}

export function createMapEventQuery({ fetchEvents = getMapEvents } = {}) {
	let results = $state(null);
	let majorCount = $state(0);
	let incidentCount = $state(0);
	let loading = $state(false);
	let error = $state('');
	let groupedResults = $state(null);
	let requestVersion = 0;

	async function load({ selected, mode, page, signal }) {
		if (!selected) return;
		const request = ++requestVersion;
		loading = true;
		error = '';
		const scope = ['region', 'province', 'city', 'municipality'].includes(selected.type)
			? selected.type
			: 'province';
		const id = selected.type === 'region' ? selected.psgc : selected.id;

		try {
			const data = await fetchEvents({ scope, id, mode, page: String(page) }, { signal });
			if (signal.aborted || request !== requestVersion) return;
			results = Array.isArray(data?.events) ? data.events : [];
			majorCount = safeCount(data?.majorCount);
			incidentCount = safeCount(data?.incidentCount);
			groupedResults = groupEventsByAlternates(results);
		} catch (requestError) {
			if (request !== requestVersion) return;
			if (requestError.name === 'AbortError') return;
			if (requestError.kind === 'network') {
				error = 'Could not reach the data service. Check your connection, then use Try again.';
			} else if (requestError.kind === 'timeout') {
				error = 'The records took too long to load. Use Try again to retry this area.';
			} else if (requestError.status === 429) {
				error =
					'The data service is receiving too many requests. Wait a moment, then use Try again.';
			} else if (requestError.status >= 500) {
				error = 'The map records are temporarily unavailable. Use Try again shortly.';
			} else {
				error = 'The records could not be loaded. Use Try again to retry this area.';
			}
		} finally {
			if (!signal.aborted && request === requestVersion) loading = false;
		}
	}

	function reset() {
		requestVersion += 1;
		loading = false;
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
			requestVersion += 1;
			loading = false;
			results = null;
			groupedResults = null;
		},
		reset,
		load,
	};
}
