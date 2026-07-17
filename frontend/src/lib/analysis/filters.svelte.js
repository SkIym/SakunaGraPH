export const EVENT_TYPE_OPTIONS = Object.freeze([
	{ value: 'all', label: 'All' },
	{ value: 'major', label: 'Major' },
	{ value: 'incidents', label: 'Incidents' }
]);

const EVENT_TYPES = new Set(EVENT_TYPE_OPTIONS.map((option) => option.value));

function toggleValue(values, value) {
	const normalized = String(value ?? '').trim();
	if (!normalized) return values;
	return values.includes(normalized)
		? values.filter((item) => item !== normalized)
		: [...values, normalized];
}

class AnalysisFilterState {
	eventType = $state('all');
	startDate = $state('');
	endDate = $state('');
	locationIds = $state([]);
	disasterTypes = $state([]);
	q = $state('');

	get activeCount() {
		return (
			(this.eventType === 'all' ? 0 : 1) +
			(this.startDate ? 1 : 0) +
			(this.endDate ? 1 : 0) +
			this.locationIds.length +
			this.disasterTypes.length +
			(this.q.trim() ? 1 : 0)
		);
	}

	get hasActiveFilters() {
		return this.activeCount > 0;
	}

	setEventType(value) {
		if (EVENT_TYPES.has(value)) this.eventType = value;
	}

	setStartDate(value) {
		this.startDate = value;
		if (value && this.endDate && this.endDate < value) this.endDate = value;
	}

	setEndDate(value) {
		this.endDate = value;
		if (value && this.startDate && this.startDate > value) this.startDate = value;
	}

	setQuery(value) {
		this.q = value;
	}

	toggleLocation(id) {
		this.locationIds = toggleValue(this.locationIds, id);
	}

	removeLocation(id) {
		this.locationIds = this.locationIds.filter((value) => value !== id);
	}

	toggleDisasterType(id) {
		this.disasterTypes = toggleValue(this.disasterTypes, id);
	}

	removeDisasterType(id) {
		this.disasterTypes = this.disasterTypes.filter((value) => value !== id);
	}

	reset() {
		this.eventType = 'all';
		this.startDate = '';
		this.endDate = '';
		this.locationIds = [];
		this.disasterTypes = [];
		this.q = '';
	}
}

export const analysisFilters = new AnalysisFilterState();

function appendParam(params, key, value) {
	if (value === undefined || value === null || value === '') return;
	params.append(key, String(value));
}

export function toAnalysisParams(overrides = {}) {
	const params = new URLSearchParams();
	params.set('event_type', analysisFilters.eventType);
	appendParam(params, 'start_date', analysisFilters.startDate);
	appendParam(params, 'end_date', analysisFilters.endDate);
	analysisFilters.locationIds.forEach((id) => appendParam(params, 'location_ids', id));
	analysisFilters.disasterTypes.forEach((id) => appendParam(params, 'disaster_types', id));
	appendParam(params, 'q', analysisFilters.q.trim());

	for (const [key, value] of Object.entries(overrides)) {
		params.delete(key);
		if (Array.isArray(value)) {
			value.forEach((item) => appendParam(params, key, item));
		} else {
			appendParam(params, key, value);
		}
	}

	return params;
}
