import { apiRequestWithMeta, apiJson, withQuery } from './client.js';

const ANALYSIS_TIMEOUT_MS = 45_000;

function get(path, params, options = {}) {
	return apiJson(withQuery(path, params), { timeoutMs: ANALYSIS_TIMEOUT_MS, ...options });
}

export function getAnalysisFilterOptions(options) {
	return get('/api/analysis/filter-options', {}, options);
}

export function getAnalysisEvents(params, options) {
	return get('/api/analysis/events', params, options);
}

export async function exportAnalysisEvents(params, options = {}) {
	const { data: blob, response } = await apiRequestWithMeta(
		withQuery('/api/analysis/events/export.csv', params),
		{ timeoutMs: 60_000, ...options, responseType: 'blob' },
	);
	const disposition = response.headers.get('content-disposition') ?? '';
	const filename = disposition.match(/filename="?([^";]+)"?/i)?.[1] ?? 'sakunagraph-events.csv';
	return { blob, filename };
}

export function getAnalysisSummary(params, options) {
	return get('/api/analysis/summary', params, options);
}

export function getAnalysisDisasterCounts(params, options) {
	return get('/api/analysis/disaster-counts', params, options);
}

export function getAnalysisVictimTrends(params, options) {
	return get('/api/analysis/victim-trends', params, options);
}

export function getAnalysisRegionRankings(params, options) {
	return get('/api/analysis/region-rankings', params, options);
}

export function getAnalysisDisasterRankings(params, options) {
	return get('/api/analysis/disaster-rankings', params, options);
}

export function getAnalysisDamageHistogram(params, options) {
	return get('/api/analysis/damage-histogram', params, options);
}

export function getAnalysisDamageAffected(params, options) {
	return get('/api/analysis/damage-vs-affected', params, options);
}

export function getAnalysisCalendarYears(params, options) {
	return get('/api/analysis/calendar/years', params, options);
}

export function getAnalysisCalendarMonths(params, options) {
	return get('/api/analysis/calendar/months', params, options);
}

export function getAnalysisCalendarDays(params, options) {
	return get('/api/analysis/calendar/days', params, options);
}

export function getAnalysisCategoryStacks(params, options) {
	return get('/api/analysis/timeline/category-stacks', params, options);
}

export function getAnalysisDateEvents(params, options) {
	return get('/api/analysis/timeline/date-events', params, options);
}
