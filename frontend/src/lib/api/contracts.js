/**
 * Runtime inventory paired with the JSDoc contracts below. Paths and methods are verified against
 * the committed FastAPI OpenAPI snapshot by the contract suite.
 */
export const CONSUMED_API_OPERATIONS = Object.freeze({
	sparql: { method: 'post', path: '/api/sparql', response: 'SparqlQueryResponse' },
	ask: { method: 'post', path: '/api/ask', response: 'AskResponse' },
	askPreview: { method: 'post', path: '/api/ask/preview', response: 'AskPreviewResponse' },
	askStream: { method: 'post', path: '/api/ask/stream', response: 'text/event-stream' },
	mapEvents: { method: 'get', path: '/api/map/events', response: 'MapEventsResponse' },
	disasterDetails: {
		method: 'get',
		path: '/api/disasters/details',
		response: 'EventDetailsResponse',
	},
	ontologyGraph: { method: 'get', path: '/api/ontology/graph', response: 'OntologyGraphResponse' },
	ontologyTaxonomy: { method: 'get', path: '/api/ontology/taxonomy', response: 'TaxonomyNode' },
	ontologyPsgc: { method: 'get', path: '/api/ontology/psgc', response: 'PsgcGraphResponse' },
	analysisFilterOptions: {
		method: 'get',
		path: '/api/analysis/filter-options',
		response: 'AnalysisFilterOptionsResponse',
	},
	analysisEvents: {
		method: 'get',
		path: '/api/analysis/events',
		response: 'AnalysisEventsResponse',
	},
	analysisEventsExport: {
		method: 'get',
		path: '/api/analysis/events/export.csv',
		response: 'text/csv',
	},
	analysisSummary: {
		method: 'get',
		path: '/api/analysis/summary',
		response: 'AnalysisSummaryResponse',
	},
	analysisDisasterCounts: {
		method: 'get',
		path: '/api/analysis/disaster-counts',
		response: 'AnalysisDisasterCountsResponse',
	},
	analysisVictimTrends: {
		method: 'get',
		path: '/api/analysis/victim-trends',
		response: 'AnalysisVictimTrendsResponse',
	},
	analysisRegionRankings: {
		method: 'get',
		path: '/api/analysis/region-rankings',
		response: 'AnalysisRegionRankingsResponse',
	},
	analysisDisasterRankings: {
		method: 'get',
		path: '/api/analysis/disaster-rankings',
		response: 'AnalysisDisasterRankingsResponse',
	},
	analysisDamageHistogram: {
		method: 'get',
		path: '/api/analysis/damage-histogram',
		response: 'AnalysisDamageHistogramResponse',
	},
	analysisDamageAffected: {
		method: 'get',
		path: '/api/analysis/damage-vs-affected',
		response: 'AnalysisDamageAffectedResponse',
	},
	analysisCalendarYears: {
		method: 'get',
		path: '/api/analysis/calendar/years',
		response: 'AnalysisCalendarResponse',
	},
	analysisCalendarMonths: {
		method: 'get',
		path: '/api/analysis/calendar/months',
		response: 'AnalysisCalendarResponse',
	},
	analysisCalendarDays: {
		method: 'get',
		path: '/api/analysis/calendar/days',
		response: 'AnalysisCalendarResponse',
	},
	analysisCategoryStacks: {
		method: 'get',
		path: '/api/analysis/timeline/category-stacks',
		response: 'AnalysisTimelineCategoryStacksResponse',
	},
	analysisDateEvents: {
		method: 'get',
		path: '/api/analysis/timeline/date-events',
		response: 'AnalysisTimelineDateEventsResponse',
	},
});

/** @typedef {{ value: string, type: string, datatype?: string }} SparqlTerm */
/** @typedef {{ head?: {vars?: string[]}, results?: {bindings?: Record<string, SparqlTerm>[]}, boolean?: boolean }} SparqlQueryResponse */
/** @typedef {{ id: string, label: string, uri: string, sourceRecord?: string, excerpt?: string }} AskCitation */
/** @typedef {{ mode?: 'legacy'|'graphrag'|'fallback', indexVersion?: string, sourceCount?: number }} AskRetrieval */
/** @typedef {{ sparql: string, answer: string, rows: Record<string, unknown>[], citations?: AskCitation[], retrieval?: AskRetrieval }} AskResponse */
/** @typedef {{ sparql: string }} AskPreviewResponse */
/** @typedef {{ type: 'meta', sparql: string, rows: Record<string, unknown>[], citations?: AskCitation[], retrieval?: AskRetrieval, requestId?: string }} AskStreamMetaEvent */
/** @typedef {{ type: 'token', text: string }} AskStreamTokenEvent */
/** @typedef {{ type: 'citation'|`citation.v${number}`, citation: AskCitation }} AskStreamCitationEvent */
/** @typedef {{ type: 'done', citations?: AskCitation[], retrieval?: AskRetrieval }} AskStreamDoneEvent */
/** @typedef {{ type: 'error', status: number, detail: string, requestId?: string }} AskStreamErrorEvent */

/** @typedef {{ event: string, eventName: string, startDate: string, disasterTypes?: string[], locations?: string[], source?: string, alternates?: MapEvent[] }} MapEvent */
/** @typedef {{ events: MapEvent[], majorCount: number, incidentCount: number }} MapEventsResponse */
/** @typedef {{ uri: string, id: string, label: string }} IriLabel */
/** @typedef {{ uri: string, name: string, startDate?: string, endDate?: string, eventType?: string }} RelatedEvent */
/** @typedef {{ uri: string, reportName: string, reportLink?: string, format?: string, obtainedDate?: string, lastUpdateDate?: string, attributedTo?: IriLabel[] }} EventSource */
/** @typedef {{ event: string, name: string, eventType: string, startDate?: string, endDate?: string, remarks?: string[], locations?: IriLabel[], disasterTypes?: IriLabel[], incidents?: RelatedEvent[], majorEvents?: RelatedEvent[], sources?: EventSource[], alternates?: RelatedEvent[] }} EventDetailsResponse */

/** @typedef {{ id: string, label: string, group: string, definition: string, dataProperties?: {label: string, range: string}[] }} OntologyNode */
/** @typedef {{ source: string, target: string, type: string, label: string }} OntologyLink */
/** @typedef {{ nodes: OntologyNode[], links: OntologyLink[] }} OntologyGraphResponse */
/** @typedef {{ id: string, label: string, group: string, definition: string, children?: TaxonomyNode[] }} TaxonomyNode */
/** @typedef {{ id: string, label: string, level?: string, island?: string, population?: number, psgcCode?: string }} PsgcNode */
/** @typedef {{ nodes: PsgcNode[], links: {source: string, target: string}[] }} PsgcGraphResponse */

/** @typedef {{ id: string, label: string }} AnalysisFacet */
/** @typedef {{ locations: AnalysisFacet[], disasterTypes: AnalysisFacet[] }} AnalysisFilterOptionsResponse */
/** @typedef {{ event: string, eventName: string, eventType: string, startDate: string, endDate?: string, locations?: string[], disasterTypes?: string[], source?: string, impact?: Record<string, unknown>, alternates?: AnalysisEvent[] }} AnalysisEvent */
/** @typedef {{ items: AnalysisEvent[], page: number, page_size: number, total: number, sort_by: string, sort_dir: string }} AnalysisEventsResponse */
/** @typedef {{ record_count?: number, affectedFamilies?: number, affectedPersons?: number, dead?: number, injured?: number, missing?: number, damage?: {amount: number, unit: string}[] }} AnalysisSummaryResponse */
/** @typedef {{ id: string, label: string, count: number }} AnalysisCount */
/** @typedef {{ group_by: string, items?: AnalysisCount[] }} AnalysisDisasterCountsResponse */
/** @typedef {{ year: number, dead?: number, injured?: number, missing?: number }} AnalysisVictimTrend */
/** @typedef {{ items?: AnalysisVictimTrend[] }} AnalysisVictimTrendsResponse */
/** @typedef {{ items?: AnalysisCount[] }} AnalysisRegionRankingsResponse */
/** @typedef {{ id: string, label: string, dead?: number }} AnalysisDisasterRanking */
/** @typedef {{ items?: AnalysisDisasterRanking[] }} AnalysisDisasterRankingsResponse */
/** @typedef {{ unit: string, lowerBound: number, upperBound: number, count: number }} AnalysisHistogramBin */
/** @typedef {{ bins?: AnalysisHistogramBin[] }} AnalysisDamageHistogramResponse */
/** @typedef {{ event: string, eventName: string, unit: string, damage: number, affectedFamilies?: number, affectedPersons?: number }} AnalysisDamageAffectedPoint */
/** @typedef {{ items?: AnalysisDamageAffectedPoint[] }} AnalysisDamageAffectedResponse */
/** @typedef {{ period: string, count: number, dead?: number, injured?: number, missing?: number }} AnalysisCalendarItem */
/** @typedef {{ items?: AnalysisCalendarItem[] }} AnalysisCalendarResponse */
/** @typedef {{ period: string, categories?: Record<string, number> }} AnalysisTimelineCategoryStack */
/** @typedef {{ bucket: string, items?: AnalysisTimelineCategoryStack[] }} AnalysisTimelineCategoryStacksResponse */
/** @typedef {{ date_prefix: string, items?: AnalysisEvent[] }} AnalysisTimelineDateEventsResponse */
