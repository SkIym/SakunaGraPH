import { COMPETENCY_QUESTIONS } from '../../competency_queries.js';
import { runSparql } from '../../api/sparql.js';

export const DEFAULT_QUERY = `PREFIX :     <https://sakuna.ph/>

SELECT DISTINCT ?event ?disasterType
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType .
}
LIMIT 10`;

export const QUERY_PRESETS = Object.freeze([
	{ label: 'Disaster events', query: DEFAULT_QUERY },
	{
		label: 'Events by type',
		query: `PREFIX :     <https://sakuna.ph/>

SELECT ?disasterType (COUNT(DISTINCT ?event) AS ?count)
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType .
}
GROUP BY ?disasterType
ORDER BY DESC(?count)
LIMIT 10`,
	},
	{
		label: 'Named events',
		query: `PREFIX :    <https://sakuna.ph/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?event ?name ?disasterType
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType ;
         owl:topDataProperty ?name .
  FILTER(datatype(?name) = xsd:string)
}
LIMIT 10`,
	},
	{
		label: 'Ontology classes',
		query: `PREFIX : <https://sakuna.ph/>

SELECT DISTINCT ?class (COUNT(?inst) AS ?count)
WHERE {
  ?inst a ?class .
  FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
}
GROUP BY ?class
ORDER BY DESC(?count)
LIMIT 15`,
	},
]);

const WRITE_PATTERNS = Object.freeze([
	/\bINSERT\b/i,
	/\bDELETE\b/i,
	/\bCLEAR\b/i,
	/\bDROP\b/i,
	/\bCREATE\s+GRAPH\b/i,
	/\bLOAD\b/i,
	/\bCOPY\s+GRAPH\b/i,
	/\bMOVE\s+GRAPH\b/i,
]);

export function isWriteOperation(query) {
	return WRITE_PATTERNS.some((pattern) => pattern.test(query));
}

export function createQueryWorkbench({ execute = runSparql } = {}) {
	let query = $state(DEFAULT_QUERY);
	let editorKey = $state(0);
	let selectedCompetency = $state('');
	let results = $state(null);
	let loading = $state(false);
	let error = $state('');
	let resultsOpen = $state(false);
	let activeRequest = null;

	function loadQuery(nextQuery) {
		query = nextQuery;
		editorKey += 1;
		error = '';
	}

	function selectCompetency() {
		if (!selectedCompetency) return;
		const competency = COMPETENCY_QUESTIONS[Number.parseInt(selectedCompetency, 10)];
		if (competency) loadQuery(competency.query);
	}

	function loadPreset(preset) {
		selectedCompetency = '';
		loadQuery(preset.query);
	}

	async function run() {
		error = '';
		const trimmed = query.trim();
		if (!trimmed) {
			error = 'Please enter a SPARQL query.';
			return;
		}
		if (isWriteOperation(trimmed)) {
			error =
				'Write operations (INSERT, DELETE, CLEAR, DROP, etc.) are not permitted. This is a read-only interface.';
			return;
		}

		loading = true;
		activeRequest = new AbortController();
		try {
			results = await execute(trimmed, { signal: activeRequest.signal });
			resultsOpen = true;
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			error =
				requestError.kind === 'network'
					? 'Could not reach the server. Please try again.'
					: requestError.message || 'An error occurred while processing the query.';
		} finally {
			loading = false;
			activeRequest = null;
		}
	}

	function handleEditorKeydown(event) {
		if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
			event.preventDefault();
			void run();
		}
	}

	return {
		get query() {
			return query;
		},
		set query(value) {
			query = value;
		},
		get editorKey() {
			return editorKey;
		},
		get selectedCompetency() {
			return selectedCompetency;
		},
		set selectedCompetency(value) {
			selectedCompetency = value;
		},
		get results() {
			return results;
		},
		get loading() {
			return loading;
		},
		get error() {
			return error;
		},
		get resultsOpen() {
			return resultsOpen;
		},
		loadQuery,
		selectCompetency,
		loadPreset,
		run,
		handleEditorKeydown,
		closeResults() {
			resultsOpen = false;
		},
		reset() {
			selectedCompetency = '';
			loadQuery(DEFAULT_QUERY);
		},
		cancel() {
			activeRequest?.abort();
		},
	};
}
