import { COMPETENCY_QUESTIONS } from '../../competency_queries.js';
import { runSparql } from '../../api/sparql.js';

export const DEFAULT_QUERY = `PREFIX :     <https://sakuna.ph/>

SELECT DISTINCT ?event ?disasterType
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType .
}
LIMIT 10`;

export const MAX_QUERY_LENGTH = 50_000;

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

function stripSparqlComments(query) {
	let output = '';
	let quote = '';
	let inIri = false;
	let escaped = false;
	let comment = false;

	for (const character of query) {
		if (comment) {
			if (character === '\n' || character === '\r') {
				comment = false;
				output += character;
			}
			continue;
		}
		if (escaped) {
			escaped = false;
			output += character;
			continue;
		}
		if (quote) {
			if (character === '\\') escaped = true;
			if (character === quote) quote = '';
			output += character;
			continue;
		}
		if (inIri) {
			if (character === '>') inIri = false;
			output += character;
			continue;
		}
		if (character === '<') inIri = true;
		else if (character === '"' || character === "'") quote = character;
		else if (character === '#') {
			comment = true;
			continue;
		}
		output += character;
	}

	return output;
}

export function isSelectQuery(query) {
	const withoutComments = stripSparqlComments(query);
	const withoutPrologue = withoutComments.replace(
		/^\s*(?:(?:PREFIX\s+(?:[A-Za-z][\w-]*)?:\s*<[^>]+>|BASE\s*<[^>]+>)\s*)*/i,
		'',
	);
	return /^SELECT\b/i.test(withoutPrologue);
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
		if (loading) return;
		error = '';
		const trimmed = query.trim();
		if (!trimmed) {
			error = 'Enter a SPARQL SELECT query before running it.';
			return;
		}
		if (isWriteOperation(trimmed)) {
			error =
				'This workspace is read-only. Remove the write operation and run a SPARQL SELECT query instead.';
			return;
		}
		if (!isSelectQuery(trimmed)) {
			error = 'Only SPARQL SELECT queries are supported in this read-only workspace.';
			return;
		}
		if (trimmed.length > MAX_QUERY_LENGTH) {
			error = `The query is too long. Keep it under ${MAX_QUERY_LENGTH.toLocaleString()} characters.`;
			return;
		}

		loading = true;
		const controller = new AbortController();
		activeRequest = controller;
		try {
			results = await execute(trimmed, { signal: controller.signal });
			resultsOpen = true;
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			if (requestError.kind === 'network') {
				error =
					'Could not reach the data service. Check your connection, then run the query again.';
			} else if (requestError.kind === 'timeout') {
				error =
					'The query took too long to complete. Narrow its scope or add a LIMIT, then try again.';
			} else if (requestError.status === 429) {
				error = 'The data service is receiving too many queries. Wait a moment, then try again.';
			} else if (requestError.status === 400 || requestError.status === 422) {
				error =
					requestError.message || 'The query could not be parsed. Check its syntax and try again.';
			} else if (requestError.status >= 500) {
				error =
					'The query service is temporarily unavailable. Your query is preserved; try again shortly.';
			} else {
				error = 'The query could not be completed. Review it and try again.';
			}
		} finally {
			if (activeRequest === controller) {
				loading = false;
				activeRequest = null;
			}
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
			activeRequest?.abort();
			activeRequest = null;
			loading = false;
			selectedCompetency = '';
			loadQuery(DEFAULT_QUERY);
		},
		cancel() {
			activeRequest?.abort();
		},
	};
}
