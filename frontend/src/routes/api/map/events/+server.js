import { json, error } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

const PAGE_SIZE = 10;

const P = `PREFIX :    <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX prov: <http://www.w3.org/ns/prov#>
`;

// ── Query builders ────────────────────────────────────────────────────────────
function regionEventsQuery(psgc, offset, limit, eventType) {
	return P + `
    SELECT ?event ?startDate
        (SAMPLE(?eventName)                              AS ?eventName)
        (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
        (GROUP_CONCAT(DISTINCT ?dtype;   separator=",")  AS ?disasterType)
        (SAMPLE(?alts)                                   AS ?alternates)
        (SAMPLE(?srcLabel)                               AS ?source)
    WHERE {
    ?event a :${eventType} ;
            :hasDisasterType ?dtype ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* :${psgc} .
    OPTIONAL { ?event    :eventName  ?eventName }
    OPTIONAL { ?location rdfs:label  ?locLabel  }
    OPTIONAL {
        SELECT ?event (GROUP_CONCAT(DISTINCT ?alt; separator=",") AS ?alts)
        WHERE { ?event prov:alternateOf ?alt }
        GROUP BY ?event
    }
    OPTIONAL { ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?agent .
                ?agent skos:prefLabel ?srcLabel }
    }
    GROUP BY ?event ?startDate
    ORDER BY DESC(?startDate)
    LIMIT ${limit}
    OFFSET ${offset}`;
}

function provinceEventsQuery(normalized, offset, limit, eventType) {
	return P + `
    SELECT ?event ?startDate
        (SAMPLE(?eventName)                              AS ?eventName)
        (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
        (GROUP_CONCAT(DISTINCT ?dtype;   separator=",")  AS ?disasterTypes)
        (SAMPLE(?alts)                                   AS ?alternates)
        (SAMPLE(?srcLabel)                               AS ?source)
    WHERE {
    ?event a :${eventType} ;
            :hasDisasterType ?dtype ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* ?prov .
    ?prov a :Province ; rdfs:label ?provLabel .
    FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalized}")
    OPTIONAL { ?event    :eventName  ?eventName }
    OPTIONAL { ?location rdfs:label  ?locLabel  }
    OPTIONAL {
        SELECT ?event (GROUP_CONCAT(DISTINCT ?alt; separator=",") AS ?alts)
        WHERE { ?event prov:alternateOf ?alt }
        GROUP BY ?event
    }
    OPTIONAL { ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?agent .
                ?agent skos:prefLabel ?srcLabel }
    }
    GROUP BY ?event ?startDate
    ORDER BY DESC(?startDate)
    LIMIT ${limit}
    OFFSET ${offset}`;
}

function regionCountQuery(psgc, eventType) {
    return P + `
    SELECT (COUNT(DISTINCT ?event) AS ?count)
    WHERE {
    ?event a :${eventType} ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* :${psgc} .
    OPTIONAL {
    ?event prov:alternateOf ?alt .
    ?alt :startDate ?altDate .
    FILTER(?altDate < ?startDate || (?altDate = ?startDate && STR(?alt) < STR(?event)))
    }
    FILTER(!BOUND(?altDate))
    }`;
}

function provinceCountQuery(normalized, eventType) {
  return P + `
    SELECT (COUNT(DISTINCT ?event) AS ?count)
    WHERE {
    ?event a :${eventType} ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* ?prov .
    ?prov a :Province ; rdfs:label ?provLabel .
    FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalized}")
    OPTIONAL {
    ?event prov:alternateOf ?alt .
    ?alt :startDate ?altDate .
    FILTER(?altDate < ?startDate || (?altDate = ?startDate && STR(?alt) < STR(?event)))
    }
    FILTER(!BOUND(?altDate))
    }`;
}

function cityEventsQuery(normalizedCity, normalizedProv, offset, limit, eventType) {
	const provFilter = normalizedProv
		? `?muni :isPartOf ?prov . ?prov a :Province ; rdfs:label ?provLabel .
    FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalizedProv}")`
		: '';
	return P + `
    SELECT ?event ?startDate
        (SAMPLE(?eventName)                              AS ?eventName)
        (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
        (GROUP_CONCAT(DISTINCT ?dtype;   separator=",")  AS ?disasterTypes)
        (SAMPLE(?alts)                                   AS ?alternates)
        (SAMPLE(?srcLabel)                               AS ?source)
    WHERE {
    ?event a :${eventType} ;
            :hasDisasterType ?dtype ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* ?muni .
    ?muni a :Municipality ; rdfs:label ?muniLabel .
    FILTER(REPLACE(LCASE(STR(?muniLabel)), "[^a-z0-9]", "") = "${normalizedCity}")
    ${provFilter}
    OPTIONAL { ?event    :eventName  ?eventName }
    OPTIONAL { ?location rdfs:label  ?locLabel  }
    OPTIONAL {
        SELECT ?event (GROUP_CONCAT(DISTINCT ?alt; separator=",") AS ?alts)
        WHERE { ?event prov:alternateOf ?alt }
        GROUP BY ?event
    }
    OPTIONAL { ?event prov:wasDerivedFrom+/prov:wasAttributedTo ?agent .
                ?agent skos:prefLabel ?srcLabel }
    }
    GROUP BY ?event ?startDate
    ORDER BY DESC(?startDate)
    LIMIT ${limit}
    OFFSET ${offset}`;
}

function cityCountQuery(normalizedCity, normalizedProv, eventType) {
	const provFilter = normalizedProv
		? `?muni :isPartOf ?prov . ?prov a :Province ; rdfs:label ?provLabel .
    FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalizedProv}")`
		: '';
	return P + `
    SELECT (COUNT(DISTINCT ?event) AS ?count)
    WHERE {
    ?event a :${eventType} ;
            :startDate ?startDate ;
            :hasLocation ?location .
    ?location :isPartOf* ?muni .
    ?muni a :Municipality ; rdfs:label ?muniLabel .
    FILTER(REPLACE(LCASE(STR(?muniLabel)), "[^a-z0-9]", "") = "${normalizedCity}")
    ${provFilter}
    OPTIONAL {
    ?event prov:alternateOf ?alt .
    ?alt :startDate ?altDate .
    FILTER(?altDate < ?startDate || (?altDate = ?startDate && STR(?alt) < STR(?event)))
    }
    FILTER(!BOUND(?altDate))
    }`;
}
// ── Helpers ───────────────────────────────────────────────────────────────────

function extractCount(result) {
	const v = result?.results?.bindings?.[0]?.count?.value;
	return v ? parseInt(v, 10) : 0;
}

// ── Handler ───────────────────────────────────────────────────────────────────

export async function GET({ url }) {
	const scope    = url.searchParams.get('scope');    // 'region' | 'province' | 'city'
	const id       = url.searchParams.get('id');        // PSGC code or raw GADM name
	const province = url.searchParams.get('province'); // city scope only — disambiguates same-name cities
	const mode     = url.searchParams.get('mode') ?? 'major';  // 'major' | 'incidents'
	const page     = Math.max(1, parseInt(url.searchParams.get('page') ?? '1', 10));

	if (!scope || !id) {
		throw error(400, 'Missing required params: scope, id');
	}
	if (scope !== 'region' && scope !== 'province' && scope !== 'city') {
		throw error(400, 'scope must be "region", "province", or "city"');
	}

	const activeType = mode === 'major' ? 'MajorEvent' : 'Incident';
	const offset     = (page - 1) * PAGE_SIZE;

	let eventsQ, majorCountQ, incidentCountQ;

	if (scope === 'region') {
		eventsQ        = regionEventsQuery(id, offset, PAGE_SIZE, activeType);
		majorCountQ    = regionCountQuery(id, 'MajorEvent');
		incidentCountQ = regionCountQuery(id, 'Incident');
	} else if (scope === 'city') {
		const normalizedCity = id.toLowerCase().replace(/[^a-z0-9]/g, '');
		const normalizedProv = province ? province.toLowerCase().replace(/[^a-z0-9]/g, '') : null;
		eventsQ        = cityEventsQuery(normalizedCity, normalizedProv, offset, PAGE_SIZE, activeType);
		majorCountQ    = cityCountQuery(normalizedCity, normalizedProv, 'MajorEvent');
		incidentCountQ = cityCountQuery(normalizedCity, normalizedProv, 'Incident');
	} else {
		const normalized = id.toLowerCase().replace(/[^a-z0-9]/g, '');
		eventsQ        = provinceEventsQuery(normalized, offset, PAGE_SIZE, activeType);
		majorCountQ    = provinceCountQuery(normalized, 'MajorEvent');
		incidentCountQ = provinceCountQuery(normalized, 'Incident');
	}

	const [eventsRes, majorCountRes, incidentCountRes] = await Promise.all([
		executeSparql(eventsQ),
		executeSparql(majorCountQ),
		executeSparql(incidentCountQ),
	]);

	return json({
		events:        eventsRes.results.bindings,
		majorCount:    extractCount(majorCountRes),
		incidentCount: extractCount(incidentCountRes),
	});
}