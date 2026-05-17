import type { RequestHandler } from './$types';

const GRAPHDB = 'http://localhost:7200/repositories/SakunaGraph';

const PREFIX = `
    PREFIX sakuna: <https://sakuna.ph/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
`;

async function sparql(query: string) {
    const res = await fetch(GRAPHDB, {
        method: 'POST',
        headers: { 'Content-Type': 'application/sparql-query', 'Accept': 'application/sparql-results+json' },
        body: PREFIX + query
    });
    if (!res.ok) throw new Error(`SPARQL ${res.status}`);
    return (await res.json()).results.bindings as Record<string, { value: string }>[];
}

function val(b: Record<string, { value: string }>, k: string) {
    return b[k]?.value ?? '';
}

// GraphDB PSGC labels → GeoJSON name / REGION values
const REGION_NORM: Record<string, string> = {
    'Region I (Ilocos Region)': 'I - Ilocos Region',
    'Region II (Cagayan Valley)': 'II - Cagayan Valley',
    'Region III (Central Luzon)': 'III - Central Luzon',
    'Region IV-A (CALABARZON)': 'IV-A - Calabarzon',
    'MIMAROPA Region': 'Mimaropa',
    'Region V (Bicol Region)': 'V - Bicol Region',
    'Region VI (Western Visayas)': 'VI - Western Visayas',
    'Region VII (Central Visayas)': 'VII - Central Visayas',
    'Region VIII (Eastern Visayas)': 'VIII - Eastern Visayas',
    'Region IX (Zamboanga Peninsula)': 'IX - Zamboanga Peninsula',
    'Region X (Northern Mindanao)': 'X - Northern Mindanao',
    'Region XI (Davao Region)': 'XI - Davao Region',
    'Region XII (SOCCSKSARGEN)': 'XII - Soccsksargen',
    'Region XIII (Caraga)': 'XIII - Caraga',
    'Cordillera Administrative Region (CAR)': 'Cordillera Administrative Region',
    'National Capital Region (NCR)': 'National Capital Region',
    'Bangsamoro Autonomous Region In Muslim Mindanao (BARMM)': 'Bangsamoro',
    'Negros Island Region (NIR)': 'Negros Island Region',
};

function normalizeRegion(label: string): string {
    return REGION_NORM[label] ?? label;
}

let cache: string | null = null;
let cacheTime = 0;
const CACHE_TTL = 10 * 60 * 1000; // 10 minutes

export const GET: RequestHandler = async () => {
    const now = Date.now();
    if (cache && (now - cacheTime) < CACHE_TTL) {
        return new Response(cache, { headers: { 'Content-Type': 'application/json' } });
    }

    const [eventsRows, locRows, casRows] = await Promise.all([
        sparql(`
            SELECT DISTINCT ?event (SAMPLE(?n) AS ?name) (SAMPLE(?tn) AS ?type) (SAMPLE(?d) AS ?date)
            WHERE {
                ?event a sakuna:DisasterEvent .
                OPTIONAL { ?event sakuna:eventName ?n }
                OPTIONAL { ?event sakuna:hasDisasterType ?dt . ?dt skos:prefLabel ?tn }
                OPTIONAL { ?event sakuna:startDate ?d }
            } GROUP BY ?event
        `),
        sparql(`
            SELECT ?event ?munLabel ?provLabel ?regLabel WHERE {
                ?event a sakuna:DisasterEvent .
                ?event sakuna:hasLocation ?loc .
                OPTIONAL {
                    ?loc a sakuna:Municipality . ?loc rdfs:label ?munLabel .
                    OPTIONAL { ?loc sakuna:isPartOf ?prov . ?prov a sakuna:Province . ?prov rdfs:label ?provLabel .
                               OPTIONAL { ?prov sakuna:isPartOf ?reg . ?reg a sakuna:Region . ?reg rdfs:label ?regLabel } }
                }
                OPTIONAL {
                    FILTER NOT EXISTS { ?loc a sakuna:Municipality }
                    ?loc a sakuna:Province . ?loc rdfs:label ?provLabel .
                    OPTIONAL { ?loc sakuna:isPartOf ?reg . ?reg a sakuna:Region . ?reg rdfs:label ?regLabel }
                }
                OPTIONAL {
                    FILTER NOT EXISTS { ?loc a sakuna:Municipality }
                    FILTER NOT EXISTS { ?loc a sakuna:Province }
                    ?loc a sakuna:Region . ?loc rdfs:label ?regLabel
                }
            }
        `),
        sparql(`
            SELECT ?event
                (SUM(COALESCE(IF(STR(?ct) = "DEAD",    xsd:integer(?cc), 0), 0)) AS ?casualties)
                (SUM(COALESCE(IF(STR(?ct) = "INJURED", xsd:integer(?cc), 0), 0)) AS ?injured)
                (SUM(COALESCE(IF(STR(?ct) = "MISSING", xsd:integer(?cc), 0), 0)) AS ?missing)
                (MAX(COALESCE(?ap, 0)) AS ?affected_families)
            WHERE {
                ?event a sakuna:DisasterEvent .
                OPTIONAL { ?event sakuna:hasCasualties ?cas . ?cas sakuna:casualtyType ?ct . ?cas sakuna:casualtyCount ?cc }
                OPTIONAL { ?event sakuna:hasAffectedPopulation ?a . ?a sakuna:affectedPersons ?ap }
            } GROUP BY ?event
        `)
    ]);

    const eventsMap = new Map<string, { name: string; type: string; date: string }>();
    for (const b of eventsRows) {
        eventsMap.set(val(b, 'event'), {
            name: val(b, 'name'),
            type: val(b, 'type'),
            date: val(b, 'date').split('T')[0]
        });
    }

    const locMap = new Map<string, { city_municipality: string; province: string; region: string }[]>();
    for (const b of locRows) {
        const iri = val(b, 'event');
        const entry = { city_municipality: val(b, 'munLabel'), province: val(b, 'provLabel'), region: normalizeRegion(val(b, 'regLabel')) };
        if (!locMap.has(iri)) locMap.set(iri, []);
        locMap.get(iri)!.push(entry);
    }

    const casMap = new Map<string, { casualties: number; injured: number; missing: number; affected_families: number }>();
    for (const b of casRows) {
        casMap.set(val(b, 'event'), {
            casualties: parseInt(val(b, 'casualties')) || 0,
            injured: parseInt(val(b, 'injured')) || 0,
            missing: parseInt(val(b, 'missing')) || 0,
            affected_families: parseInt(val(b, 'affected_families')) || 0
        });
    }

    const rows: object[] = [];
    const seen = new Set<string>();
    for (const [iri, event] of eventsMap) {
        const locs = locMap.get(iri) ?? [{ city_municipality: '', province: '', region: '' }];
        const cas = casMap.get(iri) ?? { casualties: 0, injured: 0, missing: 0, affected_families: 0 };
        for (const loc of locs) {
            const key = `${iri}|${loc.region}|${loc.province}|${loc.city_municipality}`;
            if (seen.has(key)) continue;
            seen.add(key);
            rows.push({ name: event.name, type: event.type, date: event.date, ...loc, ...cas, monetary_damage: 0 });
        }
    }

    cache = JSON.stringify(rows);
    cacheTime = Date.now();

    return new Response(cache, { headers: { 'Content-Type': 'application/json' } });
};
