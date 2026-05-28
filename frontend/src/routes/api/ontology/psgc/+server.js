import { json } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

// ── SPARQL queries ────────────────────────────────────────────────────────────

// Only Regions, Provinces, and independent cities (HUC / ICC) are included.
// NCR municipalities (e.g. Pateros) are also included as city-level nodes
// because they report directly to the NCR region with no province in between.
//
// Node shapes:
//   Region:   { id, label, fullName, level:"Region",   island, population, psgcCode }
//   Province: { id, label,           level:"Province", island, incomeClass, population, psgcCode, regionId }
//   City:     { id, label,           level:"City",     island, population, psgcCode,
//               cityType:"HUC"|"ICC"|"Municipality", regionId, regionLabel, note? }
//
// Links: { source: child_code, target: region_code }

const PSGC_REGIONS_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT ?code ?label ?fullName ?population WHERE {
    ?r a :Region .
    ?r :psgcCode ?code .
    ?r rdfs:label     ?label .
    OPTIONAL { ?r skos:altLabel        ?fullName   }
    OPTIONAL { ?r :population2024 ?population }
}
`;

const PSGC_PROVINCES_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?code ?label ?population ?incomeClass ?regionCode WHERE {
    ?p a :Province .
    ?p :psgcCode ?code .
    ?p rdfs:label     ?label .
    ?p :isPartOf ?region .
    ?region a :Region .
    ?region :psgcCode ?regionCode .
    OPTIONAL { ?p :population2024      ?population  }
    OPTIONAL { ?p :incomeClassification ?incomeClass }
}
`;

// Covers HUC/ICC cities (isPartOf a Region directly) plus NCR municipalities.
const PSGC_CITIES_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:    <http://www.w3.org/2001/XMLSchema#>

SELECT ?code ?label ?population ?incomeClass ?cityClass ?regionCode ?regionLabel ?note WHERE {
    {
        ?c a :City .
        ?c :cityClass ?cityClass .
        FILTER(STR(?cityClass) = "HUC" || STR(?cityClass) = "ICC")
        ?c :isPartOf ?region .
        ?region a :Region .
    } UNION {
        # Pateros and any other municipality that reports directly to NCR
        ?c a :Municipality .
        BIND("Municipality" AS ?cityClass)
        ?c :isPartOf ?region .
        ?region :psgcCode "1300000000"^^xsd:string .
    }
    ?c :psgcCode ?code .
    ?c rdfs:label      ?label .
    ?region :psgcCode ?regionCode .
    ?region rdfs:label      ?regionLabel .
    OPTIONAL { ?c :population2024      ?population  }
    OPTIONAL { ?c :incomeClassification ?incomeClass }
    OPTIONAL { ?c rdfs:comment               ?note        }
}
`;

// ── Helpers ───────────────────────────────────────────────────────────────────

const ISLAND_LUZON    = new Set(['01', '02', '03', '04', '05', '14', '17']);
const ISLAND_VISAYAS  = new Set(['06', '07', '08', '18']);

function regionToIsland(code) {
	if (code === '1300000000') return 'NCR';
	const prefix = code.slice(0, 2);
	if (ISLAND_LUZON.has(prefix))   return 'Luzon';
	if (ISLAND_VISAYAS.has(prefix)) return 'Visayas';
	return 'Mindanao';
}

function val(binding, key, fallback = null) {
	return binding[key]?.value ?? fallback;
}

function buildPsgc(regionBindings, provinceBindings, cityBindings) {
	const nodes = [];
	const links = [];

	for (const b of regionBindings) {
		const code  = val(b, 'code');
		const label = val(b, 'label');
		nodes.push({
			id:         code,
			label,
			fullName:   val(b, 'fullName') ?? label,
			level:      'Region',
			island:     regionToIsland(code),
			population: parseInt(val(b, 'population', 0)),
			psgcCode:   code,
		});
	}

	for (const b of provinceBindings) {
		const code       = val(b, 'code');
		const regionCode = val(b, 'regionCode');
		nodes.push({
			id:          code,
			label:       val(b, 'label'),
			level:       'Province',
			island:      regionToIsland(regionCode),
			incomeClass: val(b, 'incomeClass'),
			population:  parseInt(val(b, 'population', 0)),
			psgcCode:    code,
			regionId:    regionCode,
		});
		links.push({ source: code, target: regionCode });
	}

	for (const b of cityBindings) {
		const code       = val(b, 'code');
		const regionCode = val(b, 'regionCode');
		const node = {
			id:          code,
			label:       val(b, 'label'),
			level:       'City',
			island:      regionToIsland(regionCode),
			population:  parseInt(val(b, 'population', 0)),
			psgcCode:    code,
			cityType:    val(b, 'cityClass'),
			regionId:    regionCode,
			regionLabel: val(b, 'regionLabel'),
		};
		const note = val(b, 'note');
		if (note) node.note = note;
		nodes.push(node);
		links.push({ source: code, target: regionCode });
	}

	return { nodes, links };
}

// ── Handler ───────────────────────────────────────────────────────────────────

export async function GET() {
	const [regionRes, provinceRes, cityRes] = await Promise.all([
		executeSparql(PSGC_REGIONS_QUERY),
		executeSparql(PSGC_PROVINCES_QUERY),
		executeSparql(PSGC_CITIES_QUERY),
	]);

	return json(buildPsgc(
		regionRes.results.bindings,
		provinceRes.results.bindings,
		cityRes.results.bindings,
	));
}