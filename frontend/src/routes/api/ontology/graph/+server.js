import { json, error } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

// ── SPARQL queries ────────────────────────────────────────────────────────────

const GRAPH_CLASSES_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?class ?label ?definition WHERE {
    ?class a owl:Class .
    FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
    FILTER(isIRI(?class))
    OPTIONAL { ?class rdfs:label    ?label      }
    OPTIONAL { ?class skos:definition ?definition }
}
`;

const GRAPH_SUBCLASSOF_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?child ?parent WHERE {
    ?child rdfs:subClassOf ?parent .
    FILTER(STRSTARTS(STR(?child),  "https://sakuna.ph/"))
    FILTER(STRSTARTS(STR(?parent), "https://sakuna.ph/"))
    FILTER(isIRI(?child))
    FILTER(isIRI(?parent))
    FILTER(?child != ?parent)
}
`;

// Only top-level : properties (no sub-properties, no external namespaces).
// Union-typed domain/range nodes fail the isIRI() filter, so properties like
// isPartOf are excluded — keeps the graph clean.
const GRAPH_OBJPROPS_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?prop ?label ?domain ?range WHERE {
    ?prop a owl:ObjectProperty .
    FILTER(STRSTARTS(STR(?prop), "https://sakuna.ph/"))
    FILTER NOT EXISTS {
        ?prop rdfs:subPropertyOf ?super .
        FILTER(?super != ?prop)
        FILTER(STRSTARTS(STR(?super), "https://sakuna.ph/"))
    }
    ?prop rdfs:domain ?domain .
    ?prop rdfs:range  ?range .
    FILTER(isIRI(?domain) && STRSTARTS(STR(?domain), "https://sakuna.ph/"))
    FILTER(isIRI(?range)  && STRSTARTS(STR(?range),  "https://sakuna.ph/"))
    OPTIONAL { ?prop rdfs:label ?label }
}
`;

// Data properties grouped by their (possibly union) domain class.
const GRAPH_DATAPROPS_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:    <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?class ?propLabel ?range WHERE {
    ?prop a owl:DatatypeProperty .
    {
        ?prop rdfs:domain ?class .
        FILTER(isIRI(?class))
    } UNION {
        ?prop rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?class .
        FILTER(isIRI(?class))
    }
    FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
    FILTER NOT EXISTS {
        ?sub rdfs:subClassOf ?class .
        FILTER(?sub != ?class)
        FILTER(STRSTARTS(STR(?sub), "https://sakuna.ph/"))
        {
            ?prop rdfs:domain ?sub .
            FILTER(isIRI(?sub))
        } UNION {
            ?prop rdfs:domain/owl:unionOf/rdf:rest*/rdf:first ?sub .
            FILTER(isIRI(?sub))
        }
    }
    OPTIONAL { ?prop rdfs:label ?propLabel }
    OPTIONAL {
        ?prop rdfs:range ?range .
        FILTER(!STRSTARTS(STR(?range), "http://www.w3.org/2000/01/rdf-schema#"))
    }
}
`;

// ── Helpers ───────────────────────────────────────────────────────────────────

const CLASS_BLACKLIST = new Set(['DisasterTypeScheme']);

const NODE_GROUP = {
	DisasterEvent: 'core', MajorEvent: 'core', Incident: 'core',
	Impact: 'impact', AffectedPopulation: 'impact', Casualties: 'impact',
	HousingDamage: 'impact', AgricultureDamage: 'impact',
	InfrastructureDamage: 'impact', DamageGeneral: 'impact',
	AirportDisruption: 'impact', FlightDisruption: 'impact',
	SeaportDisruption: 'impact', StrandedEvent: 'impact',
	PowerDisruption: 'impact', WaterDisruption: 'impact',
	CommunicationLineDisruption: 'impact', ClassSuspension: 'impact',
	WorkSuspension: 'impact', RoadAndBridgesDamage: 'impact',
	Response: 'response', Assistance: 'response', Relief: 'response',
	Recovery: 'response', DeclarationOfCalamity: 'response',
	Preparedness: 'preparedness', PreemptiveEvacuation: 'preparedness',
	Rescue: 'preparedness',
	Location: 'location', Country: 'location', IslandGroup: 'location',
	Region: 'location', Province: 'location', Municipality: 'location',
	City: 'location', SubMunicipality: 'location', Barangay: 'location',
	DisasterType: 'type',
	Source: 'source',
};

function val(binding, key, fallback = null) {
	return binding[key]?.value ?? fallback;
}

function localName(iri) {
	return iri.includes('#') ? iri.split('#').at(-1) : iri.split('/').at(-1);
}

function rangeLabel(rangeIri) {
	if (!rangeIri) return '';
	const local = localName(rangeIri);
	return rangeIri.includes('XMLSchema') ? `xsd:${local}` : local;
}

function buildGraph(classBindings, subclassofBindings, objpropBindings, datapropBindings) {
	// Data properties keyed by class local-name → deduped list
	const dataProps = {};
	const seenDp = new Set();
	for (const b of datapropBindings) {
		const classIri = val(b, 'class');
		if (!classIri) continue;
		const classLocal = localName(classIri);
		const label = val(b, 'propLabel', '');
		const rng = rangeLabel(val(b, 'range'));
		const key = `${classLocal}|${label}|${rng}`;
		if (seenDp.has(key)) continue;
		seenDp.add(key);
		(dataProps[classLocal] ??= []).push({ label, range: rng });
	}

	// Nodes
	const nodes = [];
	for (const b of classBindings) {
		const classIri = val(b, 'class');
		if (!classIri) continue;
		const local = localName(classIri);
		if (CLASS_BLACKLIST.has(local)) continue;
		const node = {
			id:         local,
			label:      val(b, 'label', local),
			group:      NODE_GROUP[local] ?? 'source',
			definition: val(b, 'definition', ''),
		};
		if (dataProps[local]?.length) node.dataProperties = dataProps[local];
		nodes.push(node);
	}

	// subClassOf links
	const links = [];
	for (const b of subclassofBindings) {
		const child  = val(b, 'child');
		const parent = val(b, 'parent');
		if (!child || !parent) continue;
		links.push({ source: localName(child), target: localName(parent), type: 'subClassOf', label: 'subClassOf' });
	}

	// Object property links (deduplicated)
	const seenOp = new Set();
	for (const b of objpropBindings) {
		const domain = val(b, 'domain');
		const rng    = val(b, 'range');
		const label  = val(b, 'label', '');
		if (!domain || !rng) continue;
		const src = localName(domain);
		const tgt = localName(rng);
		const key = `${src}|${tgt}|${label}`;
		if (seenOp.has(key)) continue;
		seenOp.add(key);
		links.push({ source: src, target: tgt, type: 'objectProperty', label: label || 'objectProperty' });
	}

	// hasSource / hasLocation use union domains so they won't appear from SPARQL.
	// Inject fallback links when nothing else covers them.
	const fallbacks = [
		['DisasterEvent',   'Source',          'hasSource'],
		['DisasterEvent',   'Location',        'hasLocation'],
		['IslandGroup',     'Country',         'isPartOf'],
		['Region',          'IslandGroup',     'isPartOf'],
		['Province',        'Region',          'isPartOf'],
		['Municipality',    'Province',        'isPartOf'],
		['City',            'Province',        'isPartOf'],
		['SubMunicipality', 'Municipality',    'isPartOf'],
		['Barangay',        'Municipality',    'isPartOf'],
	];
	for (const [src, tgt, lbl] of fallbacks) {
		if (!links.some(l => l.source === src && l.target === tgt)) {
			links.push({ source: src, target: tgt, type: 'objectProperty', label: lbl });
		}
	}

	return { nodes, links };
}

// ── Handler ───────────────────────────────────────────────────────────────────

export async function GET() {
	const [classRes, subclassofRes, objpropRes, datapropRes] = await Promise.all([
		executeSparql(GRAPH_CLASSES_QUERY),
		executeSparql(GRAPH_SUBCLASSOF_QUERY),
		executeSparql(GRAPH_OBJPROPS_QUERY),
		executeSparql(GRAPH_DATAPROPS_QUERY),
	]);

	return json(buildGraph(
		classRes.results.bindings,
		subclassofRes.results.bindings,
		objpropRes.results.bindings,
		datapropRes.results.bindings,
	));
}