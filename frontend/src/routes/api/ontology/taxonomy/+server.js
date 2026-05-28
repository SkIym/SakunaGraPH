import { json } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

// ── SPARQL query ──────────────────────────────────────────────────────────────

// DisasterType individuals are SKOS concepts stored as owl:NamedIndividual.
// Hierarchy uses skos:broader; top-level concepts (Natural, Technological)
// have no skos:broader and become direct children of the synthetic root node.
const TAXONOMY_QUERY = `
PREFIX : <https://sakuna.ph/>
PREFIX skos:   <http://www.w3.org/2004/02/skos/core#>

SELECT ?concept ?label ?definition ?parent WHERE {
    ?concept a :DisasterType .
    ?concept skos:prefLabel ?label .
    OPTIONAL { ?concept skos:definition ?definition }
    OPTIONAL { ?concept skos:broader ?parent }
}
`;

// ── Group assignment ──────────────────────────────────────────────────────────

// Group is assigned at certain "pivot" nodes and inherited downward.
// Children keep their parent's group unless they appear in this map.
const TAXONOMY_GROUP = {
	Natural:               'natural',
	Biological:            'biological',
	Climatological:        'climatological',
	Extraterrestrial:      'extraterrestrial',
	Geophysical:           'geophysical',
	Hydrological:          'hydrological',
	Meteorological:        'meteorological',
	Technological:         'tech',
	ArmedConflict:         'armedconflict',
	IndustrialAccident:    'industrial',
	MiscellaneousAccident: 'miscellaneous',
	Transport:             'transport',
};

// ── Helpers ───────────────────────────────────────────────────────────────────

function val(binding, key, fallback = null) {
	return binding[key]?.value ?? fallback;
}

function localName(iri) {
	return iri.includes('#') ? iri.split('#').at(-1) : iri.split('/').at(-1);
}

function buildTaxonomyTree(bindings) {
	const concepts = new Map();   // iri → node metadata
	const childrenOf = new Map(); // iri → [childIri, ...]

	for (const b of bindings) {
		const iri = val(b, 'concept');
		if (!iri) continue;
		const local     = localName(iri);
		const parentIri = val(b, 'parent');

		if (!concepts.has(iri)) {
			concepts.set(iri, {
				id:         local,
				label:      val(b, 'label', local),
				definition: val(b, 'definition', ''),
				parent:     parentIri,
			});
		}
		if (parentIri) {
			if (!childrenOf.has(parentIri)) childrenOf.set(parentIri, []);
			childrenOf.get(parentIri).push(iri);
		}
	}

	// Assign group top-down so children inherit from parent
	function assignGroup(iri, inherited) {
		const node = concepts.get(iri);
		node.group = TAXONOMY_GROUP[node.id] ?? inherited;
		for (const childIri of childrenOf.get(iri) ?? []) {
			assignGroup(childIri, node.group);
		}
	}

	const topLevel = [...concepts.entries()]
		.filter(([, c]) => !c.parent)
		.map(([iri]) => iri);

	for (const iri of topLevel) assignGroup(iri, 'natural');

	function buildNode(iri) {
		const n = concepts.get(iri);
		const node = {
			id:         n.id,
			label:      n.label,
			group:      n.group ?? 'natural',
			definition: n.definition,
		};
		const kids = childrenOf.get(iri);
		if (kids?.length) node.children = kids.map(buildNode);
		return node;
	}

	return {
		id:         'root',
		label:      'Disaster Types',
		group:      'root',
		definition: 'SakunaGraPH Disaster Type Classification Scheme based on the Emergency Events Database (EM-DAT) classification.',
		children:   topLevel.map(buildNode),
	};
}

// ── Handler ───────────────────────────────────────────────────────────────────

export async function GET() {
	const results = await executeSparql(TAXONOMY_QUERY);
	return json(buildTaxonomyTree(results.results.bindings));
}