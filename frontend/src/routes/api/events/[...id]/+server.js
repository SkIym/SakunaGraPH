import { json, error } from '@sveltejs/kit';
import { executeSparql } from '$lib/server/sparql.js';

const P = `PREFIX :     <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
`;

export async function GET({ params }) {
	const id = params.id;
	if (!id) throw error(400, 'Missing event id');

	const iri = `https://sakuna.ph/${id}`;

	const coreQ = P + `
SELECT ?type ?name ?startDate ?endDate ?remarks
    (GROUP_CONCAT(DISTINCT ?dtype;   separator=",") AS ?disasterTypes)
    (GROUP_CONCAT(DISTINCT ?locLabel; separator="|") AS ?locations)
    (GROUP_CONCAT(DISTINCT ?srcLabel; separator="|") AS ?sources)
WHERE {
    BIND(<${iri}> AS ?ev)
    ?ev a ?type .
    OPTIONAL { ?ev :eventName    ?name      }
    OPTIONAL { ?ev :startDate    ?startDate }
    OPTIONAL { ?ev :endDate      ?endDate   }
    OPTIONAL { ?ev :remarks      ?remarks   }
    OPTIONAL { ?ev :hasDisasterType ?dtype  }
    OPTIONAL { ?ev :hasLocation ?loc . ?loc rdfs:label ?locLabel }
    OPTIONAL {
        ?ev prov:wasDerivedFrom ?src .
        BIND(REPLACE(STR(?src), "https://sakuna.ph/", "") AS ?srcLabel)
    }
}
GROUP BY ?type ?name ?startDate ?endDate ?remarks`;

	const impactQ = P + `
SELECT
    (SUM(COALESCE(?af,  0)) AS ?affectedFamilies)
    (SUM(COALESCE(?ap,  0)) AS ?affectedPersons)
    (SUM(COALESCE(?df,  0)) AS ?displacedFamilies)
    (SUM(COALESCE(?dp,  0)) AS ?displacedPersons)
    (SUM(COALESCE(?tdh, 0)) AS ?totallyDamaged)
    (SUM(COALESCE(?pdh, 0)) AS ?partiallyDamaged)
    (SUM(COALESCE(?ec,  0)) AS ?evacuationCenters)
WHERE {
    BIND(<${iri}> AS ?ev)
    OPTIONAL {
        ?ev :hasAffectedPopulation ?pop .
        OPTIONAL { ?pop :affectedFamilies  ?af }
        OPTIONAL { ?pop :affectedPersons   ?ap }
        OPTIONAL { ?pop :displacedFamilies ?df }
        OPTIONAL { ?pop :displacedPersons  ?dp }
    }
    OPTIONAL {
        ?ev :hasHousingDamage ?hd .
        OPTIONAL { ?hd :totallyDamagedHouses   ?tdh }
        OPTIONAL { ?hd :partiallyDamagedHouses ?pdh }
    }
    OPTIONAL {
        ?ev :hasPreemptiveEvacuation ?pe .
        OPTIONAL { ?pe :evacuationCenters ?ec }
    }
}`;

	const casualtiesQ = P + `
SELECT ?casType (SUM(?cnt) AS ?total)
    (GROUP_CONCAT(DISTINCT ?cause; separator="|") AS ?causes)
WHERE {
    BIND(<${iri}> AS ?ev)
    ?ev :hasCasualties ?cas .
    ?cas :casualtyType ?casType ;
         :casualtyCount ?cnt .
    OPTIONAL { ?cas :casualtyCause ?cause }
}
GROUP BY ?casType`;

	const alternatesQ = P + `
SELECT DISTINCT ?alt ?altName ?altType
WHERE {
    BIND(<${iri}> AS ?ev)
    { ?ev prov:alternateOf ?alt } UNION { ?alt prov:alternateOf ?ev }
    FILTER(?alt != ?ev)
    ?alt a ?altType .
    OPTIONAL { ?alt :eventName ?altName }
}
LIMIT 20`;

	const [coreRes, impactRes, casRes, altRes] = await Promise.all([
		executeSparql(coreQ),
		executeSparql(impactQ),
		executeSparql(casualtiesQ),
		executeSparql(alternatesQ),
	]);

	const core    = coreRes.results.bindings[0] ?? null;
	const impact  = impactRes.results.bindings[0] ?? null;
	const casualties = casRes.results.bindings;
	const alternates = altRes.results.bindings;

	if (!core) throw error(404, `Event not found: ${iri}`);

	function val(b, k) { return b?.[k]?.value ?? null; }
	function num(b, k) { const v = val(b, k); return v !== null ? parseInt(v, 10) : 0; }

	return json({
		iri,
		type:       val(core, 'type')?.replace('https://sakuna.ph/', '') ?? null,
		name:       val(core, 'name'),
		startDate:  val(core, 'startDate')?.split('T')[0] ?? null,
		endDate:    val(core, 'endDate')?.split('T')[0] ?? null,
		remarks:    val(core, 'remarks'),
		disasterTypes: (val(core, 'disasterTypes') ?? '').split(',').map(s => s.replace('https://sakuna.ph/', '').trim()).filter(Boolean),
		locations:  (val(core, 'locations') ?? '').split('|').filter(Boolean),
		sources:    (val(core, 'sources') ?? '').split('|').filter(Boolean),
		impact: {
			affectedFamilies:  num(impact, 'affectedFamilies'),
			affectedPersons:   num(impact, 'affectedPersons'),
			displacedFamilies: num(impact, 'displacedFamilies'),
			displacedPersons:  num(impact, 'displacedPersons'),
			totallyDamaged:    num(impact, 'totallyDamaged'),
			partiallyDamaged:  num(impact, 'partiallyDamaged'),
			evacuationCenters: num(impact, 'evacuationCenters'),
		},
		casualties: casualties.map(r => ({
			type:   val(r, 'casType') ?? '',
			total:  num(r, 'total'),
			causes: (val(r, 'causes') ?? '').split('|').filter(Boolean),
		})),
		alternates: alternates.map(r => ({
			iri:  val(r, 'alt'),
			name: val(r, 'altName'),
			type: val(r, 'altType')?.replace('https://sakuna.ph/', '') ?? null,
		})),
	});
}
