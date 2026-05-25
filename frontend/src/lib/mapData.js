// Philippine map metadata and SPARQL query builders

export const REGION_LABELS = {
	'0100000000': 'Region I — Ilocos Region',
	'0200000000': 'Region II — Cagayan Valley',
	'0300000000': 'Region III — Central Luzon',
	'0400000000': 'Region IV-A — CALABARZON',
	'0500000000': 'Region V — Bicol Region',
	'0600000000': 'Region VI — Western Visayas',
	'0700000000': 'Region VII — Central Visayas',
	'0800000000': 'Region VIII — Eastern Visayas',
	'0900000000': 'Region IX — Zamboanga Peninsula',
	'1000000000': 'Region X — Northern Mindanao',
	'1100000000': 'Region XI — Davao Region',
	'1200000000': 'Region XII — SOCCSKSARGEN',
	'1300000000': 'National Capital Region (NCR)',
	'1400000000': 'Cordillera Administrative Region (CAR)',
	'1500000000': 'Region XV — BARMM',
	'1600000000': 'Region XIII — Caraga',
	'1700000000': 'Region IV-B — MIMAROPA'
};

// GADM concatenates words; map known tricky cases to proper display names
const PROV_DISPLAY = {
	AgusandelNorte: 'Agusan del Norte',
	AgusandelSur: 'Agusan del Sur',
	SurigaodelNorte: 'Surigao del Norte',
	SurigaodelSur: 'Surigao del Sur',
	LanaodelNorte: 'Lanao del Norte',
	LanaodelSur: 'Lanao del Sur',
	DavaodelNorte: 'Davao del Norte',
	DavaodelSur: 'Davao del Sur',
	DinagatIslands: 'Dinagat Islands',
	MetropolitanManila: 'Metro Manila (NCR)',
	MisamisOccidental: 'Misamis Occidental',
	MisamisOriental: 'Misamis Oriental',
	DavaoOriental: 'Davao Oriental',
	CompostelaValley: 'Compostela Valley',
	NorthCotabato: 'North Cotabato',
	SouthCotabato: 'South Cotabato',
	SultanKudarat: 'Sultan Kudarat',
	IlocosNorte: 'Ilocos Norte',
	IlocosSur: 'Ilocos Sur',
	LaUnion: 'La Union',
	MountainProvince: 'Mountain Province',
	NuevaVizcaya: 'Nueva Vizcaya',
	NuevaEcija: 'Nueva Ecija',
	OccidentalMindoro: 'Occidental Mindoro',
	OrientalMindoro: 'Oriental Mindoro',
	NorthernSamar: 'Northern Samar',
	EasternSamar: 'Eastern Samar',
	WesternSamar: 'Western Samar',
	SouthernLeyte: 'Southern Leyte',
	ZamboangadelNorte: 'Zamboanga del Norte',
	ZamboangadelSur: 'Zamboanga del Sur',
	ZamboangaSibugay: 'Zamboanga Sibugay'
};

export function formatProvName(gadmName) {
	if (PROV_DISPLAY[gadmName]) return PROV_DISPLAY[gadmName];
	// Insert space before capital letters that follow lowercase letters
	return gadmName.replace(/([a-z])([A-Z])/g, '$1 $2');
}

/** Extract the 10-digit PSGC region code from a GADM CC_1 field */
export function regionPsgcFromCC1(cc1) {
	const n = parseInt(cc1, 10);
	const regionNum = Math.floor(n / 100);
	return String(regionNum).padStart(2, '0') + '00000000';
}

const P = `PREFIX :    <https://sakuna.ph/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
`;

/** Build paginated data query for a selected region (by PSGC code) */
export function buildRegionQuery(psgc, offset = 0, limit = 10) {
	return (
		P +
		`SELECT DISTINCT ?event ?eventName ?disasterType ?startDate ?locLabel
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* :${psgc} .
  OPTIONAL { ?event    :eventName  ?eventName }
  OPTIONAL { ?location rdfs:label  ?locLabel  }
}
ORDER BY DESC(?startDate)
LIMIT ${limit}
OFFSET ${offset}`
	);
}

/** Build count query for a selected region */
export function buildRegionCountQuery(psgc) {
	return (
		P +
		`SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {
  ?event a :DisasterEvent ;
         :hasLocation ?location .
  ?location :isPartOf* :${psgc} .
}`
	);
}

/** Build paginated data query for a selected province (by name matching) */
export function buildProvinceQuery(gadmName, offset = 0, limit = 10) {
	const normalized = gadmName.toLowerCase().replace(/[^a-z0-9]/g, '');
	return (
		P +
		`SELECT DISTINCT ?event ?eventName ?disasterType ?startDate ?locLabel
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType ;
         :startDate ?startDate ;
         :hasLocation ?location .
  ?location :isPartOf* ?prov .
  ?prov a :Province ; rdfs:label ?provLabel .
  FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalized}")
  OPTIONAL { ?event    :eventName  ?eventName }
  OPTIONAL { ?location rdfs:label  ?locLabel  }
}
ORDER BY DESC(?startDate)
LIMIT ${limit}
OFFSET ${offset}`
	);
}

/** Build count query for a province */
export function buildProvinceCountQuery(gadmName) {
	const normalized = gadmName.toLowerCase().replace(/[^a-z0-9]/g, '');
	return (
		P +
		`SELECT (COUNT(DISTINCT ?event) AS ?count)
WHERE {
  ?event a :DisasterEvent ;
         :hasLocation ?location .
  ?location :isPartOf* ?prov .
  ?prov a :Province ; rdfs:label ?provLabel .
  FILTER(REPLACE(LCASE(STR(?provLabel)), "[^a-z0-9]", "") = "${normalized}")
}`
	);
}

/** Extract a short display value from a SPARQL binding value */
export function bindingDisplay(val) {
	if (!val) return '—';
	if (val.type === 'literal') return val.value;
	if (val.type === 'uri') {
		const parts = val.value.split(/[/#]/);
		return parts[parts.length - 1] || val.value;
	}
	return val.value;
}

/** Insert spaces in a camelCase disaster type IRI local name */
export function formatDisasterType(uriOrLocal) {
	const local = uriOrLocal.split(/[/#]/).pop() || uriOrLocal;
	return local.replace(/([a-z])([A-Z])/g, '$1 $2');
}
