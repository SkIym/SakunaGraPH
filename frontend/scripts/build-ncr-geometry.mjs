import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const NCR_PSGC = '1300000000';
const SOURCE_ROOT =
	'https://raw.githubusercontent.com/faeldon/philippines-json-maps/refs/heads/master/2023/geojson';
const SOURCE_URLS = [
	`${SOURCE_ROOT}/country/medres/country.0.01.json`,
	...['1303900000', '1307400000', '1307500000', '1307600000'].map(
		(code) => `${SOURCE_ROOT}/provdists/medres/municities-provdist-${code}.0.01.json`,
	),
];

function psgc(value) {
	return String(value ?? '').padStart(10, '0');
}

function signedRingArea(ring) {
	return (
		ring.reduce((area, point, index) => {
			const next = ring[(index + 1) % ring.length];
			return area + point[0] * next[1] - next[0] * point[1];
		}, 0) / 2
	);
}

// d3-geo expects small spherical polygon exteriors clockwise. The upstream
// GeoJSON follows RFC 7946's opposite winding, so normalize it once here.
function rewindRing(ring, clockwise) {
	const isClockwise = signedRingArea(ring) < 0;
	return isClockwise === clockwise ? ring : [...ring].reverse();
}

function rewindPolygon(rings) {
	return rings.map((ring, index) => rewindRing(ring, index === 0));
}

function rewindGeometry(geometry) {
	if (geometry.type === 'Polygon') {
		return { ...geometry, coordinates: rewindPolygon(geometry.coordinates) };
	}
	if (geometry.type === 'MultiPolygon') {
		return { ...geometry, coordinates: geometry.coordinates.map(rewindPolygon) };
	}
	throw new Error(`Unsupported NCR geometry type: ${geometry.type}`);
}

async function downloadGeoJson(url) {
	const response = await fetch(url);
	if (!response.ok) throw new Error(`${response.status} while downloading ${url}`);
	return response.json();
}

const [country, ...districts] = await Promise.all(SOURCE_URLS.map(downloadGeoJson));
const overview = country.features.find(
	(feature) => psgc(feature.properties?.adm1_psgc) === NCR_PSGC,
);
if (!overview) throw new Error('The source country map does not contain an NCR region feature.');

const localities = districts
	.flatMap((collection) => collection.features)
	.map((feature) => ({
		type: 'Feature',
		id: psgc(feature.properties?.adm3_psgc ?? feature.id),
		properties: {
			psgc: psgc(feature.properties?.adm3_psgc ?? feature.id),
			name: feature.properties?.adm3_en,
			areaType: ['mun', 'municipality'].includes(
				String(feature.properties?.geo_level).toLowerCase(),
			)
				? 'municipality'
				: 'city',
			regionPsgc: NCR_PSGC,
		},
		geometry: rewindGeometry(feature.geometry),
	}))
	.sort((left, right) => left.properties.name.localeCompare(right.properties.name));

if (localities.length !== 17) {
	throw new Error(`Expected 17 NCR localities, received ${localities.length}.`);
}

const output = {
	type: 'FeatureCollection',
	name: 'National Capital Region city and municipality boundaries',
	metadata: {
		regionPsgc: NCR_PSGC,
		snapshot: '2023-12-31',
		source: 'faeldon/philippines-json-maps',
		license: 'MIT',
		sourceUrls: SOURCE_URLS,
	},
	overview: {
		type: 'Feature',
		id: NCR_PSGC,
		properties: {
			psgc: NCR_PSGC,
			name: 'National Capital Region (NCR)',
			areaType: 'region',
			regionPsgc: NCR_PSGC,
		},
		geometry: rewindGeometry(overview.geometry),
	},
	features: localities,
};

const scriptDirectory = fileURLToPath(new URL('.', import.meta.url));
const destination = resolve(scriptDirectory, '../static/data/ncr-cities.geojson');
await writeFile(destination, `${JSON.stringify(output)}\n`, 'utf8');
console.log(`Wrote ${localities.length} NCR localities to ${destination}`);
