import { normalizePsgcCode } from '../../mapData.js';

export const MAP_WIDTH = 700;
export const MAP_HEIGHT = 800;
export const FULL_MAP_VIEW_BOX = `0 0 ${MAP_WIDTH} ${MAP_HEIGHT}`;
export const MAP_ASSET_VERSION = '2026-06-23';
export const MAP_ASSET_URL = `/data/regions.geojson?v=${MAP_ASSET_VERSION}`;
export const NCR_REGION_PSGC = '1300000000';
export const NCR_MAP_ASSET_VERSION = '2026-09-11';
export const NCR_MAP_ASSET_URL = `/data/ncr-cities.geojson?v=${NCR_MAP_ASSET_VERSION}`;

function recordMeasurement(name, start, end) {
	try {
		globalThis.performance?.measure(name, { start, duration: end - start });
	} catch {
		// Measurements are diagnostics only and must never prevent map rendering.
	}
}

export async function loadMapGeometry({ signal, fetchImpl = globalThis.fetch } = {}) {
	const startedAt = globalThis.performance?.now?.() ?? Date.now();
	const { geoMercator, geoPath } = await import('d3-geo');
	// The GeoJSON files are versioned static assets, not backend operations.
	const downloadStartedAt = globalThis.performance?.now?.() ?? Date.now();
	const [response, ncrResponse] = await Promise.all([
		fetchImpl(MAP_ASSET_URL, { signal }),
		fetchImpl(NCR_MAP_ASSET_URL, { signal }),
	]);
	if (!response.ok) throw new Error(`HTTP ${response.status}`);
	if (!ncrResponse.ok) throw new Error(`HTTP ${ncrResponse.status}`);
	const [source, ncrSource] = await Promise.all([response.text(), ncrResponse.text()]);
	const downloadFinishedAt = globalThis.performance?.now?.() ?? Date.now();
	const parseStartedAt = globalThis.performance?.now?.() ?? Date.now();
	const geojson = JSON.parse(source);
	const ncrGeojson = JSON.parse(ncrSource);
	const parseFinishedAt = globalThis.performance?.now?.() ?? Date.now();

	const projectionStartedAt = globalThis.performance?.now?.() ?? Date.now();
	const projection = geoMercator().fitSize([MAP_WIDTH, MAP_HEIGHT], geojson);
	const pathGenerator = geoPath(projection);
	const provincePathData = geojson.features
		.filter((feature) => normalizePsgcCode(feature.properties.adm1_psgc) !== NCR_REGION_PSGC)
		.map((feature) => ({
			d: pathGenerator(feature),
			gid: normalizePsgcCode(feature.properties.adm2_psgc),
			name: feature.properties.adm2_en,
			regionPsgc: normalizePsgcCode(feature.properties.adm1_psgc),
			areaType: 'province',
			feature,
		}));
	const ncrOverviewFeature = ncrGeojson.overview;
	const ncrOverview = {
		d: pathGenerator(ncrOverviewFeature),
		gid: NCR_REGION_PSGC,
		name: ncrOverviewFeature.properties.name,
		regionPsgc: NCR_REGION_PSGC,
		areaType: 'ncr',
		feature: ncrOverviewFeature,
	};
	const pathData = [...provincePathData, ncrOverview];
	const ncrCityPathData = ncrGeojson.features.map((feature) => ({
		d: pathGenerator(feature),
		gid: normalizePsgcCode(feature.properties.psgc),
		name: feature.properties.name,
		regionPsgc: NCR_REGION_PSGC,
		areaType: feature.properties.areaType,
		feature,
	}));

	const tightViewBox = viewBoxFor(pathData, pathGenerator, 8);
	const ncrViewBox = viewBoxFor(ncrCityPathData, pathGenerator, 3);
	const projectionFinishedAt = globalThis.performance?.now?.() ?? Date.now();
	const metrics = {
		downloadMs: downloadFinishedAt - downloadStartedAt,
		parseMs: parseFinishedAt - parseStartedAt,
		projectionMs: projectionFinishedAt - projectionStartedAt,
		totalMs: projectionFinishedAt - startedAt,
		featureCount: pathData.length + ncrCityPathData.length,
		sourceBytes:
			new TextEncoder().encode(source).byteLength + new TextEncoder().encode(ncrSource).byteLength,
	};
	recordMeasurement('sakunagraph:map-download', downloadStartedAt, downloadFinishedAt);
	recordMeasurement('sakunagraph:map-parse', parseStartedAt, parseFinishedAt);
	recordMeasurement('sakunagraph:map-projection', projectionStartedAt, projectionFinishedAt);

	return { pathData, pathGenerator, tightViewBox, ncrCityPathData, ncrViewBox, metrics };
}

function viewBoxFor(items, pathGenerator, padding) {
	let xMin = Infinity;
	let yMin = Infinity;
	let xMax = -Infinity;
	let yMax = -Infinity;
	for (const item of items) {
		try {
			const [[x0, y0], [x1, y1]] = pathGenerator.bounds(item.feature);
			xMin = Math.min(xMin, x0);
			yMin = Math.min(yMin, y0);
			xMax = Math.max(xMax, x1);
			yMax = Math.max(yMax, y1);
		} catch {}
	}
	if (![xMin, yMin, xMax, yMax].every(Number.isFinite)) return FULL_MAP_VIEW_BOX;
	return `${xMin - padding} ${yMin - padding} ${xMax - xMin + padding * 2} ${yMax - yMin + padding * 2}`;
}

export function detailViewBoxFor({ selected, view, pathData, pathGenerator }) {
	if (!selected || !pathGenerator || pathData.length === 0) return null;
	const selectionKey = view === 'regions' ? selected.psgc : selected.id;
	const items = pathData.filter((item) =>
		view === 'regions' ? item.regionPsgc === selectionKey : item.gid === selectionKey,
	);
	if (items.length === 0) return null;

	let xMin = Infinity;
	let yMin = Infinity;
	let xMax = -Infinity;
	let yMax = -Infinity;
	for (const item of items) {
		try {
			const [[x0, y0], [x1, y1]] = pathGenerator.bounds(item.feature);
			xMin = Math.min(xMin, x0);
			yMin = Math.min(yMin, y0);
			xMax = Math.max(xMax, x1);
			yMax = Math.max(yMax, y1);
		} catch {}
	}
	const padding = 25;
	return `${xMin - padding} ${yMin - padding} ${xMax - xMin + padding * 2} ${yMax - yMin + padding * 2}`;
}
