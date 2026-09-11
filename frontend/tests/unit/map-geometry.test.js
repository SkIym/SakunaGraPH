import { describe, expect, it, vi } from 'vitest';
import {
	loadMapGeometry,
	MAP_ASSET_URL,
	NCR_MAP_ASSET_URL,
} from '../../src/lib/features/map/geometry.js';

const fixture = {
	type: 'FeatureCollection',
	features: [
		{
			type: 'Feature',
			properties: {
				adm2_psgc: '1303900000',
				adm2_en: 'Manila',
				adm1_psgc: '1300000000',
			},
			geometry: {
				type: 'Polygon',
				coordinates: [
					[
						[120.9, 14.5],
						[121.1, 14.5],
						[121.1, 14.7],
						[120.9, 14.5],
					],
				],
			},
		},
	],
};

const ncrFixture = {
	type: 'FeatureCollection',
	overview: {
		type: 'Feature',
		properties: {
			psgc: '1300000000',
			name: 'National Capital Region (NCR)',
			areaType: 'region',
		},
		geometry: fixture.features[0].geometry,
	},
	features: [
		{
			type: 'Feature',
			properties: {
				psgc: '1380600000',
				name: 'City of Manila',
				areaType: 'city',
			},
			geometry: fixture.features[0].geometry,
		},
	],
};

describe('map geometry performance instrumentation', () => {
	it('uses the versioned asset URL and reports parse/projection metrics', async () => {
		const source = JSON.stringify(fixture);
		const ncrSource = JSON.stringify(ncrFixture);
		const fetchImpl = vi.fn().mockImplementation(async (url) => ({
			ok: true,
			text: vi.fn().mockResolvedValue(url === MAP_ASSET_URL ? source : ncrSource),
		}));

		const result = await loadMapGeometry({ fetchImpl });

		expect(fetchImpl).toHaveBeenCalledWith(MAP_ASSET_URL, { signal: undefined });
		expect(fetchImpl).toHaveBeenCalledWith(NCR_MAP_ASSET_URL, { signal: undefined });
		expect(result.pathData).toHaveLength(1);
		expect(result.pathData[0]).toMatchObject({ gid: '1300000000', areaType: 'ncr' });
		expect(result.ncrCityPathData).toHaveLength(1);
		expect(result.ncrCityPathData[0]).toMatchObject({ gid: '1380600000', areaType: 'city' });
		expect(result.metrics).toMatchObject({
			featureCount: 2,
			sourceBytes:
				new TextEncoder().encode(source).byteLength +
				new TextEncoder().encode(ncrSource).byteLength,
		});
		expect(result.metrics.parseMs).toBeGreaterThanOrEqual(0);
		expect(result.metrics.projectionMs).toBeGreaterThanOrEqual(0);
	});
});
