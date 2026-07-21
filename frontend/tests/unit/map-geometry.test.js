import { describe, expect, it, vi } from 'vitest';
import { loadMapGeometry, MAP_ASSET_URL } from '../../src/lib/features/map/geometry.js';

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

describe('map geometry performance instrumentation', () => {
	it('uses the versioned asset URL and reports parse/projection metrics', async () => {
		const source = JSON.stringify(fixture);
		const fetchImpl = vi.fn().mockResolvedValue({
			ok: true,
			text: vi.fn().mockResolvedValue(source),
		});

		const result = await loadMapGeometry({ fetchImpl });

		expect(fetchImpl).toHaveBeenCalledWith(MAP_ASSET_URL, { signal: undefined });
		expect(result.pathData).toHaveLength(1);
		expect(result.metrics).toMatchObject({
			featureCount: 1,
			sourceBytes: new TextEncoder().encode(source).byteLength,
		});
		expect(result.metrics.parseMs).toBeGreaterThanOrEqual(0);
		expect(result.metrics.projectionMs).toBeGreaterThanOrEqual(0);
	});
});
