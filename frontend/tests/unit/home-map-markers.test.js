import { describe, expect, it, vi } from 'vitest';
import { buildHomeRecordMarkers } from '../../src/lib/features/home/mapMarkers.js';

describe('home map record markers', () => {
	it('places only the highest real positive region counts and scales their markers', () => {
		const pathGenerator = {
			centroid: vi.fn().mockReturnValueOnce([100, 200]).mockReturnValueOnce([300, 400]),
		};
		const pathData = [
			{ regionPsgc: '1300000000', feature: { id: 'ncr' } },
			{ regionPsgc: '0300000000', feature: { id: 'central-luzon' } },
		];

		const markers = buildHomeRecordMarkers({
			rankings: [
				{ id: '0300000000', label: 'Central Luzon', count: 12 },
				{ id: '130000000', label: 'NCR', count: 48 },
				{ id: '0400000000', label: 'CALABARZON', count: 0 },
			],
			pathData,
			pathGenerator,
			limit: 2,
		});

		expect(markers).toHaveLength(2);
		expect(markers.map(({ label, count }) => ({ label, count }))).toEqual([
			{ label: 'NCR', count: 48 },
			{ label: 'Central Luzon', count: 12 },
		]);
		expect(markers[0]).toMatchObject({ x: 100, y: 200, radius: 8 });
		expect(markers[1].radius).toBeLessThan(markers[0].radius);
	});

	it('omits markers when geometry or ranking data cannot support them', () => {
		expect(buildHomeRecordMarkers()).toEqual([]);
		expect(
			buildHomeRecordMarkers({
				rankings: [{ id: '1300000000', count: 2 }],
				pathData: [],
				pathGenerator: { centroid: () => [Number.NaN, Number.NaN] },
			}),
		).toEqual([]);
	});
});
