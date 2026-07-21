import { describe, expect, it } from 'vitest';
import { formatDisasterType, formatProvName, normalizePsgcCode } from '../../src/lib/mapData.js';

describe('map display helpers', () => {
	it('normalizes PSGC codes from numeric GeoJSON values', () => {
		expect(normalizePsgcCode(130000000)).toBe('0130000000');
	});

	it('keeps user-facing geography and disaster labels readable', () => {
		expect(formatProvName('MetropolitanManila')).toBe('Metro Manila (NCR)');
		expect(formatDisasterType('https://sakuna.ph/TropicalCyclone')).toBe('Tropical Cyclone');
	});
});
