import { describe, expect, it } from 'vitest';
import { mergeAnalysisParams } from '../../src/lib/features/analysis/params.js';

describe('analysis query parameters', () => {
	it('preserves repeated filters while adding endpoint-specific values', () => {
		const params = mergeAnalysisParams('location_ids=01&location_ids=02', {
			year: 2024,
			include_impacts: false,
		});

		expect(params.getAll('location_ids')).toEqual(['01', '02']);
		expect(params.get('year')).toBe('2024');
		expect(params.get('include_impacts')).toBe('false');
	});
});
