import { describe, expect, it } from 'vitest';
import { ASK_MODES, enabledFeatureFlag, preferredAskMode } from '../../src/lib/api/ask.js';

describe('ask transport feature flag', () => {
	it('keeps legacy mode as the default', () => {
		expect(preferredAskMode()).toBe(ASK_MODES.LEGACY);
	});

	it('accepts explicit public boolean flag values', () => {
		for (const value of ['1', 'true', 'YES', 'on']) expect(enabledFeatureFlag(value)).toBe(true);
		for (const value of [undefined, '', '0', 'false', 'off']) {
			expect(enabledFeatureFlag(value)).toBe(false);
		}
	});
});
