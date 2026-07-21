import { describe, expect, it, vi } from 'vitest';
import { createMapEventQuery } from '../../src/lib/features/map/eventQuery.svelte.js';
import { groupEventsByAlternates } from '../../src/lib/features/map/groupEvents.js';

describe('map feature state', () => {
	it('groups transitive alternates under the earliest representative', () => {
		const events = [
			{ event: 'b', startDate: '2024-01-02', alternates: ['c'] },
			{ event: 'a', startDate: '2024-01-01', alternates: ['b'] },
			{ event: 'c', startDate: '2024-01-03', alternates: [] },
			{ event: 'standalone', startDate: '2024-02-01' },
		];

		expect(groupEventsByAlternates(events)).toEqual([
			{ row: events[1], subs: [events[0], events[2]] },
			{ row: events[3], subs: [] },
		]);
	});

	it('owns map endpoint parameters and response state', async () => {
		const fetchEvents = vi.fn().mockResolvedValue({
			events: [{ event: 'event-1', startDate: '2024-01-01' }],
			majorCount: 3,
			incidentCount: 7,
		});
		const query = createMapEventQuery({ fetchEvents });
		const controller = new AbortController();

		await query.load({
			selected: { type: 'region', psgc: '130000000' },
			mode: 'major',
			page: 2,
			signal: controller.signal,
		});

		expect(fetchEvents).toHaveBeenCalledWith(
			{ scope: 'region', id: '130000000', mode: 'major', page: '2' },
			{ signal: controller.signal },
		);
		expect(query.results).toHaveLength(1);
		expect(query.countFor('major')).toBe(3);
		expect(query.countFor('incidents')).toBe(7);
	});
});
