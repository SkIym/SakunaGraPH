import { fireEvent, render, screen } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import EventDetails from '../../src/lib/components/EventDetails.svelte';
import EventTable from '../../src/lib/components/analysis/EventTable.svelte';
import PhilMap from '../../src/lib/components/map/PhilMap.svelte';
import { getDisasterDetails } from '../../src/lib/api/disasters.js';

vi.mock('../../src/lib/api/disasters.js', () => ({
	getDisasterDetails: vi.fn(),
}));

const eventDetails = {
	event: 'https://sakuna.ph/test/event-1',
	name: 'Typhoon Salome',
	eventType: 'MajorEvent',
	startDate: '2023-08-14',
	endDate: '2023-08-16',
	remarks: [],
	locations: [
		{
			uri: 'https://sakuna.ph/psgc/130000000',
			id: '130000000',
			label: 'National Capital Region',
		},
	],
	disasterTypes: [],
	majorEvents: [],
	incidents: [
		{
			uri: 'https://sakuna.ph/test/incident-1',
			name: 'Flooding incident',
			eventType: 'Incident',
			startDate: '2023-08-14',
		},
	],
	alternates: [],
	sources: [],
};

describe('visual interaction regressions', () => {
	beforeEach(() => {
		getDisasterDetails.mockResolvedValue(eventDetails);
	});

	it('uses stable chevrons for the event-detail disclosures', async () => {
		render(EventDetails, { event: eventDetails.event });

		const locations = await screen.findByRole('button', { name: /Locations affected/ });
		const incidents = screen.getByRole('button', { name: /Linked incidents/ });
		const locationsChevron = locations.querySelector('[aria-hidden="true"]');
		const incidentsChevron = incidents.querySelector('[aria-hidden="true"]');

		expect(locationsChevron?.querySelector('svg')).toBeInTheDocument();
		expect(incidentsChevron?.querySelector('svg')).toBeInTheDocument();
		expect(locationsChevron).not.toHaveClass('rotate-180');
		expect(incidentsChevron).not.toHaveClass('rotate-180');

		await fireEvent.click(locations);
		await fireEvent.click(incidents);

		expect(locations).toHaveAttribute('aria-expanded', 'true');
		expect(incidents).toHaveAttribute('aria-expanded', 'true');
		expect(locationsChevron).toHaveClass('rotate-180');
		expect(incidentsChevron).toHaveClass('rotate-180');
	});

	it('removes the default SVG outline while preserving a map-specific focus style', async () => {
		const onselect = vi.fn();
		render(PhilMap, {
			pathData: [
				{
					gid: 'province-1',
					regionPsgc: '130000000',
					name: 'National Capital Region',
					d: 'M 0 0 L 10 0 L 10 10 Z',
				},
			],
			onselect,
		});

		const area = screen.getByRole('button', { name: 'Select National Capital Region' });
		expect(area).toHaveClass('map-area', 'outline-none');

		await fireEvent.keyDown(area, { key: 'Enter' });
		expect(onselect).toHaveBeenCalledOnce();
	});

	it('vertically centers analysis table values', () => {
		render(EventTable, {
			items: [
				{
					event: eventDetails.event,
					eventName: eventDetails.name,
					locations: [{ id: '130000000', label: 'National Capital Region' }],
				},
			],
			columns: [
				{ id: 'eventName', label: 'Event', sortable: true },
				{ id: 'locations', label: 'Locations', sortable: false },
			],
			visibleColumns: new Set(['eventName', 'locations']),
		});

		const row = screen.getByRole('button', { name: `View details for ${eventDetails.name}` });
		expect(row).toHaveClass('align-middle');
		for (const cell of row.querySelectorAll('td')) {
			expect(cell).toHaveClass('align-middle');
		}
	});
});
