const eventIri = 'https://sakuna.ph/test/event-1';

const eventDetails = {
	event: eventIri,
	name: 'Typhoon Salome',
	eventType: 'MajorEvent',
	startDate: '2023-08-14',
	endDate: '2023-08-16',
	remarks: ['Test fixture used by the frontend smoke suite.'],
	locations: [
		{
			uri: 'https://sakuna.ph/psgc/130000000',
			id: '130000000',
			label: 'National Capital Region',
		},
	],
	disasterTypes: [
		{
			uri: 'https://sakuna.ph/Typhoon',
			id: 'Typhoon',
			label: 'Typhoon',
		},
	],
	majorEvents: [],
	incidents: [],
	alternates: [],
	sources: [],
};

const analysisEvent = {
	event: eventIri,
	eventName: 'Typhoon Salome',
	eventType: 'MajorEvent',
	startDate: '2023-08-14',
	endDate: '2023-08-16',
	locations: [{ id: '130000000', label: 'National Capital Region' }],
	disasterTypes: [{ id: 'Typhoon', label: 'Typhoon' }],
	source: 'NDRRMC',
	alternates: [],
	impact: {
		dead: 2,
		injured: 4,
		missing: 0,
		affectedFamilies: 120,
		affectedPersons: 480,
		damageAmount: 250000,
		damageUnit: 'PHP',
		damageByUnit: [{ amount: 250000, unit: 'PHP' }],
	},
};

const json = (body) => ({
	status: 200,
	contentType: 'application/json',
	body: JSON.stringify(body),
});

function responseFor(pathname, request) {
	switch (pathname) {
		case '/api/sparql':
			return json({
				head: { vars: ['event', 'disasterType'] },
				results: {
					bindings: [
						{
							event: { type: 'uri', value: eventIri },
							disasterType: { type: 'literal', value: 'Typhoon' },
						},
					],
				},
			});
		case '/api/ask': {
			const query = request.postDataJSON()?.query ?? '';
			if (/failure/i.test(query)) {
				return {
					status: 503,
					contentType: 'application/json',
					body: JSON.stringify({ detail: 'The knowledge graph is unavailable.' }),
				};
			}
			return json({
				sparql: 'SELECT ?event WHERE { ?event a <https://sakuna.ph/DisasterEvent> } LIMIT 1',
				answer: /empty/i.test(query)
					? 'No matching events were found.'
					: 'One matching disaster event was found.',
				rows: /empty/i.test(query) ? [] : [{ event: eventIri }],
			});
		}
		case '/api/map/events':
			return json({
				events: [
					{
						event: eventIri,
						eventName: 'Typhoon Salome',
						startDate: '2023-08-14',
						locations: ['National Capital Region'],
						disasterTypes: ['Typhoon'],
						alternates: [],
						source: 'NDRRMC',
					},
				],
				majorCount: 1,
				incidentCount: 0,
			});
		case '/api/disasters/details':
			return json(eventDetails);
		case '/api/ontology/graph':
			return json({
				nodes: [
					{
						id: 'DisasterEvent',
						label: 'Disaster Event',
						group: 'core',
						definition: 'A disaster represented in the knowledge graph.',
						dataProperties: [],
					},
					{
						id: 'Impact',
						label: 'Impact',
						group: 'impact',
						definition: 'A reported effect of a disaster.',
						dataProperties: [],
					},
				],
				links: [
					{
						source: 'Impact',
						target: 'DisasterEvent',
						type: 'objectProperty',
						label: 'affects',
					},
				],
			});
		case '/api/ontology/taxonomy':
			return json({
				id: 'root',
				label: 'Disaster Type',
				group: 'root',
				definition: 'Disaster classification root.',
				children: [
					{
						id: 'natural',
						label: 'Natural',
						group: 'natural',
						definition: 'Natural hazards.',
						children: [],
					},
				],
			});
		case '/api/ontology/psgc':
			return json({
				nodes: [
					{
						id: '130000000',
						label: 'NCR',
						fullName: 'National Capital Region',
						level: 'Region',
						island: 'NCR',
						population: 13484462,
						psgcCode: '130000000',
					},
				],
				links: [],
			});
		case '/api/analysis/filter-options':
			return json({
				locations: { nodes: [], links: [] },
				disasterTypes: {
					id: 'root',
					label: 'Disaster Type',
					group: 'root',
					definition: 'Disaster classification root.',
					children: [],
				},
			});
		case '/api/analysis/events':
			return json({
				items: [analysisEvent],
				page: 1,
				page_size: 25,
				total: 1,
				sort_by: 'startDate',
				sort_dir: 'desc',
			});
		case '/api/analysis/summary':
			return json({
				record_count: 1,
				dead: 2,
				injured: 4,
				missing: 0,
				affectedFamilies: 120,
				affectedPersons: 480,
				damage: [{ amount: 250000, unit: 'PHP' }],
			});
		case '/api/analysis/disaster-counts':
			return json({
				group_by: 'taxonomy',
				items: [{ id: 'meteorological', label: 'Meteorological', count: 1 }],
			});
		case '/api/analysis/victim-trends':
			return json({ items: [{ year: 2023, dead: 2, injured: 4, missing: 0 }] });
		case '/api/analysis/region-rankings':
			return json({ items: [{ id: '130000000', label: 'NCR', count: 1 }] });
		case '/api/analysis/disaster-rankings':
			return json({ items: [{ id: 'Typhoon', label: 'Typhoon', dead: 2 }] });
		case '/api/analysis/damage-histogram':
			return json({ bins: [{ unit: 'PHP', lowerBound: 0, upperBound: 500000, count: 1 }] });
		case '/api/analysis/damage-vs-affected':
			return json({
				items: [
					{
						event: eventIri,
						eventName: 'Typhoon Salome',
						unit: 'PHP',
						damage: 250000,
						affectedFamilies: 120,
						affectedPersons: 480,
					},
				],
			});
		case '/api/analysis/calendar/years':
			return json({ items: [{ period: '2023', count: 1 }] });
		case '/api/analysis/calendar/months':
			return json({ items: [{ period: '2023-08', count: 1 }] });
		case '/api/analysis/calendar/days':
			return json({ items: [{ period: '2023-08-14', count: 1 }] });
		case '/api/analysis/timeline/category-stacks':
			return json({
				bucket: 'month_year',
				items: [
					{
						period: '2023-08',
						categories: [{ id: 'meteorological', label: 'Meteorological', count: 1 }],
					},
				],
			});
		case '/api/analysis/timeline/date-events':
			return json({ date_prefix: '2023-08-14', items: [analysisEvent] });
		default:
			return null;
	}
}

export async function mockApi(page, { delay = 0 } = {}) {
	await page.route(
		(url) => url.pathname.startsWith('/api/'),
		async (route) => {
			if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
			const response = responseFor(new URL(route.request().url()).pathname, route.request());
			if (response) await route.fulfill(response);
			else await route.fulfill({ status: 404, body: 'Unmocked API route' });
		},
	);
}

export async function gotoReady(page, path) {
	let response = await page.goto(path);
	// Vite can invalidate the first document while newly discovered dynamic imports are optimized.
	// A single retry keeps the dev-server suites deterministic without masking persistent failures.
	if (response && response.status() >= 500) {
		await page.waitForTimeout(100);
		response = await page.reload();
	}
	if (response && response.status() >= 500) {
		throw new Error(`Route ${path} returned HTTP ${response.status()}.`);
	}
	await page.locator('[data-app-hydrated="true"]').waitFor();
}
