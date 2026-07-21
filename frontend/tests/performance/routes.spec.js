import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from '../e2e/fixtures/api-mocks.js';

const budget = JSON.parse(
	readFileSync(resolve(process.cwd(), 'performance-budgets.json'), 'utf8'),
).runtime;

const routes = [
	['/', { role: 'button', name: 'Run Query' }],
	['/map', { role: 'button', name: /^Select / }],
	['/ontology', { text: 'Legend' }],
	['/ask', { role: 'heading', name: 'Ask SakunaGraPH' }],
	['/analysis', { role: 'heading', name: 'Current scope' }],
	['/analysis/events', { role: 'heading', name: 'Disaster event records' }],
	['/analysis/metrics', { role: 'heading', name: 'Metrics dashboard' }],
	['/analysis/timeline', { role: 'heading', name: 'Timeline and date analysis' }],
];

test.beforeEach(async ({ page }) => {
	await mockApi(page);
});

for (const [route, ready] of routes) {
	test(`${route} stays within initial-render and transfer budgets`, async ({ page }) => {
		await gotoReady(page, route);
		if (ready.role) await page.getByRole(ready.role, { name: ready.name }).first().waitFor();
		else await page.getByText(ready.text, { exact: true }).first().waitFor();

		const metrics = await page.evaluate(() => {
			const navigation = performance.getEntriesByType('navigation')[0];
			const fcp = performance.getEntriesByName('first-contentful-paint')[0];
			const resources = performance.getEntriesByType('resource');
			return {
				hydratedMs: performance.now(),
				firstContentfulPaintMs: fcp?.startTime ?? navigation?.domContentLoadedEventEnd ?? 0,
				jsTransferBytes: resources
					.filter((entry) => entry.name.includes('/_app/') && entry.name.endsWith('.js'))
					.reduce((total, entry) => total + entry.transferSize, 0),
				cssTransferBytes: resources
					.filter((entry) => entry.name.includes('/_app/') && entry.name.endsWith('.css'))
					.reduce((total, entry) => total + entry.transferSize, 0),
			};
		});

		expect(metrics.hydratedMs).toBeLessThanOrEqual(budget.hydratedMs);
		expect(metrics.firstContentfulPaintMs).toBeLessThanOrEqual(budget.firstContentfulPaintMs);
		expect(metrics.jsTransferBytes).toBeLessThanOrEqual(budget.initialJsTransferBytes);
		expect(metrics.cssTransferBytes).toBeLessThanOrEqual(budget.initialCssTransferBytes);
	});
}

test('map parsing, projection, and first SVG render are measured', async ({ page }) => {
	await gotoReady(page, '/map');
	await page
		.getByRole('button', { name: /^Select / })
		.first()
		.waitFor();
	const measures = await page.evaluate(() =>
		Object.fromEntries(
			performance
				.getEntriesByType('measure')
				.filter((entry) => entry.name.startsWith('sakunagraph:map-'))
				.map((entry) => [entry.name, entry.duration]),
		),
	);

	expect(measures['sakunagraph:map-parse']).toBeLessThanOrEqual(budget.mapParseMs);
	expect(measures['sakunagraph:map-projection']).toBeLessThanOrEqual(budget.mapProjectionMs);
	expect(measures['sakunagraph:map-initial-render']).toBeLessThanOrEqual(budget.mapInitialRenderMs);
});

test('production caching distinguishes hashed assets from updateable map data', async ({
	page,
}) => {
	const response = await page.goto('/');
	expect(response?.ok()).toBe(true);
	const immutableAsset = await page
		.locator('link[rel="stylesheet"][href*="/_app/immutable/"]')
		.first()
		.getAttribute('href');
	const assetResponse = await page.request.get(immutableAsset);
	expect(assetResponse.headers()['cache-control']).toContain('max-age=31536000');
	expect(assetResponse.headers()['cache-control']).toContain('immutable');

	const mapResponse = await page.request.get('/data/regions.geojson?v=2026-06-23');
	expect(mapResponse.ok()).toBe(true);
	expect(mapResponse.headers()['cache-control'] ?? '').not.toContain('immutable');
});
