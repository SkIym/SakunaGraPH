import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from '../e2e/fixtures/api-mocks.js';

const routes = [
	'/',
	'/map',
	'/ontology',
	'/ask',
	'/analysis',
	'/analysis/events',
	'/analysis/metrics',
	'/analysis/timeline',
];

test.beforeEach(async ({ page }) => {
	await mockApi(page);
});

for (const route of routes) {
	test(`${route} has no automatically detectable WCAG A/AA violations`, async ({ page }) => {
		await gotoReady(page, route);
		await page.locator('body').waitFor();

		const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();

		expect(results.violations).toEqual([]);
	});
}

test('query results trap focus and restore it to the run control', async ({ page }) => {
	await gotoReady(page, '/');
	const runButton = page.getByRole('button', { name: 'Run Query' });
	await runButton.click();
	const closeButton = page.getByRole('button', { name: 'Close results' });
	await expect(closeButton).toBeFocused();
	await page.keyboard.press('Shift+Tab');
	await expect(closeButton).toBeFocused();
	await page.keyboard.press('Escape');
	await expect(runButton).toBeFocused();
});

test('event details trap focus and restore it to the selected record', async ({ page }) => {
	await gotoReady(page, '/map');
	const area = page.getByRole('button', { name: /^Select / }).first();
	await area.focus();
	await page.keyboard.press('Enter');
	const eventRow = page.getByRole('button', { name: 'View details for Typhoon Salome' });
	await eventRow.focus();
	await page.keyboard.press('Enter');
	const closeButton = page.getByRole('button', { name: 'Close event details' }).last();
	await expect(closeButton).toBeFocused();
	await page.keyboard.press('Escape');
	await expect(eventRow).toBeFocused();
});

test('mobile filters trap focus and restore it to the opener', async ({ page }) => {
	await page.setViewportSize({ width: 390, height: 844 });
	await gotoReady(page, '/analysis/events');
	const openButton = page.getByRole('button', { name: 'Open analysis filters' });
	await openButton.click();
	const closeButton = page.getByRole('button', { name: 'Close filters' }).last();
	await expect(closeButton).toBeFocused();
	await page.keyboard.press('Escape');
	await expect(openButton).toBeFocused();
});

test('ontology graphs expose their nodes to keyboard users', async ({ page }) => {
	await gotoReady(page, '/ontology');
	const node = page
		.getByLabel('Interactive core ontology class graph')
		.locator('[role="button"]')
		.first();
	await node.focus();
	await page.keyboard.press('Enter');
	await expect(page.getByRole('heading', { name: 'Disaster Event' })).toBeVisible();
});

test('forced colors preserve interactive map, graph, and chart marks', async ({ page }) => {
	await page.emulateMedia({ forcedColors: 'active', reducedMotion: 'reduce' });

	await gotoReady(page, '/map');
	const mapArea = page.getByRole('button', { name: /^Select / }).first();
	await mapArea.focus();
	await expect(mapArea).toBeFocused();
	expect(await mapArea.evaluate((element) => getComputedStyle(element).stroke)).not.toBe('none');

	await gotoReady(page, '/ontology');
	const graphNode = page
		.getByLabel('Interactive core ontology class graph')
		.locator('[role="button"]')
		.first();
	await graphNode.focus();
	await page.keyboard.press('Enter');
	await expect(graphNode).toHaveAttribute('aria-pressed', 'true');
	expect(await graphNode.evaluate((element) => getComputedStyle(element).stroke)).not.toBe('none');

	await gotoReady(page, '/analysis/metrics');
	const chartMark = page.getByLabel('Event counts by disaster category').locator('path').first();
	await expect(chartMark).toBeVisible();
	expect(await chartMark.evaluate((element) => getComputedStyle(element).stroke)).not.toBe('none');
});
