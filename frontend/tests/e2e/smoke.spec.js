import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from './fixtures/api-mocks.js';

test.beforeEach(async ({ page }) => {
	await mockApi(page);
});

test('top-level navigation keeps every route reachable', async ({ page }) => {
	await gotoReady(page, '/');

	for (const [name, pathname] of [
		['Map', '/map'],
		['Ontology', '/ontology'],
		['Ask', '/ask'],
		['Analysis', '/analysis'],
		['Home', '/'],
	]) {
		await page.getByRole('link', { name, exact: true }).click();
		await expect(page).toHaveURL(new RegExp(`${pathname === '/' ? '/$' : `${pathname}$`}`));
	}
});

test('SPARQL query returns results in an accessible dialog', async ({ page }) => {
	await gotoReady(page, '/');
	await page.getByRole('button', { name: 'Run Query' }).click();

	const dialog = page.getByRole('dialog');
	await expect(dialog).toContainText('Query Results');
	await expect(dialog).toContainText('Typhoon');
	await page.keyboard.press('Escape');
	await expect(dialog).toBeHidden();
});

test('map loads and supports keyboard selection and event details', async ({ page }) => {
	await gotoReady(page, '/map');
	const area = page.getByRole('button', { name: /^Select / }).first();
	await expect(area).toBeVisible();
	await area.focus();
	await page.keyboard.press('Enter');

	const eventRow = page.getByRole('button', { name: 'View details for Typhoon Salome' });
	await expect(eventRow).toBeVisible();
	await eventRow.click();
	await expect(page.getByRole('dialog', { name: /Typhoon Salome/ })).toBeVisible();
});

test('ontology graph and secondary datasets load', async ({ page }) => {
	await gotoReady(page, '/ontology');
	await expect(page.getByText('Legend', { exact: true })).toBeVisible();

	await page.getByRole('button', { name: 'Disaster Taxonomy' }).click();
	await expect(page.getByText('Category', { exact: true })).toBeVisible();

	await page.getByRole('button', { name: 'PSGC Locations' }).click();
	await expect(page.getByText('Island Group', { exact: true })).toBeVisible();
});

test('analysis overview, table, metrics, timeline, and event details load', async ({ page }) => {
	await gotoReady(page, '/analysis');
	await expect(page.getByRole('heading', { name: 'Current scope' })).toBeVisible();

	await gotoReady(page, '/analysis/events');
	await expect(page.getByRole('heading', { name: 'Disaster event records' })).toBeVisible();
	await page.getByRole('button', { name: 'View details for Typhoon Salome' }).click();
	await expect(page.getByRole('dialog', { name: /Typhoon Salome/ })).toBeVisible();
	await page.keyboard.press('Escape');

	await gotoReady(page, '/analysis/metrics');
	await expect(page.getByRole('heading', { name: 'Metrics dashboard' })).toBeVisible();
	await expect(page.getByText('1', { exact: true }).first()).toBeVisible();

	await gotoReady(page, '/analysis/timeline');
	await expect(page.getByRole('heading', { name: 'Timeline and date analysis' })).toBeVisible();
	await expect(page.getByText('Calendar drill-down')).toBeVisible();
});

test('ask keeps the legacy answer, SPARQL, and rows contract', async ({ page }) => {
	await gotoReady(page, '/ask');
	await page.getByRole('button', { name: /How many flood events/ }).click();

	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
	await expect(page.getByText('SPARQL Query')).toBeVisible();
	await expect(page.getByText(/Results/)).toBeVisible();
});
