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

	await page.getByRole('link', { name: 'Run a SPARQL query' }).click();
	await expect(page).toHaveURL(/\/query$/);
});

test('decorative schema background cannot become a blocking polygon layer', async ({ page }) => {
	await gotoReady(page, '/');

	const background = page.locator('svg.schema-field');
	await expect(background).toHaveAttribute('aria-hidden', 'true');
	await expect(background).toHaveAttribute('fill', 'none');
	await expect(background).toHaveAttribute('pointer-events', 'none');
	await expect(background).toHaveCSS('pointer-events', 'none');
	expect(
		await background
			.locator('g path')
			.evaluateAll((paths) => paths.every((path) => path.getAttribute('fill') === 'none')),
	).toBe(true);

	await page.getByRole('link', { name: 'Explore the map' }).click();
	await expect(page).toHaveURL(/\/map$/);
});

test('home province preview identifies provinces and opens the province map', async ({
	page,
	isMobile,
}) => {
	await gotoReady(page, '/');

	const preview = page.getByLabel('Province map preview');
	await expect(preview).toBeVisible();
	const rankingOverlay = page.getByRole('img', {
		name: 'Regions with the highest linked disaster-record counts',
	});
	await expect(rankingOverlay).toBeVisible();
	await expect(rankingOverlay.locator('circle')).toHaveAttribute('fill', 'var(--color-action)');
	await expect(page.getByText('Highest record-count regions')).toBeVisible();
	const province = preview.getByRole('button').first();
	const actionLabel = await province.getAttribute('aria-label');
	const provinceName = actionLabel?.match(/^Open (.+) in the full map$/)?.[1];
	if (!provinceName) throw new Error(`Unexpected province action label: ${actionLabel}`);

	if (!isMobile) {
		await province.hover();
		await expect(page.getByRole('tooltip')).toHaveText(provinceName);
		await expect(province).toHaveAttribute('fill', 'var(--color-accent)');
	}
	await province.click();

	await expect(page).toHaveURL(/\/map\?view=provinces&province=\d+$/);
	await expect(page.getByRole('heading', { name: provinceName, exact: true })).toBeVisible();
	await expect(page.getByRole('button', { name: 'View details for Typhoon Salome' })).toBeVisible();
});

test('home and ask stay within a narrow mobile viewport with touch-safe controls', async ({
	page,
}) => {
	await page.setViewportSize({ width: 320, height: 740 });
	await gotoReady(page, '/');
	await page.evaluate(() => document.fonts.ready);

	const wordmark = await page.getByRole('link', { name: 'SakunaGraPH home' }).boundingBox();
	expect(wordmark.x).toBeGreaterThanOrEqual(0);
	expect(wordmark.x + wordmark.width).toBeLessThanOrEqual(320);
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
		true,
	);

	const publicAction = await page.getByRole('link', { name: 'Explore the map' }).boundingBox();
	const researchAction = await page.getByRole('link', { name: 'Run a SPARQL query' }).boundingBox();
	expect(researchAction.y).toBeGreaterThan(publicAction.y + publicAction.height);
	await expect(page.getByRole('heading', { name: 'Data freshness' })).toBeVisible();

	await gotoReady(page, '/ask');
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
		true,
	);

	for (const control of await page
		.getByRole('navigation', { name: 'Primary navigation' })
		.getByRole('link')
		.all()) {
		const box = await control.boundingBox();
		expect(box.height).toBeGreaterThanOrEqual(44);
	}
	const sendButton = await page.getByRole('button', { name: 'Send' }).boundingBox();
	expect(sendButton.height).toBeGreaterThanOrEqual(44);
});

test('SPARQL query returns results in an accessible dialog', async ({ page }) => {
	await gotoReady(page, '/query');
	const editor = page.locator('.cm-editor');
	await editor.click();
	expect(await editor.evaluate((element) => getComputedStyle(element).outlineStyle)).not.toBe(
		'none',
	);
	await page.getByRole('button', { name: 'Run query' }).click();

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
	await expect(area).toHaveClass(/outline-none/);
	await area.focus();
	await page.keyboard.press('Enter');

	const eventRow = page.getByRole('button', { name: 'View details for Typhoon Salome' });
	await expect(eventRow).toBeVisible();
	await expect(eventRow.locator('td').last()).toHaveCSS('vertical-align', 'middle');
	await eventRow.click();
	await expect(page.getByRole('dialog', { name: /Typhoon Salome/ })).toBeVisible();
});

test('map and ontology adapt to a narrow touch viewport', async ({ page }) => {
	await page.setViewportSize({ width: 320, height: 740 });
	await gotoReady(page, '/map');

	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
		true,
	);
	for (const name of ['By Region', 'By Province']) {
		const box = await page.getByRole('button', { name }).boundingBox();
		expect(box.height).toBeGreaterThanOrEqual(44);
	}

	const area = page.getByRole('button', { name: /^Select / }).first();
	await area.dispatchEvent('click');
	await expect(page.getByRole('button', { name: 'View details for Typhoon Salome' })).toBeVisible();
	const closeBox = await page.getByRole('button', { name: 'Close results' }).boundingBox();
	expect(closeBox.height).toBeGreaterThanOrEqual(44);

	await gotoReady(page, '/ontology');
	expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(
		true,
	);
	for (const name of ['Core Ontology', 'Disaster Taxonomy', 'PSGC Locations']) {
		const box = await page.getByRole('button', { name }).boundingBox();
		expect(box.height).toBeGreaterThanOrEqual(44);
	}
	await expect(page.getByRole('button', { name: 'Legend' })).toBeVisible();
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
	const analysisEventRow = page.getByRole('button', { name: 'View details for Typhoon Salome' });
	await expect(analysisEventRow.locator('td').first()).toHaveCSS('vertical-align', 'middle');
	await expect(analysisEventRow.locator('td').last()).toHaveCSS('vertical-align', 'middle');
	await analysisEventRow.click();
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
	await expect(page.getByText('Query used')).toBeVisible();
	await expect(page.getByText(/Results/)).toBeVisible();
});
