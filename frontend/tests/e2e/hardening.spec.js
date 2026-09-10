import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from './fixtures/api-mocks.js';

test('homepage survives 200% text scaling and expanded copy without page-level overflow', async ({
	page,
}) => {
	await gotoReady(page, '/');
	await page.evaluate(() => {
		document.documentElement.style.fontSize = '200%';
		const heading = document.querySelector('h1');
		if (heading) {
			heading.textContent =
				'Erkunden Sie philippinische Katastrophenaufzeichnungen nach Verwaltungsgebiet.';
		}
	});

	await expect(page.getByLabel('Province map preview')).toBeVisible();
	const dimensions = await page.evaluate(() => ({
		clientWidth: document.documentElement.clientWidth,
		scrollWidth: document.documentElement.scrollWidth,
		offenders: [...document.querySelectorAll('body *')]
			.filter(
				(element) =>
					element.getBoundingClientRect().right > document.documentElement.clientWidth + 1,
			)
			.slice(0, 8)
			.map((element) => ({
				tag: element.tagName,
				className: String(element.className).slice(0, 160),
				right: Math.round(element.getBoundingClientRect().right),
				scrollWidth: element.scrollWidth,
			})),
	}));
	expect(dimensions.scrollWidth, JSON.stringify(dimensions.offenders)).toBeLessThanOrEqual(
		dimensions.clientWidth + 1,
	);
});

test('question composer enforces its documented input boundary', async ({ page }) => {
	await gotoReady(page, '/ask');
	const composer = page.getByRole('textbox', { name: 'Question' });
	expect(await composer.getAttribute('maxlength')).toBe('1000');
	await composer.fill('🌧️'.repeat(800));
	expect((await composer.inputValue()).length).toBeLessThanOrEqual(1000);
});

test('failed map geometry can be retried without reloading the page', async ({ page }) => {
	await mockApi(page);
	let shouldFail = true;
	await page.route('**/data/regions.geojson*', async (route) => {
		if (shouldFail) {
			shouldFail = false;
			await route.fulfill({ status: 503, contentType: 'application/json', body: '{}' });
			return;
		}
		await route.continue();
	});

	await page.goto('/map');
	await expect(page.getByText('The map could not be loaded.')).toBeVisible();
	await page.getByRole('button', { name: 'Try again' }).click();
	await expect(page.getByLabel('Map of Philippine regions and provinces')).toBeVisible();
});
