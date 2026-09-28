import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from './fixtures/api-mocks.js';

test.beforeEach(async ({ page }) => {
	await mockApi(page);
	await gotoReady(page, '/ask');
	await page.evaluate(() => localStorage.clear());
	await page.reload();
	await page.locator('[data-app-hydrated="true"]').waitFor();
});

test('restores a locally saved research session after reload', async ({ page }) => {
	const question = 'How many flood events were recorded in 2023?';
	await page.getByRole('textbox', { name: 'Question' }).fill(question);
	await page.getByRole('button', { name: 'Send' }).click();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
	await expect(page.getByText('Saved on this device')).toBeVisible();

	await page.reload();
	await page.locator('[data-app-hydrated="true"]').waitFor();
	await expect(page.getByText(question).first()).toBeVisible();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
	await expect(page.getByText(question).first()).toBeVisible();
});

test('starts new research and can reopen the previous session', async ({ page }) => {
	const question = 'Which region had the most casualties from typhoons?';
	await page.getByRole('textbox', { name: 'Question' }).fill(question);
	await page.getByRole('button', { name: 'Send' }).click();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
	await expect(page.getByText('Saved on this device')).toBeVisible();

	await page.locator('summary').filter({ hasText: 'Research' }).click();
	await page.getByRole('button', { name: 'New research' }).click();
	await expect(page.getByRole('heading', { name: /Ask SakunaGraPH/ })).toBeVisible();
	await page.locator('summary').filter({ hasText: 'Research' }).click();
	await page.locator('button.session-select').filter({ hasText: question }).click();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
});

test('exports the active research session as Markdown', async ({ page }) => {
	await page.getByRole('textbox', { name: 'Question' }).fill('Show one event');
	await page.getByRole('button', { name: 'Send' }).click();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();

	const downloadPromise = page.waitForEvent('download');
	await page.locator('summary').filter({ hasText: 'Research' }).click();
	await page.getByRole('button', { name: 'Export current' }).click();
	const download = await downloadPromise;
	expect(download.suggestedFilename()).toMatch(/show-one-event\.md$/);
});
