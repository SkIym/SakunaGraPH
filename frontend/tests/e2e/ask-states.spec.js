import { expect, test } from '@playwright/test';
import { gotoReady, mockApi } from './fixtures/api-mocks.js';

test.beforeEach(async ({ page }) => {
	await mockApi(page, { delay: 250 });
	await gotoReady(page, '/ask');
});

async function ask(page, question) {
	await page.getByPlaceholder(/Ask a question/).fill(question);
	await page.getByRole('button', { name: 'Send' }).click();
}

test('records the current loading and success states', async ({ page }) => {
	await ask(page, 'Show one event');
	await expect(page.getByText(/Querying knowledge graph/)).toBeVisible();
	await expect(page.getByText('One matching disaster event was found.')).toBeVisible();
});

test('records the current empty state', async ({ page }) => {
	await ask(page, 'Return an empty result');
	await expect(page.getByText('No matching events were found.')).toBeVisible();
	await expect(page.getByText('No matching records in the knowledge graph.')).toBeVisible();
});

test('records the current failure state', async ({ page }) => {
	await ask(page, 'Simulate a failure');
	await expect(page.getByText('The knowledge graph is unavailable.')).toBeVisible();
});
