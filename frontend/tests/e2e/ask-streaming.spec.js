import { expect, test } from '@playwright/test';
import { gotoReady } from './fixtures/api-mocks.js';

async function send(page, question) {
	await page.getByRole('textbox', { name: 'Question' }).fill(question);
	await page.getByRole('button', { name: 'Send' }).click();
}

async function streamStats(page) {
	const response = await page.request.get('/api/test/stream-stats');
	return response.json();
}

test('streams GraphRAG prose and source provenance through the API proxy', async ({ page }) => {
	await gotoReady(page, '/ask');
	const composer = page.getByRole('textbox', { name: 'Question' });
	await composer.fill('Stream an answer');
	await composer.press('Enter');
	await expect(composer).toBeFocused();

	await expect(page.getByText('Generating response…')).toBeVisible();
	await expect(page.getByText('One streamed event.')).toBeVisible();
	await expect(page.getByText('GraphRAG')).toBeVisible();
	await expect(page.getByText(/Index fixture-v1/)).toBeVisible();
	await expect(page.getByRole('link', { name: 'NDRRMC Situation Report' })).toHaveAttribute(
		'href',
		'https://example.test/reports/1',
	);
	await expect(page.getByText('SPARQL Query')).toBeVisible();
	await expect(page.getByText(/Results/)).toBeVisible();
	await expect(composer).toBeFocused();
});

test('falls back to legacy ask when the rollout endpoint is unavailable', async ({ page }) => {
	await gotoReady(page, '/ask');
	await send(page, 'Use fallback mode');

	await expect(page.getByText('Legacy rollout fallback.')).toBeVisible();
	await expect(page.getByText('Fallback', { exact: true })).toBeVisible();
});

test('user cancellation and navigation close upstream streams', async ({ page }) => {
	await gotoReady(page, '/ask');
	const initial = await streamStats(page);
	await send(page, 'Cancel this response');
	await expect(page.getByText('Partial')).toBeVisible();
	await page.getByRole('button', { name: 'Cancel' }).click();

	await expect(page.getByText('Response cancelled.')).toBeVisible();
	await expect(page.getByRole('status')).toHaveText('Request cancelled.');
	await expect
		.poll(async () => (await streamStats(page)).cancelledStreams)
		.toBeGreaterThan(initial.cancelledStreams);

	const afterUserCancel = await streamStats(page);
	await send(page, 'Cancel by navigating away');
	await expect(page.getByText('Partial')).toBeVisible();
	await page.goto('/map');
	await expect
		.poll(async () => (await streamStats(page)).cancelledStreams)
		.toBeGreaterThan(afterUserCancel.cancelledStreams);
});
