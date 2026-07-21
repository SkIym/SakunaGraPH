import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from '@playwright/test';
import manifest from '../visual-baselines/manifest.json' with { type: 'json' };
import { gotoReady, mockApi } from '../e2e/fixtures/api-mocks.js';

const testDirectory = dirname(fileURLToPath(import.meta.url));
const outputDirectory = process.env.VISUAL_OUTPUT_DIR
	? resolve(process.env.VISUAL_OUTPUT_DIR)
	: resolve(testDirectory, '../visual-baselines');

test.beforeEach(async ({ page }) => {
	await mockApi(page);
	await page.addStyleTag({
		content: '*, *::before, *::after { animation: none !important; transition: none !important; }',
	});
});

for (const entry of manifest.routes) {
	test(`capture ${entry.name}`, async ({ page }, testInfo) => {
		await gotoReady(page, entry.path);
		await page.locator('body').waitFor();
		const filename = `${entry.name}-${testInfo.project.name}.jpg`;
		mkdirSync(outputDirectory, { recursive: true });
		await page.screenshot({
			path: resolve(outputDirectory, filename),
			type: 'jpeg',
			quality: 82,
			fullPage: false,
		});
	});
}
