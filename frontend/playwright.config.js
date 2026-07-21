import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
	testDir: './tests',
	testIgnore: 'e2e/ask-streaming.spec.js',
	fullyParallel: false,
	forbidOnly: Boolean(process.env.CI),
	retries: process.env.CI ? 2 : 0,
	workers: 2,
	expect: { timeout: 10_000 },
	timeout: 60_000,
	reporter: process.env.CI ? 'github' : 'list',
	use: {
		baseURL: 'http://127.0.0.1:4173',
		trace: 'on-first-retry',
		screenshot: 'only-on-failure',
		reducedMotion: 'reduce',
	},
	projects: [
		{
			name: 'desktop-chromium',
			use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } },
		},
		{
			name: 'mobile-chromium',
			use: { ...devices['Pixel 7'] },
		},
	],
	webServer: {
		command: 'npm run dev -- --host 127.0.0.1 --port 4173',
		url: 'http://127.0.0.1:4173',
		reuseExistingServer: !process.env.CI,
		timeout: 120_000,
	},
});
