import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
	testDir: './tests/performance',
	fullyParallel: false,
	workers: 1,
	timeout: 60_000,
	reporter: process.env.CI ? 'github' : 'list',
	use: {
		...devices['Desktop Chrome'],
		baseURL: 'http://127.0.0.1:4175',
		viewport: { width: 1440, height: 900 },
		reducedMotion: 'reduce',
		trace: 'on-first-retry',
	},
	webServer: {
		command: 'node build',
		url: 'http://127.0.0.1:4175',
		reuseExistingServer: !process.env.CI,
		timeout: 120_000,
		env: {
			HOST: '127.0.0.1',
			PORT: '4175',
		},
	},
});
