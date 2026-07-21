import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
	testDir: './tests/e2e',
	testMatch: 'ask-streaming.spec.js',
	fullyParallel: false,
	workers: 1,
	expect: { timeout: 10_000 },
	timeout: 30_000,
	reporter: process.env.CI ? 'github' : 'list',
	use: {
		baseURL: 'http://127.0.0.1:4174',
		trace: 'on-first-retry',
		screenshot: 'only-on-failure',
		reducedMotion: 'reduce',
	},
	projects: [
		{
			name: 'streaming-chromium',
			use: { ...devices['Desktop Chrome'], viewport: { width: 1440, height: 900 } },
		},
	],
	webServer: [
		{
			command: 'node tests/e2e/fixtures/stream-server.js',
			url: 'http://127.0.0.1:4180/health',
			reuseExistingServer: false,
			timeout: 30_000,
		},
		{
			command: 'npm run dev -- --host 127.0.0.1 --port 4174',
			url: 'http://127.0.0.1:4174',
			reuseExistingServer: false,
			timeout: 120_000,
			env: {
				PUBLIC_ASK_STREAMING_ENABLED: 'true',
				API_PROXY_TARGET: 'http://127.0.0.1:4180',
			},
		},
	],
});
