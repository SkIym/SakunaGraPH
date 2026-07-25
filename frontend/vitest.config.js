import { fileURLToPath } from 'node:url';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { svelteTesting } from '@testing-library/svelte/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [svelte(), svelteTesting()],
	resolve: {
		alias: {
			$lib: fileURLToPath(new URL('./src/lib', import.meta.url)),
			'$env/dynamic/public': fileURLToPath(
				new URL('./tests/mocks/dynamic-public.js', import.meta.url),
			),
		},
	},
	test: {
		environment: 'jsdom',
		setupFiles: ['./tests/setup.js'],
		include: [
			'tests/unit/**/*.test.js',
			'tests/component/**/*.test.js',
			'tests/contracts/**/*.test.js',
		],
		clearMocks: true,
	},
});
