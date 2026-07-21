import js from '@eslint/js';
import { defineConfig } from 'eslint/config';
import globals from 'globals';
import svelte from 'eslint-plugin-svelte';
import svelteConfig from './svelte.config.js';

export default defineConfig([
	{
		ignores: [
			'.svelte-kit/**',
			'build/**',
			'node_modules/**',
			'playwright-report/**',
			'test-results/**',
			'tests/visual-baselines/**',
		],
	},
	js.configs.recommended,
	svelte.configs.recommended,
	{
		languageOptions: {
			globals: {
				...globals.browser,
				...globals.node,
			},
		},
	},
	{
		files: ['**/*.svelte', '**/*.svelte.js'],
		languageOptions: {
			parserOptions: { svelteConfig },
		},
	},
	{
		files: ['tests/**/*.js'],
		languageOptions: {
			globals: { ...globals.browser, ...globals.node },
		},
	},
	{
		rules: {
			'no-empty': ['error', { allowEmptyCatch: true }],
			'no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
			'svelte/no-navigation-without-resolve': 'off',
			'svelte/prefer-svelte-reactivity': 'off',
			'svelte/require-each-key': 'off',
		},
	},
]);
