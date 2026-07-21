import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	optimizeDeps: {
		include: [
			'codemirror',
			'@codemirror/state',
			'@codemirror/language',
			'@lezer/highlight',
			'@codemirror/legacy-modes/mode/sparql',
			'd3-drag',
			'd3-force',
			'd3-geo',
			'd3-hierarchy',
			'd3-selection',
			'd3-zoom',
		],
	},
	server: {
		proxy: {
			'/api': {
				target: process.env.API_PROXY_TARGET ?? 'http://localhost:8000',
				changeOrigin: true,
			},
		},
	},
});
