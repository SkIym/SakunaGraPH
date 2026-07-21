import { existsSync, readFileSync, statSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const outputRoot = resolve(root, '.svelte-kit/output');
const clientRoot = resolve(outputRoot, 'client');
const serverRoot = resolve(outputRoot, 'server');
const budget = JSON.parse(readFileSync(resolve(root, 'performance-budgets.json'), 'utf8'));

if (!existsSync(resolve(serverRoot, 'manifest-full.js'))) {
	throw new Error('No production output found. Run npm run build before checking budgets.');
}

const { manifest } = await import(
	`${pathToFileURL(resolve(serverRoot, 'manifest-full.js')).href}?budget=${Date.now()}`
);

function gzipBytes(relativePath) {
	return gzipSync(readFileSync(resolve(clientRoot, relativePath))).byteLength;
}

function assertBudget(label, actual, limit) {
	if (actual > limit) {
		throw new Error(
			`${label} is ${actual.toLocaleString()} bytes (budget ${limit.toLocaleString()}).`,
		);
	}
}

const clientFiles = new Set(manifest._.client.imports);
clientFiles.add(manifest._.client.start);
clientFiles.add(manifest._.client.app);

const routeResults = [];
for (const route of manifest._.routes.filter((item) => item.page)) {
	const files = new Set(clientFiles);
	const nodeIndexes = [...route.page.layouts.filter(Number.isInteger), route.page.leaf];
	for (const nodeIndex of nodeIndexes) {
		const node = await import(
			`${pathToFileURL(resolve(serverRoot, `nodes/${nodeIndex}.js`)).href}?budget=${Date.now()}`
		);
		for (const file of node.imports ?? []) files.add(file);
	}
	const gzip = [...files]
		.filter((file) => file.endsWith('.js'))
		.reduce((total, file) => total + gzipBytes(file), 0);
	routeResults.push({ route: route.id, gzip });
	assertBudget(
		`Initial JavaScript for ${route.id}`,
		gzip,
		budget.build.largestInitialRouteJsGzipBytes,
	);
}

const viteManifest = JSON.parse(readFileSync(resolve(clientRoot, '.vite/manifest.json'), 'utf8'));
const lazyChunks = Object.values(viteManifest).filter(
	(entry) => entry.file?.endsWith('.js') && entry.isDynamicEntry,
);
const largestLazy = lazyChunks
	.map((entry) => ({ file: entry.file, gzip: gzipBytes(entry.file) }))
	.sort((left, right) => right.gzip - left.gzip)[0];
assertBudget(
	`Largest lazy JavaScript chunk (${largestLazy.file})`,
	largestLazy.gzip,
	budget.build.largestLazyChunkGzipBytes,
);

const cssFiles = [...new Set(Object.values(viteManifest).flatMap((entry) => entry.css ?? []))];
const largestCss = cssFiles
	.map((file) => ({ file, gzip: gzipBytes(file) }))
	.sort((left, right) => right.gzip - left.gzip)[0];
assertBudget(
	`Largest stylesheet (${largestCss.file})`,
	largestCss.gzip,
	budget.build.largestCssGzipBytes,
);

const mapBytes = statSync(resolve(root, 'static/data/regions.geojson')).size;
assertBudget('Map GeoJSON', mapBytes, budget.assets.mapGeoJsonBytes);
for (const image of ['elle-208.jpg', 'elle-416.jpg', 'abram-208.jpg', 'abram-416.jpg']) {
	assertBudget(
		`Team image ${image}`,
		statSync(resolve(root, `static/${image}`)).size,
		budget.assets.teamImageBytes,
	);
}

console.table(routeResults.map(({ route, gzip }) => ({ route, gzipBytes: gzip })));
console.log(
	`Budgets passed: lazy JS ${largestLazy.gzip} B gzip, CSS ${largestCss.gzip} B gzip, map ${mapBytes} B.`,
);
