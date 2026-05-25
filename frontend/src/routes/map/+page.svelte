<script>
	import { onMount } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import PhilMap from '$lib/components/map/PhilMap.svelte';
	import {
		regionPsgcFromCC1,
		formatProvName,
		REGION_LABELS,
		REGION_COLORS,
		buildRegionQuery,
		buildRegionCountQuery,
		buildProvinceQuery,
		buildProvinceCountQuery,
		bindingDisplay,
		formatDisasterType
	} from '$lib/mapData.js';

	// ── Map data (loaded once) ───────────────────────────────────────────────
	const SVG_W = 700;
	const SVG_H = 800;

	let pathData = $state([]);
	let fullViewBox = `0 0 ${SVG_W} ${SVG_H}`;
	let tightViewBox = $state(`0 0 ${SVG_W} ${SVG_H}`); // snug around Philippines for thumbnail
	let detailViewBox = $state(null);
	let mapLoading = $state(true);
	let mapError = $state('');
	let pathGen = null; // d3 geoPath, kept as plain variable

	// ── UI state ─────────────────────────────────────────────────────────────
	let view = $state('regions');  // 'regions' | 'provinces'
	let selected = $state(null);   // {type, psgc, id, name, rawName?}

	// ── Results state ────────────────────────────────────────────────────────
	const PAGE_SIZE = 10;
	let results = $state(null);
	let majorCount = $state(0);
	let incidentCount = $state(0);
	let resultMode = $state('major');  // 'major' | 'incidents'
	let page = $state(1);
	let queryLoading = $state(false);
	let queryError = $state('');

	const totalCount = $derived(resultMode === 'major' ? majorCount : incidentCount);
	const totalPages = $derived(Math.max(1, Math.ceil(totalCount / PAGE_SIZE)));

	// ── Load GeoJSON + build paths ───────────────────────────────────────────
	onMount(async () => {
		try {
			const { geoMercator, geoPath } = await import('d3-geo');
			const res = await fetch('/data/regions.geojson');
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			const geojson = await res.json();

			const projection = geoMercator().fitSize([SVG_W, SVG_H], geojson);
			pathGen = geoPath(projection);

			pathData = geojson.features.map((f) => ({
				d: pathGen(f),
				gid: f.properties.adm2_psgc,
				name: f.properties.adm2_en,
				regionPsgc: f.properties.adm1_psgc,
				feature: f
			}));

			// Compute tight bounding box of all provinces for the mini thumbnail
			let bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
			for (const item of pathData) {
				try {
					const [[x0, y0], [x1, y1]] = pathGen.bounds(item.feature);
					bx0 = Math.min(bx0, x0); by0 = Math.min(by0, y0);
					bx1 = Math.max(bx1, x1); by1 = Math.max(by1, y1);
				} catch {}
			}
			const tp = 8; // tight padding
			tightViewBox = `${bx0 - tp} ${by0 - tp} ${bx1 - bx0 + tp * 2} ${by1 - by0 + tp * 2}`;

			mapLoading = false;
		} catch (e) {
			mapError = `Map failed to load: ${e.message}`;
			mapLoading = false;
		}
	});

	// ── Recompute detail viewBox whenever selection or view changes ──────────
	$effect(() => {
		if (!selected || !pathGen || pathData.length === 0) {
			detailViewBox = null;
			return;
		}
		const sk = view === 'regions' ? selected.psgc : selected.id;
		const items = pathData.filter((p) =>
			view === 'regions' ? p.regionPsgc === sk : p.gid === sk
		);
		if (items.length === 0) { detailViewBox = null; return; }

		let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
		for (const item of items) {
			try {
				const [[fx0, fy0], [fx1, fy1]] = pathGen.bounds(item.feature);
				x0 = Math.min(x0, fx0); y0 = Math.min(y0, fy0);
				x1 = Math.max(x1, fx1); y1 = Math.max(y1, fy1);
			} catch {}
		}
		const pad = 25;
		detailViewBox = `${x0 - pad} ${y0 - pad} ${x1 - x0 + pad * 2} ${y1 - y0 + pad * 2}`;
	});

	// ── When selection, mode, or page changes, fetch SPARQL data ───────────
	$effect(() => {
		if (!selected) { results = null; majorCount = 0; incidentCount = 0; return; }
		const _p = page;
		const _mode = resultMode;
		if (_p < 1) return;
		void fetchPage();
	});

	function sparql(query) {
		return fetch('/api/sparql', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ query })
		});
	}

	function countVal(json) {
		const v = json?.results?.bindings?.[0]?.count?.value;
		return v ? parseInt(v, 10) : 0;
	}

	async function fetchPage() {
		if (!selected) return;
		queryLoading = true;
		queryError = '';
		const offset = (page - 1) * PAGE_SIZE;
		const activeType  = resultMode === 'major' ? 'MajorEvent' : 'DisasterEvent';

		const isRegion = selected.type === 'region';
		const dataQ = isRegion
			? buildRegionQuery(selected.psgc, offset, PAGE_SIZE, activeType)
			: buildProvinceQuery(selected.rawName, offset, PAGE_SIZE, activeType);
		const majorCountQ = isRegion
			? buildRegionCountQuery(selected.psgc, 'MajorEvent')
			: buildProvinceCountQuery(selected.rawName, 'MajorEvent');
		const incidentCountQ = isRegion
			? buildRegionCountQuery(selected.psgc, 'DisasterEvent')
			: buildProvinceCountQuery(selected.rawName, 'DisasterEvent');

		try {
			const [dataRes, majorCountRes, incidentCountRes] = await Promise.all([
				sparql(dataQ), sparql(majorCountQ), sparql(incidentCountQ)
			]);
			const [dataJson, majorCountJson, incidentCountJson] = await Promise.all([
				dataRes.json(), majorCountRes.json(), incidentCountRes.json()
			]);

			if (!dataRes.ok) { queryError = dataJson.error ?? 'Query failed.'; return; }
			results = dataJson;
			majorCount    = countVal(majorCountJson);
			incidentCount = countVal(incidentCountJson);
		} catch {
			queryError = 'Could not reach server.';
		} finally {
			queryLoading = false;
		}
	}

	// ── Map interaction handlers ─────────────────────────────────────────────
	function handleMapSelect(item) {
		tooltipItem = null;
		resultMode = 'major';
		page = 1;
		results = null;

		if (view === 'regions') {
			const psgc = item.regionPsgc;
			const name = REGION_LABELS[psgc] ?? `Region ${psgc}`;
			selected = { type: 'region', psgc, name, id: psgc };
		} else {
			selected = {
				type: 'province',
				psgc: null,
				id: item.gid,
				name: formatProvName(item.name),
				rawName: item.name
			};
		}
	}

	function switchResultMode(mode) {
		if (resultMode === mode) return;
		resultMode = mode;
		page = 1;
	}

	// ── Per-view map rendering props ─────────────────────────────────────────
	// Region view: pastel fills per region, near-invisible internal province borders
	// Province view: all white, clearly drawn individual borders
	const mapColorMap = $derived(view === 'regions' ? REGION_COLORS : {});
	const mapStrokeColor = $derived(
		view === 'regions' ? 'rgba(55,65,81,0.42)' : '#374151'
	);
	const mapStrokeWidth = $derived(view === 'regions' ? 0.5 : 0.65);

	function deselect() {
		selected = null;
		results = null;
		queryError = '';
	}

	function switchView(v) {
		if (view === v) return;
		view = v;
		deselect();
	}

	// ── Hover tooltip state ──────────────────────────────────────────────────
	let tooltipItem = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);

	function getHoverLabel(item) {
		if (!item) return '';
		if (view === 'regions') return REGION_LABELS[item.regionPsgc] ?? formatProvName(item.name);
		return formatProvName(item.name);
	}

	// ── Pagination helpers ───────────────────────────────────────────────────
	const paginationPages = $derived.by(() => {
		if (totalPages <= 7) return Array.from({ length: totalPages }, (_, i) => i + 1);
		const pages = [];
		if (page <= 4) {
			for (let i = 1; i <= 5; i++) pages.push(i);
			pages.push('…', totalPages);
		} else if (page >= totalPages - 3) {
			pages.push(1, '…');
			for (let i = totalPages - 4; i <= totalPages; i++) pages.push(i);
		} else {
			pages.push(1, '…', page - 1, page, page + 1, '…', totalPages);
		}
		return pages;
	});

	// Result column display helpers
	function colValue(row, col) {
		const v = row[col];
		if (!v) return '—';
		if (col === 'disasterType') return formatDisasterType(bindingDisplay(v));
		if (col === 'startDate') return v.value?.split('T')[0] ?? v.value ?? '—';
		return bindingDisplay(v);
	}

	const DISPLAY_COLS = ['eventName', 'disasterType', 'startDate', 'locLabel'];
	const COL_LABELS = {
		eventName: 'Event',
		disasterType: 'Type',
		startDate: 'Date',
		locLabel: 'Location'
	};
</script>

<svelte:head>
	<title>Map · SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<!-- ── Cursor-following hover tooltip ────────────────────────────────────── -->
{#if tooltipItem}
	<div
		class="fixed z-50 pointer-events-none rounded-lg bg-slate-800/90 px-3 py-1.5 text-xs font-medium text-white shadow-lg"
		style="left:{tooltipX + 16}px; top:{tooltipY}px; transform:translateY(-50%); backdrop-filter:blur(4px);"
	>
		{getHoverLabel(tooltipItem)}
	</div>
{/if}

<!-- ── Full-screen layout container ─────────────────────────────────────── -->
<div
	class="relative"
	style="height: calc(100vh - 52px); z-index: 1; overflow: hidden;"
>
	<!-- ── View toggle — hidden when a region/province is selected ──────────── -->
	{#if !selected}
		<div
			class="absolute top-4 left-1/2 -translate-x-1/2 z-10 flex gap-1 rounded-full border border-slate-200/80 bg-white/80 p-1 shadow-sm"
			style="backdrop-filter:blur(10px);"
		>
			<button
				onclick={() => switchView('regions')}
				class="rounded-full px-4 py-1.5 text-xs font-semibold transition-all duration-150
				{view === 'regions' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
			>
				By Region
			</button>
			<button
				onclick={() => switchView('provinces')}
				class="rounded-full px-4 py-1.5 text-xs font-semibold transition-all duration-150
				{view === 'provinces' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
			>
				By Province
			</button>
		</div>
	{/if}

	<!-- ── Map panel (left side, shrinks on selection) ─────────────────────── -->
	<div
		class="absolute top-0 left-0 h-full transition-all duration-500 ease-out"
		style="width: {selected ? '42%' : '100%'};"
	>
		{#if selected}
			<!-- Zoomed detail map fills the whole panel -->
			<div class="absolute inset-0 p-4">
				{#if pathData.length > 0}
					<PhilMap
						{pathData}
						viewBox={detailViewBox ?? fullViewBox}
						{view}
						{selected}
						interactive={false}
						strokeWidth={1.4}
					/>
				{/if}
			</div>

			<!-- Compact back button + mini thumbnail — upper-left corner -->
			<div class="absolute top-3 left-3 z-10">
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<div
					class="cursor-pointer rounded-xl border border-slate-200/80 bg-white/90 p-2 shadow-md hover:bg-slate-50 transition-colors"
					style="backdrop-filter:blur(8px);"
					role="button"
					tabindex="0"
					title="Back to full map"
					onclick={deselect}
					onkeydown={(e) => e.key === 'Enter' && deselect()}
				>
					<div class="flex items-center gap-1.5 mb-1.5">
						<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="text-slate-500">
							<path d="M19 12H5"/><path d="m12 5-7 7 7 7"/>
						</svg>
						<span class="text-[9px] font-semibold uppercase tracking-wider text-slate-400">Back</span>
					</div>
					<div style="width:96px; height:84px; pointer-events:none;">
						{#if pathData.length > 0}
							<PhilMap
								{pathData}
								viewBox={tightViewBox}
								{view}
								{selected}
								colorMap={mapColorMap}
								interactive={false}
								strokeWidth={0.15}
								strokeColor="rgba(55,65,81,0.35)"
							/>
						{/if}
					</div>
				</div>
			</div>
		{:else}
			<!-- Full map — row layout: map on left, label on right -->
			<div class="flex items-center justify-center h-full gap-10 px-8 pt-12">
				{#if mapLoading}
					<p class="text-slate-400 text-sm">Loading map…</p>
				{:else if mapError}
					<p class="text-red-500 text-sm text-center">{mapError}</p>
				{:else}
					<!-- Map — flex-shrink:0 + fixed aspect-ratio keeps width stable on hover -->
					<div style="height: calc(100vh - 120px); aspect-ratio: 7/8; flex-shrink: 0; flex-grow: 0; max-width: 58%;">
						<PhilMap
							{pathData}
							viewBox={fullViewBox}
							{view}
							selected={null}
							interactive={true}
							strokeWidth={mapStrokeWidth}
							strokeColor={mapStrokeColor}
							colorMap={mapColorMap}
							onselect={handleMapSelect}
							onhover={(item, x, y) => { tooltipItem = item; tooltipX = x; tooltipY = y; }}
						/>
					</div>

					<!-- Beside-map label — fixed width so layout never shifts on hover -->
					<!-- <div class="pointer-events-none flex-shrink-0" style="width: 200px;">
						<p
							class="font-bold text-slate-700 leading-snug"
							style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.2rem,2vw,1.8rem); overflow-wrap:break-word; word-break:break-word;"
						>
							{tooltipItem ? getHoverLabel(tooltipItem) : 'Philippines'}
						</p>
						<p class="text-[10px] font-medium uppercase tracking-widest text-slate-400 mt-1">
							{tooltipItem ? (view === 'regions' ? 'Region' : 'Province') : 'Hover to explore'}
						</p>
						<p class="mt-3 text-[11px] text-slate-400 leading-relaxed">
							Click a {view === 'regions' ? 'region' : 'province'} to explore disaster data.
						</p>
					</div> -->


					<div class="pointer-events-none flex-shrink-0" style="width: 300px;">
						<p
							class="font-black text-slate-700 leading-snug"
							style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.8rem, 3vw, 2.5rem); overflow-wrap:break-word; word-break:break-word;"
						>
							{tooltipItem ? getHoverLabel(tooltipItem) : 'Philippines'}
						</p>
						<p class="text-[10px] font-medium uppercase tracking-widest text-slate-400 mt-1">
							{tooltipItem ? (view === 'regions' ? 'Region' : 'Province') : 'Hover to explore'}
						</p>
						<p class="mt-3 text-[14px] text-slate-400 leading-relaxed whitespace-nowrap">
							Click a {view === 'regions' ? 'region' : 'province'} to explore disaster data.
						</p>
					</div>
				{/if}
			</div>
		{/if}
	</div>

	<!-- ── Results panel (right side, appears on selection) ────────────────── -->
	<div
		class="absolute top-0 right-0 h-full border-l border-slate-200/60 bg-white/80 transition-all duration-500 ease-out overflow-hidden"
		style="backdrop-filter:blur(12px); width: {selected ? '58%' : '0%'};"
	>
		{#if selected}
			<!-- Outer flex: two spacers push content to vertical center -->
			<div class="flex flex-col h-full">
				<div class="flex-1 min-h-0"></div>

				<!-- Content block — vertically centered, max 85% of panel height -->
				<div
					class="flex flex-col mx-6"
					style="max-height: 85vh; overflow: hidden;"
				>
					<!-- Header -->
					<div class="pb-3 flex-shrink-0">
						<div class="flex items-start justify-between gap-3">
							<div class="min-w-0">
								<p class="text-[10px] font-semibold uppercase tracking-widest text-slate-400 mb-1">
									{selected.type === 'region' ? 'Region' : 'Province'}
								</p>
								<h2
									class="font-bold text-slate-800 leading-tight"
									style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.1rem,2.5vw,1.6rem);"
								>
									{selected.name}
								</h2>

								{#if queryLoading && !results}
									<div class="mt-2 flex items-center gap-2 text-slate-400 text-sm">
										<svg class="animate-spin" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
											<path d="M21 12a9 9 0 1 1-6.219-8.56"/>
										</svg>
										Querying…
									</div>
								{:else}
									<!-- Primary count (active mode) -->
									<p class="mt-2 font-bold leading-none" style="color:#dc2626; font-size:clamp(1.3rem,2.5vw,1.8rem);">
										{totalCount.toLocaleString()}
										<span class="font-semibold" style="color:#dc2626; font-size:clamp(0.8rem,1.4vw,1rem);">
											{resultMode === 'major' ? 'major disaster event' : 'incident'}{totalCount === 1 ? '' : 's'}
										</span>
									</p>
									<!-- Secondary count (inactive mode) -->
									<p class="mt-0.5 text-xs text-slate-400">
										{#if resultMode === 'major'}
											{incidentCount.toLocaleString()} incident{incidentCount === 1 ? '' : 's'}
										{:else}
											{majorCount.toLocaleString()} major disaster event{majorCount === 1 ? '' : 's'}
										{/if}
									</p>
								{/if}

								<!-- Toggle -->
								<div class="mt-2.5 flex gap-1 rounded-full border border-slate-200 bg-slate-50 p-0.5 w-fit">
									<button
										onclick={() => switchResultMode('major')}
										class="rounded-full px-3 py-1 text-[11px] font-semibold transition-all duration-150
										{resultMode === 'major' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
									>Major Events</button>
									<button
										onclick={() => switchResultMode('incidents')}
										class="rounded-full px-3 py-1 text-[11px] font-semibold transition-all duration-150
										{resultMode === 'incidents' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
									>Incidents</button>
								</div>
							</div>
							<button
								onclick={deselect}
								class="mt-1 flex-shrink-0 rounded-lg p-1.5 text-slate-400 hover:bg-slate-100 hover:text-slate-600 transition-colors"
								title="Close"
							>
								<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
									<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
								</svg>
							</button>
						</div>
					</div>

					<!-- Divider -->
					<div class="border-t border-slate-100 flex-shrink-0"></div>

					<!-- Results body — scrollable -->
					<div class="overflow-y-auto py-4 flex-1 min-h-0">
						{#if queryError}
							<div class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
								{queryError}
							</div>

						{:else if results}
							{@const rows = results.results?.bindings ?? []}
							{#if rows.length === 0}
								<div class="py-10 text-center text-slate-400 text-sm">
									No disaster events found for this area.
								</div>
							{:else}
								<div class="overflow-x-auto rounded-xl border border-slate-200/80 shadow-sm">
									<table class="w-full text-xs">
										<thead>
											<tr class="bg-slate-50 border-b border-slate-200">
												{#each DISPLAY_COLS as col}
													<th class="px-3 py-2.5 text-left font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap">
														{COL_LABELS[col]}
													</th>
												{/each}
											</tr>
										</thead>
										<tbody class="divide-y divide-slate-100">
											{#each rows as row, i}
												<tr class="hover:bg-blue-50/40 transition-colors {i % 2 === 0 ? '' : 'bg-slate-50/40'}">
													{#each DISPLAY_COLS as col}
														<td class="px-3 py-2 text-slate-600 max-w-[160px] truncate" title={row[col]?.value ?? ''}>
															{colValue(row, col)}
														</td>
													{/each}
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
								{#if queryLoading}
									<div class="mt-3 text-center text-xs text-slate-400">Updating…</div>
								{/if}
							{/if}
						{/if}
					</div>

					<!-- Pagination -->
					{#if totalPages > 1}
						<div class="flex-shrink-0 border-t border-slate-100 pt-3 pb-2 flex flex-col items-center gap-2">
							<span class="text-xs text-slate-400">Page {page} of {totalPages}</span>
							<div class="flex items-center gap-1">
								<button
									onclick={() => { page = Math.max(1, page - 1); }}
									disabled={page === 1 || queryLoading}
									class="rounded-lg px-2.5 py-1 text-xs font-medium text-slate-600 border border-slate-200 hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
								>← Prev</button>

								{#each paginationPages as p}
									{#if p === '…'}
										<span class="px-1 text-slate-400 text-xs">…</span>
									{:else}
										<button
											onclick={() => { page = p; }}
											disabled={queryLoading}
											class="rounded-lg w-7 h-7 text-xs font-medium border transition-colors
											{page === p ? 'bg-slate-800 text-white border-slate-800' : 'border-slate-200 text-slate-600 hover:bg-slate-50'}"
										>{p}</button>
									{/if}
								{/each}

								<button
									onclick={() => { page = Math.min(totalPages, page + 1); }}
									disabled={page === totalPages || queryLoading}
									class="rounded-lg px-2.5 py-1 text-xs font-medium text-slate-600 border border-slate-200 hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
								>Next →</button>
							</div>
						</div>
					{/if}
				</div>

				<div class="flex-1 min-h-0"></div>
			</div>
		{/if}
	</div>
</div>
