<script>
	import { onMount } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import PhilMap from '$lib/components/map/PhilMap.svelte';
	import {
		regionPsgcFromCC1,
		formatProvName,
		REGION_LABELS,
		REGION_COLORS,
		bindingDisplay,
		formatDisasterType
	} from '$lib/mapData.js';
	import cityPathsRaw from '$lib/data/gadm41_PHL_2_paths.json';

	// ── Map data ─────────────────────────────────────────────────────────────
	const SVG_W = 700;
	const SVG_H = 800;

	const cityPathData = cityPathsRaw;

	let pathData = $state.raw([]);

	let fullViewBox = `0 0 ${SVG_W} ${SVG_H}`;
	let tightViewBox = $state(`0 0 ${SVG_W} ${SVG_H}`);
	let detailViewBox = $state(null);
	let mapLoading = $state(true);
	let mapError = $state('');
	let pathGen = null;

	// ── UI state ─────────────────────────────────────────────────────────────
	let view = $state('regions');  // 'regions' | 'provinces' | 'cities'
	let selected = $state(null);   // {type, psgc?, id, name, rawName?, province?, bounds?}

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
		if (!selected || !pathGen) { detailViewBox = null; return; }

		// City: use pre-computed bounds stored on the selected item
		if (view === 'cities') {
			if (!selected.bounds) { detailViewBox = null; return; }
			const [[x0, y0], [x1, y1]] = selected.bounds;
			const pad = 12;
			detailViewBox = `${x0 - pad} ${y0 - pad} ${x1 - x0 + pad * 2} ${y1 - y0 + pad * 2}`;
			return;
		}

		if (pathData.length === 0) { detailViewBox = null; return; }
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

	// ── When selection, mode, or page changes, fetch data ───────────────────
	$effect(() => {
		if (!selected) { results = null; majorCount = 0; incidentCount = 0; return; }
		const _p = page;
		const _mode = resultMode;
		if (_p < 1) return;
		void fetchPage();
	});

	async function fetchPage() {
		if (!selected) return;
		queryLoading = true;
		queryError = '';
		expandedRows = new Set();
        expandedAlternates = new Set();

		const scope = selected.type === 'region' ? 'region' : selected.type === 'city' ? 'city' : 'province';
		const id    = selected.type === 'region' ? selected.psgc : selected.rawName;
		const params = new URLSearchParams({ scope, id, mode: resultMode, page: String(page) });
		if (selected.type === 'city' && selected.province) params.set('province', selected.province);

		try {
			const res = await fetch(`/api/map/events?${params}`);
			const data = await res.json();

			if (!res.ok) { queryError = data.message ?? 'Query failed.'; return; }

			// Wrap bindings back into the shape the template already expects:
			// results.results.bindings — avoids touching any template code
			results       = { results: { bindings: data.events } };
			majorCount    = data.majorCount;
			incidentCount = data.incidentCount;
            groupedResults = groupByAlternates(data.events);
            console.log('sample alternates value:', data.events[0]?.alternates?.value);
            console.log('sample event IRI:', data.events[0]?.event?.value);
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
		} else if (view === 'cities') {
			selected = {
				type: 'city',
				psgc: null,
				id: item.gid,
				name: item.name,
				rawName: item.name,
				province: item.province,
				bounds: item.bounds
			};
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
	const mapColorMap    = $derived(view !== 'provinces' ? REGION_COLORS : {});
	const mapStrokeColor = $derived(view === 'regions' ? 'rgba(55,65,81,0.42)' : '#374151');
	const mapStrokeWidth = $derived(view === 'regions' ? 0.5 : view === 'cities' ? 0.3 : 0.65);

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

	// ── Search ────────────────────────────────────────────────────────────────
	let searchQuery = $state('');
	let searchOpen  = $state(false);

	function normalize(s) {
		return s.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();
	}

	function getSearchResults(q) {
		const lq = normalize(q.trim());
		if (!lq) return [];
		const out = [];

		for (const [psgc, name] of Object.entries(REGION_LABELS)) {
			if (normalize(name).includes(lq))
				out.push({ type: 'region', psgc, name });
		}
		for (const p of pathData) {
			const name = formatProvName(p.name);
			if (normalize(name).includes(lq))
				out.push({ type: 'province', item: p, name });
		}
		for (const c of cityPathData) {
			if (normalize(c.name).includes(lq) || normalize(c.province).includes(lq))
				out.push({ type: 'city', item: c, name: c.name, sub: c.province });
		}

		return out.slice(0, 12);
	}

	function selectFromSearch(result) {
		searchQuery = '';
		searchOpen  = false;
		tooltipItem = null;
		resultMode  = 'major';
		page        = 1;
		results     = null;

		if (result.type === 'region') {
			view     = 'regions';
			selected = { type: 'region', psgc: result.psgc, name: result.name, id: result.psgc };
		} else if (result.type === 'province') {
			view     = 'provinces';
			selected = { type: 'province', psgc: null, id: result.item.gid, name: formatProvName(result.item.name), rawName: result.item.name };
		} else {
			view     = 'cities';
			selected = { type: 'city', psgc: null, id: result.item.gid, name: result.item.name, rawName: result.item.name, province: result.item.province, bounds: result.item.bounds };
		}
	}

	// ── Hover tooltip state ──────────────────────────────────────────────────
	let tooltipItem = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);

	function getHoverLabel(item) {
		if (!item) return '';
		if (view === 'regions') return REGION_LABELS[item.regionPsgc] ?? formatProvName(item.name);
		if (view === 'cities') return `${item.name}, ${item.province}`;
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

	const DISPLAY_COLS = ['eventName', 'disasterType', 'startDate', 'locations'];
	const COL_LABELS = {
		eventName: 'Event',
		disasterType: 'Type',
		startDate: 'Date',
		locations: 'Locations'
	};

	let expandedRows = $state(new Set());
    let expandedAlternates = $state(new Set());
    let groupedResults = $state(null);

    function groupByAlternates(bindings) {
        const byIri = new Map();
        for (const row of bindings) {
            if (row.event?.value) byIri.set(row.event.value, row);
        }

        // Build adjacency: for each IRI, collect all known alts that exist on this page
        const adjAlts = new Map();
        for (const row of bindings) {
            const iri = row.event?.value;
            if (!iri) continue;
            const alts = (row.alternates?.value ?? '')
            .split(',').map(s => s.trim()).filter(s => s && byIri.has(s));
            adjAlts.set(iri, alts);
        }

        // Union-find to cluster all connected alternates
        const parent = new Map();
        function find(x) {
            if (!parent.has(x)) parent.set(x, x);
            if (parent.get(x) !== x) parent.set(x, find(parent.get(x)));
            return parent.get(x);
        }
        function union(x, y) {
            const px = find(x), py = find(y);
            if (px !== py) parent.set(px, py);
        }

        for (const [iri, alts] of adjAlts) {
            for (const alt of alts) union(iri, alt);
        }

        // Group IRIs by root
        const clusters = new Map(); // root -> [iri, ...]
        for (const iri of byIri.keys()) {
            const root = find(iri);
            if (!clusters.has(root)) clusters.set(root, []);
            clusters.get(root).push(iri);
        }

        // For each cluster, elect rep by earliest startDate, IRI tiebreak
        const output = [];
        const seen = new Set();
        for (const row of bindings) {
            const iri = row.event?.value;
            if (!iri || seen.has(iri)) continue;

            const root = find(iri);
            const clusterIris = clusters.get(root) ?? [iri];

            // Mark all cluster members seen so we don't emit duplicate groups
            for (const m of clusterIris) seen.add(m);

            if (clusterIris.length === 1) {
            output.push({ row, subs: [] });
            continue;
            }

            const members = clusterIris.map(m => byIri.get(m)).filter(Boolean);
            members.sort((a, b) => {
            const da = a.startDate?.value ?? '';
            const db = b.startDate?.value ?? '';
            if (da !== db) return da < db ? -1 : 1;
            const ia = a.event?.value ?? '';
            const ib = b.event?.value ?? '';
            return ia < ib ? -1 : ia > ib ? 1 : 0;
            });

            const [rep, ...subs] = members;
            output.push({ row: rep, subs });
        }

        return output;
    }
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
			<button
				onclick={() => switchView('cities')}
				class="rounded-full px-4 py-1.5 text-xs font-semibold transition-all duration-150
				{view === 'cities' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
			>
				By City
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
				{#if view === 'cities' ? cityPathData : pathData.length > 0}
					<PhilMap
						pathData={view === 'cities' ? cityPathData : pathData}
						viewBox={detailViewBox ?? fullViewBox}
						{view}
						{selected}
						interactive={false}
						strokeWidth={view === 'cities' ? 0.3 : 1.4}
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
							pathData={view === 'cities' ? cityPathData : pathData}
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


					<div class="flex-shrink-0" style="width: 300px;">
						<p
							class="font-black text-slate-700 leading-snug pointer-events-none"
							style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.8rem, 3vw, 2.5rem); overflow-wrap:break-word; word-break:break-word;"
						>
							{tooltipItem ? getHoverLabel(tooltipItem) : 'Philippines'}
						</p>
						<p class="text-[10px] font-medium uppercase tracking-widest text-slate-400 mt-1 pointer-events-none">
							{tooltipItem
								? (view === 'regions' ? 'Region' : view === 'cities' ? 'City / Municipality' : 'Province')
								: 'Hover to explore'}
						</p>
						<p class="mt-1 text-[13px] text-slate-400 leading-relaxed pointer-events-none">
							Click a {view === 'regions' ? 'region' : view === 'cities' ? 'city' : 'province'} to explore disaster data.
						</p>

						<!-- Search bar -->
						<div class="relative mt-4">
							<div class="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/95 px-3 py-2 shadow-sm focus-within:border-slate-400 transition-colors">
								<svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="text-slate-400 flex-shrink-0">
									<circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
								</svg>
								<input
									type="text"
									placeholder="Search region, province, or city…"
									bind:value={searchQuery}
									onfocus={() => searchOpen = true}
									onblur={() => setTimeout(() => searchOpen = false, 120)}
									class="flex-1 bg-transparent text-sm text-slate-700 placeholder-slate-400 focus:outline-none min-w-0"
								/>
								{#if searchQuery}
									<button
										onmousedown={() => { searchQuery = ''; }}
										class="text-slate-300 hover:text-slate-500 transition-colors flex-shrink-0"
									>
										<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
											<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>
										</svg>
									</button>
								{/if}
							</div>

							{#if searchOpen && searchQuery.trim()}
								{@const hits = getSearchResults(searchQuery)}
								{#if hits.length > 0}
									<div class="absolute top-full left-0 right-0 z-30 mt-1 max-h-60 overflow-y-auto rounded-xl border border-slate-200 bg-white shadow-lg">
										{#each hits as result}
											<button
												class="flex w-full items-center gap-2.5 px-3 py-2 text-left hover:bg-slate-50 transition-colors border-b border-slate-50 last:border-0"
												onmousedown={() => selectFromSearch(result)}
											>
												<span class="flex-shrink-0 rounded-full px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide
													{result.type === 'region'   ? 'bg-blue-100 text-blue-600'    :
													 result.type === 'province' ? 'bg-emerald-100 text-emerald-600' :
													                              'bg-violet-100 text-violet-600'}">
													{result.type === 'region' ? 'Region' : result.type === 'province' ? 'Province' : 'City'}
												</span>
												<span class="flex-1 text-sm text-slate-700 truncate">{result.name}</span>
												{#if result.sub}
													<span class="flex-shrink-0 text-xs text-slate-400 truncate max-w-[90px]">{result.sub}</span>
												{/if}
											</button>
										{/each}
									</div>
								{:else}
									<div class="absolute top-full left-0 right-0 z-30 mt-1 rounded-xl border border-slate-200 bg-white shadow-lg px-3 py-3 text-sm text-slate-400">
										No results for "{searchQuery}"
									</div>
								{/if}
							{/if}
						</div>
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
									{selected.type === 'region' ? 'Region' : selected.type === 'city' ? 'City / Municipality' : 'Province'}
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
											{#if queryLoading}
												{#each Array(PAGE_SIZE) as _, i}
													<tr class="{i % 2 === 0 ? '' : 'bg-slate-50/40'}">
														<td class="px-3 py-2"><div class="h-3 w-32 rounded-full bg-slate-200 animate-pulse"></div></td>
														<td class="px-3 py-2"><div class="h-3 w-20 rounded-full bg-slate-200 animate-pulse"></div></td>
														<td class="px-3 py-2"><div class="h-3 w-16 rounded-full bg-slate-200 animate-pulse"></div></td>
														<td class="px-3 py-2"><div class="h-3 w-24 rounded-full bg-slate-200 animate-pulse"></div></td>
													</tr>
												{/each}
											{:else}
												{#each groupedResults ?? [] as { row, subs }, i}
                                                {@const rowKey = row.event?.value ?? String(i)}
                                                {@const locs = (row.locations?.value ?? '').split('|').filter(Boolean)}
                                                {@const dtypes = (row.disasterType?.value ?? row.disasterTypes?.value ?? '')
                                                    .split(',').map(s => s.trim()).filter(Boolean)}
                                                {@const expanded = expandedRows.has(rowKey)}
                                                {@const expandable = locs.length > 1}
                                                {@const hasAlts = subs.length > 0}
                                                {@const altsExpanded = expandedAlternates.has(rowKey)}

                                                <!-- Representative row -->
                                                <!-- svelte-ignore a11y_click_events_have_key_events -->
                                                <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                                                <tr
                                                    class="transition-colors {i % 2 === 0 ? '' : 'bg-slate-50/40'}
                                                    {expandable || hasAlts ? 'cursor-pointer hover:bg-blue-50/60' : 'hover:bg-blue-50/40'}"
                                                    onclick={() => {
                                                    if (expandable) {
                                                        if (expanded) expandedRows = new Set([...expandedRows].filter(k => k !== rowKey));
                                                        else expandedRows = new Set([...expandedRows, rowKey]);
                                                    }
                                                    }}
                                                >
                                                    <!-- Event name + alternates badge -->
                                                    <td class="px-3 py-2 text-slate-600 max-w-[160px]" title={row.eventName?.value ?? ''}>
                                                    <div class="flex flex-col gap-1">
                                                        <span class="truncate">{colValue(row, 'eventName')}</span>
                                                        {#if hasAlts}
                                                            <button
                                                                class="w-fit rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-semibold text-violet-600 hover:bg-violet-200 transition-colors"
                                                                onclick={(e) => {
                                                                e.stopPropagation();
                                                                if (altsExpanded) expandedAlternates = new Set([...expandedAlternates].filter(k => k !== rowKey));
                                                                else expandedAlternates = new Set([...expandedAlternates, rowKey]);
                                                                }}
                                                            >
                                                                {altsExpanded ? '▾' : '▸'} {subs.length} alternate{subs.length > 1 ? 's' : ''}
                                                            </button>
                                                            {/if}
                                                            {#if row.source?.value}
                                                            <span class="w-fit rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-400 uppercase tracking-wide">
                                                                {row.source.value}
                                                            </span>
                                                        {/if}
                                                    </div>
                                                    </td>

                                                    <!-- Disaster type: first + +N more -->
                                                    <td class="px-3 py-2 text-slate-600">
                                                    {#if dtypes.length === 0}
                                                        <span class="text-slate-300">—</span>
                                                    {:else}
                                                        <div class="flex flex-col gap-0.5">
                                                        <span>{formatDisasterType(dtypes[0])}</span>
                                                        {#if dtypes.length > 1}
                                                            <button
                                                            class="w-fit text-blue-400 text-[10px] font-medium"
                                                            onclick={(e) => {
                                                                e.stopPropagation();
                                                                // reuse expandedRows with a dtype- prefix key
                                                                const dtKey = 'dt-' + rowKey;
                                                                if (expandedRows.has(dtKey)) expandedRows = new Set([...expandedRows].filter(k => k !== dtKey));
                                                                else expandedRows = new Set([...expandedRows, dtKey]);
                                                            }}
                                                            >
                                                            {expandedRows.has('dt-' + rowKey)
                                                                ? dtypes.slice(1).map(formatDisasterType).join(', ')
                                                                : `+${dtypes.length - 1} more`}
                                                            </button>
                                                        {/if}
                                                        </div>
                                                    {/if}
                                                    </td>

                                                    <!-- Date -->
                                                    <td class="px-3 py-2 text-slate-600 whitespace-nowrap">
                                                    {colValue(row, 'startDate')}
                                                    </td>

                                                    <!-- Locations -->
                                                    <td class="px-3 py-2 text-slate-600 align-top" style="max-width:180px;">
                                                    {#if locs.length === 0}
                                                        <span class="text-slate-300">—</span>
                                                    {:else if expanded}
                                                        <div class="flex flex-col gap-0.5 text-xs leading-snug">
                                                        {#each locs as loc}<span>{loc}</span>{/each}
                                                        </div>
                                                    {:else}
                                                        <div class="text-xs leading-snug">
                                                        <span>{locs[0]}</span>
                                                        {#if expandable}
                                                            <span class="ml-1 text-blue-400 text-[10px] font-medium whitespace-nowrap">+{locs.length - 1} more</span>
                                                        {/if}
                                                        </div>
                                                    {/if}
                                                    </td>
                                                </tr>

                                                <!-- Alternate sub-rows -->
                                                {#if altsExpanded}
                                                    {#each subs as sub, si}
                                                    {@const subLocs = (sub.locations?.value ?? '').split('|').filter(Boolean)}
                                                    {@const subTypes = (sub.disasterType?.value ?? sub.disasterTypes?.value ?? '')
                                                        .split(',').map(s => s.trim()).filter(Boolean)}
                                                    {@const subKey = sub.event?.value ?? `sub-${i}-${si}`}
                                                    {@const subExpanded = expandedRows.has(subKey)}
                                                    <tr class="bg-violet-50/60 border-l-2 border-violet-300">
                                                        <td class="pl-6 pr-3 py-1.5 text-slate-500 max-w-[160px]">
                                                        <div class="flex flex-col gap-0.5">
                                                            <span class="truncate text-xs">{sub.eventName?.value ? bindingDisplay(sub.eventName) : '—'}</span>
                                                            {#if sub.source?.value}
                                                            <span class="w-fit rounded-full bg-slate-200 px-1.5 py-0.5 text-[9px] font-semibold text-slate-500 uppercase tracking-wide">
                                                                {sub.source.value}
                                                            </span>
                                                            {/if}
                                                        </div>
                                                        </td>
                                                        <td class="px-3 py-1.5 text-slate-500 text-xs">
                                                        {subTypes.length ? formatDisasterType(subTypes[0]) : '—'}
                                                        {#if subTypes.length > 1}
                                                            <span class="text-slate-400"> +{subTypes.length - 1}</span>
                                                        {/if}
                                                        </td>
                                                        <td class="px-3 py-1.5 text-slate-500 text-xs whitespace-nowrap">
                                                        {sub.startDate?.value?.split('T')[0] ?? '—'}
                                                        </td>
                                                        <!-- svelte-ignore a11y_click_events_have_key_events -->
                                                        <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
                                                        <td
                                                        class="px-3 py-1.5 text-slate-500 align-top text-xs {subLocs.length > 1 ? 'cursor-pointer' : ''}"
                                                        style="max-width:180px;"
                                                        onclick={() => {
                                                            if (subLocs.length <= 1) return;
                                                            if (subExpanded) expandedRows = new Set([...expandedRows].filter(k => k !== subKey));
                                                            else expandedRows = new Set([...expandedRows, subKey]);
                                                        }}
                                                        >
                                                        {#if subLocs.length === 0}
                                                            <span class="text-slate-300">—</span>
                                                        {:else if subExpanded}
                                                            <div class="flex flex-col gap-0.5">{#each subLocs as l}<span>{l}</span>{/each}</div>
                                                        {:else}
                                                            {subLocs[0]}{#if subLocs.length > 1}<span class="ml-1 text-blue-400 text-[10px]">+{subLocs.length - 1}</span>{/if}
                                                        {/if}
                                                        </td>
                                                    </tr>
                                                    {/each}
                                                {/if}
                                                {/each}
											{/if}
										</tbody>
									</table>
								</div>
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