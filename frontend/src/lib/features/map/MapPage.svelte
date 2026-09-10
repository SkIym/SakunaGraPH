<script>
	import { page as appPage } from '$app/state';
	import { tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import PhilMap from '$lib/components/map/PhilMap.svelte';
	import {
		formatProvName,
		REGION_LABELS,
		REGION_COLORS,
		formatDisasterType,
	} from '$lib/mapData.js';
	import { createMapEventQuery, MAP_PAGE_SIZE } from './eventQuery.svelte.js';
	import { detailViewBoxFor, FULL_MAP_VIEW_BOX, loadMapGeometry } from './geometry.js';

	// ── Map data (loaded once) ───────────────────────────────────────────────
	let pathData = $state([]);
	let tightViewBox = $state(FULL_MAP_VIEW_BOX);
	let mapLoading = $state(true);
	let mapError = $state('');
	let mapRetryToken = $state(0);
	let pathGenerator = null;

	// ── UI state ─────────────────────────────────────────────────────────────
	const requestedProvinceId = appPage.url.searchParams.get('province')?.trim() ?? '';
	let initialProvinceApplied = $state(false);
	let view = $state(
		appPage.url.searchParams.get('view') === 'provinces' || requestedProvinceId
			? 'provinces'
			: 'regions',
	);
	let selected = $state(null); // {type, psgc, id, name}
	let selectedEvent = $state('');
	let EventDetailsComponent = $state(null);
	const detailViewBox = $derived(detailViewBoxFor({ selected, view, pathData, pathGenerator }));

	// ── Results state ────────────────────────────────────────────────────────
	const eventQuery = createMapEventQuery();
	let resultMode = $state('major'); // 'major' | 'incidents'
	let page = $state(1);
	let resultRetryToken = $state(0);

	const totalCount = $derived(eventQuery.countFor(resultMode));
	const totalPages = $derived(Math.max(1, Math.ceil(totalCount / MAP_PAGE_SIZE)));

	// ── Load GeoJSON + build paths ───────────────────────────────────────────
	$effect(() => {
		const _retry = mapRetryToken;
		const controller = new AbortController();
		mapLoading = true;
		mapError = '';
		void (async () => {
			try {
				performance.mark('sakunagraph:map-load-start');
				const geometry = await loadMapGeometry({ signal: controller.signal });
				pathData = geometry.pathData;
				pathGenerator = geometry.pathGenerator;
				tightViewBox = geometry.tightViewBox;
				mapLoading = false;
				await tick();
				await new Promise((resolve) => requestAnimationFrame(resolve));
				performance.mark('sakunagraph:map-rendered');
				performance.measure(
					'sakunagraph:map-initial-render',
					'sakunagraph:map-load-start',
					'sakunagraph:map-rendered',
				);
			} catch (requestError) {
				if (requestError?.name === 'AbortError') return;
				mapError = 'The map could not be loaded. Check your connection and try again.';
				mapLoading = false;
			}
		})();
		return () => controller.abort();
	});

	// A home-preview deep link selects the matching province after its geometry is available.
	$effect(() => {
		if (initialProvinceApplied || !requestedProvinceId || pathData.length === 0) return;
		initialProvinceApplied = true;
		const province = pathData.find((item) => item.gid === requestedProvinceId);
		if (!province) return;
		view = 'provinces';
		handleMapSelect(province);
	});

	// ── When selection, mode, or page changes, fetch data ───────────────────
	$effect(() => {
		if (!selected) {
			eventQuery.reset();
			return;
		}
		const _p = page;
		const _mode = resultMode;
		const _retry = resultRetryToken;
		if (_p < 1) return;
		const controller = new AbortController();
		expandedRows = new Set();
		expandedAlternates = new Set();
		void eventQuery.load({ selected, mode: resultMode, page, signal: controller.signal });
		return () => controller.abort();
	});

	// ── Map interaction handlers ─────────────────────────────────────────────
	function handleMapSelect(item) {
		tooltipItem = null;
		resultMode = 'major';
		page = 1;
		eventQuery.clearResults();

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
	const mapStrokeColor = $derived(view === 'regions' ? 'rgba(55,65,81,0.42)' : '#374151');
	const mapStrokeWidth = $derived(view === 'regions' ? 0.5 : 0.65);

	function deselect() {
		selected = null;
		resultRetryToken = 0;
		eventQuery.reset();
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

	function colValue(row, col) {
		const v = row[col];
		if (Array.isArray(v)) return v.length ? v.join(', ') : '—';
		return v || '—';
	}

	async function showEventDetails(row) {
		if (!row?.event) return;
		if (!EventDetailsComponent) {
			EventDetailsComponent = (await import('$lib/components/EventDetails.svelte')).default;
		}
		selectedEvent = row.event;
	}

	function handleEventRowKeydown(keyboardEvent, row) {
		if (keyboardEvent.key === 'Enter' || keyboardEvent.key === ' ') {
			keyboardEvent.preventDefault();
			showEventDetails(row);
		}
	}

	const DISPLAY_COLS = ['eventName', 'disasterTypes', 'startDate', 'locations'];
	const COL_LABELS = {
		eventName: 'Event',
		disasterTypes: 'Type',
		startDate: 'Date',
		locations: 'Locations',
	};

	let expandedRows = $state(new Set());
	let expandedAlternates = $state(new Set());
</script>

<svelte:head>
	<title>Map · SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

{#if selectedEvent && EventDetailsComponent}
	<EventDetailsComponent event={selectedEvent} onclose={() => (selectedEvent = '')} />
{/if}

<!-- ── Cursor-following hover tooltip ────────────────────────────────────── -->
{#if tooltipItem}
	<div
		class="map-hover-tooltip fixed z-50 pointer-events-none rounded-lg bg-slate-800/90 px-3 py-1.5 text-xs font-medium text-white shadow-lg"
		style="left:{tooltipX +
			16}px; top:{tooltipY}px; transform:translateY(-50%); backdrop-filter:blur(4px);"
	>
		{getHoverLabel(tooltipItem)}
	</div>
{/if}

<!-- ── Full-screen layout container ─────────────────────────────────────── -->
<div class="map-workspace relative" class:has-selection={Boolean(selected)}>
	<!-- ── View toggle — hidden when a region/province is selected ──────────── -->
	{#if !selected}
		<div
			class="map-view-toggle absolute top-4 left-1/2 -translate-x-1/2 z-10 flex gap-1 rounded-full border border-slate-200/80 bg-white/90 p-1 shadow-sm"
			style="backdrop-filter:blur(10px);"
		>
			<button
				type="button"
				onclick={() => switchView('regions')}
				aria-pressed={view === 'regions'}
				class="min-h-11 rounded-full px-4 py-2 text-xs font-semibold transition-all duration-150
				{view === 'regions' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:text-slate-700'}"
			>
				By Region
			</button>
			<button
				type="button"
				onclick={() => switchView('provinces')}
				aria-pressed={view === 'provinces'}
				class="min-h-11 rounded-full px-4 py-2 text-xs font-semibold transition-all duration-150
				{view === 'provinces'
					? 'bg-slate-800 text-white shadow-sm'
					: 'text-slate-500 hover:text-slate-700'}"
			>
				By Province
			</button>
		</div>
	{/if}

	<!-- ── Map panel (left side, shrinks on selection) ─────────────────────── -->
	<div class="map-panel transition-all duration-500 ease-out">
		{#if selected}
			<!-- Zoomed detail map fills the whole panel -->
			<div class="map-detail-canvas absolute inset-0 p-4">
				{#if pathData.length > 0}
					<PhilMap
						{pathData}
						viewBox={detailViewBox ?? FULL_MAP_VIEW_BOX}
						{view}
						{selected}
						interactive={false}
						strokeWidth={1.4}
					/>
				{/if}
			</div>

			<!-- Compact back button + mini thumbnail — upper-left corner -->
			<div class="absolute top-3 left-3 z-10">
				<button
					type="button"
					class="min-h-11 cursor-pointer rounded-xl border border-slate-200/80 bg-white/95 p-2 text-left shadow-md transition-colors hover:bg-slate-50"
					style="backdrop-filter:blur(8px);"
					aria-label="Back to full map"
					onclick={deselect}
				>
					<div class="flex items-center gap-1.5 mb-1.5">
						<svg
							xmlns="http://www.w3.org/2000/svg"
							width="10"
							height="10"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2.5"
							stroke-linecap="round"
							stroke-linejoin="round"
							class="text-slate-500"
						>
							<path d="M19 12H5" /><path d="m12 5-7 7 7 7" />
						</svg>
						<span class="text-[9px] font-semibold uppercase tracking-wider text-slate-400"
							>Back</span
						>
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
				</button>
			</div>
		{:else}
			<!-- Full map — row layout: map on left, label on right -->
			<div class="map-overview flex h-full items-center justify-center gap-10 px-8 pt-12">
				{#if mapLoading}
					<p class="text-slate-600 text-sm">Loading Philippine map…</p>
				{:else if mapError}
					<div
						class="max-w-sm rounded-xl border border-red-200 bg-red-50 px-5 py-4 text-center"
						role="alert"
					>
						<p class="break-words text-sm leading-6 text-red-800">{mapError}</p>
						<button
							type="button"
							onclick={() => (mapRetryToken += 1)}
							class="touch-target mt-2 rounded-lg px-3 text-sm font-semibold text-red-800 underline underline-offset-4 hover:bg-red-100"
						>
							Try again
						</button>
					</div>
				{:else}
					<!-- Map — flex-shrink:0 + fixed aspect-ratio keeps width stable on hover -->
					<div class="map-canvas-shell">
						<PhilMap
							{pathData}
							viewBox={FULL_MAP_VIEW_BOX}
							{view}
							selected={null}
							interactive={true}
							strokeWidth={mapStrokeWidth}
							strokeColor={mapStrokeColor}
							colorMap={mapColorMap}
							onselect={handleMapSelect}
							onhover={(item, x, y) => {
								tooltipItem = item;
								tooltipX = x;
								tooltipY = y;
							}}
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

					<div class="map-guidance pointer-events-none flex-shrink-0">
						<p
							class="font-black text-slate-700 leading-snug"
							style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.8rem, 3vw, 2.5rem); overflow-wrap:break-word; word-break:break-word;"
						>
							{tooltipItem ? getHoverLabel(tooltipItem) : 'Philippines'}
						</p>
						<p class="mt-1 text-[11px] font-medium uppercase tracking-widest text-slate-500">
							{tooltipItem ? (view === 'regions' ? 'Region' : 'Province') : 'Choose an area'}
						</p>
						<p class="mt-3 text-[15px] leading-relaxed text-slate-600">
							Select a {view === 'regions' ? 'region' : 'province'} to explore its disaster records.
						</p>
					</div>
				{/if}
			</div>
		{/if}
	</div>

	<!-- ── Results panel (right side, appears on selection) ────────────────── -->
	<div
		class="map-results-panel border-slate-200/60 bg-white/95 transition-all duration-500 ease-out"
		style="backdrop-filter:blur(12px);"
	>
		{#if selected}
			<!-- Outer flex: two spacers push content to vertical center -->
			<div class="flex h-full flex-col">
				<div class="results-spacer flex-1 min-h-0"></div>

				<!-- Content block — vertically centered, max 85% of panel height -->
				<div class="results-content mx-4 flex flex-col sm:mx-6">
					<!-- Header -->
					<div class="pb-3 flex-shrink-0">
						<div class="flex items-start justify-between gap-3">
							<div class="min-w-0">
								<p class="text-[10px] font-semibold uppercase tracking-widest text-slate-400 mb-1">
									{selected.type === 'region' ? 'Region' : 'Province'}
								</p>
								<h2
									class="break-words font-bold leading-tight text-slate-800 [overflow-wrap:anywhere]"
									style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.1rem,2.5vw,1.6rem);"
								>
									{selected.name}
								</h2>

								{#if eventQuery.loading && !eventQuery.results}
									<div class="mt-2 flex items-center gap-2 text-slate-400 text-sm">
										<svg
											class="animate-spin"
											xmlns="http://www.w3.org/2000/svg"
											width="14"
											height="14"
											viewBox="0 0 24 24"
											fill="none"
											stroke="currentColor"
											stroke-width="2.5"
											stroke-linecap="round"
											stroke-linejoin="round"
										>
											<path d="M21 12a9 9 0 1 1-6.219-8.56" />
										</svg>
										Loading records…
									</div>
								{:else}
									<!-- Primary count (active mode) -->
									<p
										class="mt-2 font-bold leading-none"
										style="color:#dc2626; font-size:clamp(1.3rem,2.5vw,1.8rem);"
									>
										{totalCount.toLocaleString()}
										<span
											class="font-semibold"
											style="color:#dc2626; font-size:clamp(0.8rem,1.4vw,1rem);"
										>
											{resultMode === 'major' ? 'major disaster event' : 'incident'}{totalCount ===
											1
												? ''
												: 's'}
										</span>
									</p>
									<!-- Secondary count (inactive mode) -->
									<p class="mt-0.5 text-xs text-slate-400">
										{#if resultMode === 'major'}
											{eventQuery.incidentCount.toLocaleString()} incident{eventQuery.incidentCount ===
											1
												? ''
												: 's'}
										{:else}
											{eventQuery.majorCount.toLocaleString()} major disaster event{eventQuery.majorCount ===
											1
												? ''
												: 's'}
										{/if}
									</p>
								{/if}

								<!-- Toggle -->
								<div
									class="mt-3 flex w-fit gap-1 rounded-full border border-slate-200 bg-slate-50 p-0.5"
								>
									<button
										onclick={() => switchResultMode('major')}
										class="min-h-11 rounded-full px-3 py-2 text-[11px] font-semibold transition-all duration-150
										{resultMode === 'major'
											? 'bg-slate-800 text-white shadow-sm'
											: 'text-slate-500 hover:text-slate-700'}">Major Events</button
									>
									<button
										onclick={() => switchResultMode('incidents')}
										class="min-h-11 rounded-full px-3 py-2 text-[11px] font-semibold transition-all duration-150
										{resultMode === 'incidents'
											? 'bg-slate-800 text-white shadow-sm'
											: 'text-slate-500 hover:text-slate-700'}">Incidents</button
									>
								</div>
							</div>
							<button
								onclick={deselect}
								class="mt-1 flex h-11 w-11 flex-shrink-0 items-center justify-center rounded-lg text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-700"
								aria-label="Close results"
							>
								<svg
									xmlns="http://www.w3.org/2000/svg"
									width="16"
									height="16"
									viewBox="0 0 24 24"
									fill="none"
									stroke="currentColor"
									stroke-width="2.5"
									stroke-linecap="round"
									stroke-linejoin="round"
								>
									<line x1="18" y1="6" x2="6" y2="18" /><line x1="6" y1="6" x2="18" y2="18" />
								</svg>
							</button>
						</div>
					</div>

					<!-- Divider -->
					<div class="border-t border-slate-100 flex-shrink-0"></div>

					<!-- Results body — scrollable -->
					<div class="overflow-y-auto py-4 flex-1 min-h-0">
						{#if eventQuery.error}
							<div
								class="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
								role="alert"
							>
								<p class="break-words leading-6 [overflow-wrap:anywhere]">{eventQuery.error}</p>
								<button
									type="button"
									onclick={() => (resultRetryToken += 1)}
									class="touch-target mt-2 rounded-lg px-2 text-xs font-semibold underline underline-offset-4 hover:bg-red-100"
								>
									Try again
								</button>
							</div>
						{:else if eventQuery.results}
							{@const rows = eventQuery.results ?? []}
							{#if rows.length === 0}
								<div class="py-10 text-center text-slate-400 text-sm">
									No {resultMode === 'major' ? 'major disaster events' : 'incidents'} are recorded for
									this area in the current graph.
								</div>
							{:else}
								<div
									class="map-results-table-wrap overflow-x-auto rounded-xl border border-slate-200/80 shadow-sm"
								>
									<table class="map-results-table w-full text-xs">
										<thead>
											<tr class="bg-slate-50 border-b border-slate-200">
												{#each DISPLAY_COLS as col}
													<th
														class="px-3 py-2.5 text-left font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap"
													>
														{COL_LABELS[col]}
													</th>
												{/each}
											</tr>
										</thead>
										<tbody class="divide-y divide-slate-100">
											{#if eventQuery.loading}
												{#each Array(MAP_PAGE_SIZE) as _, i}
													<tr class={i % 2 === 0 ? '' : 'bg-slate-50/40'}>
														<td class="px-3 py-2"
															><div
																class="h-3 w-32 rounded-full bg-slate-200 animate-pulse"
															></div></td
														>
														<td class="px-3 py-2"
															><div
																class="h-3 w-20 rounded-full bg-slate-200 animate-pulse"
															></div></td
														>
														<td class="px-3 py-2"
															><div
																class="h-3 w-16 rounded-full bg-slate-200 animate-pulse"
															></div></td
														>
														<td class="px-3 py-2"
															><div
																class="h-3 w-24 rounded-full bg-slate-200 animate-pulse"
															></div></td
														>
													</tr>
												{/each}
											{:else}
												{#each eventQuery.groupedResults ?? [] as { row, subs }, i}
													{@const rowKey = row.event ?? String(i)}
													{@const locs = row.locations ?? []}
													{@const dtypes = row.disasterTypes ?? []}
													{@const expandable = locs.length > 1}
													{@const hasAlts = subs.length > 0}
													{@const altsExpanded = expandedAlternates.has(rowKey)}

													<!-- Representative row -->
													<tr
														role="button"
														tabindex="0"
														aria-label="View details for {row.eventName || 'unnamed event'}"
														class="cursor-pointer align-middle transition-colors hover:bg-blue-50/60 focus:bg-blue-50/60 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-blue-300 {i %
															2 ===
														0
															? ''
															: 'bg-slate-50/40'}"
														onclick={() => showEventDetails(row)}
														onkeydown={(keyboardEvent) => handleEventRowKeydown(keyboardEvent, row)}
													>
														<!-- Event name + alternates badge -->
														<td
															data-label="Event"
															class="max-w-[160px] px-3 py-2 align-middle text-slate-600"
															title={row.eventName ?? ''}
														>
															<div class="flex flex-col gap-1">
																<span class="truncate">{colValue(row, 'eventName')}</span>
																{#if hasAlts}
																	<button
																		type="button"
																		class="w-fit rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-semibold text-violet-600 hover:bg-violet-200 transition-colors"
																		onclick={(e) => {
																			e.stopPropagation();
																			if (altsExpanded)
																				expandedAlternates = new Set(
																					[...expandedAlternates].filter((k) => k !== rowKey),
																				);
																			else
																				expandedAlternates = new Set([
																					...expandedAlternates,
																					rowKey,
																				]);
																		}}
																	>
																		{altsExpanded ? '▾' : '▸'}
																		{subs.length} alternate{subs.length > 1 ? 's' : ''}
																	</button>
																{/if}
																{#if row.source}
																	<span
																		class="w-fit rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-400 uppercase tracking-wide"
																	>
																		{row.source}
																	</span>
																{/if}
															</div>
														</td>

														<!-- Disaster type: first + +N more -->
														<td data-label="Type" class="px-3 py-2 align-middle text-slate-600">
															{#if dtypes.length === 0}
																<span class="text-slate-300">—</span>
															{:else}
																<div class="flex flex-col gap-0.5">
																	<span>{formatDisasterType(dtypes[0])}</span>
																	{#if dtypes.length > 1}
																		<button
																			type="button"
																			class="w-fit text-blue-400 text-[10px] font-medium"
																			onclick={(e) => {
																				e.stopPropagation();
																				// reuse expandedRows with a dtype- prefix key
																				const dtKey = 'dt-' + rowKey;
																				if (expandedRows.has(dtKey))
																					expandedRows = new Set(
																						[...expandedRows].filter((k) => k !== dtKey),
																					);
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
														<td
															data-label="Date"
															class="px-3 py-2 align-middle text-slate-600 whitespace-nowrap"
														>
															{colValue(row, 'startDate')}
														</td>

														<!-- Locations -->
														<td
															data-label="Locations"
															class="px-3 py-2 align-middle text-slate-600"
															style="max-width:180px;"
														>
															{#if locs.length === 0}
																<span class="text-slate-300">—</span>
															{:else}
																<div class="text-xs leading-snug">
																	<span>{locs[0]}</span>
																	{#if expandable}
																		<span
																			class="ml-1 text-blue-400 text-[10px] font-medium whitespace-nowrap"
																			>+{locs.length - 1} more</span
																		>
																	{/if}
																</div>
															{/if}
														</td>
													</tr>

													<!-- Alternate sub-rows -->
													{#if altsExpanded}
														{#each subs as sub (sub.event)}
															{@const subLocs = sub.locations ?? []}
															{@const subTypes = sub.disasterTypes ?? []}
															<tr
																role="button"
																tabindex="0"
																aria-label="View details for {sub.eventName || 'unnamed event'}"
																class="cursor-pointer border-l-2 border-violet-300 bg-violet-50/60 align-middle transition hover:bg-violet-100/70 focus:bg-violet-100/70 focus:outline-none"
																onclick={() => showEventDetails(sub)}
																onkeydown={(keyboardEvent) =>
																	handleEventRowKeydown(keyboardEvent, sub)}
															>
																<td
																	data-label="Alternate event"
																	class="max-w-[160px] py-1.5 pr-3 pl-6 align-middle text-slate-500"
																>
																	<div class="flex flex-col gap-0.5">
																		<span class="truncate text-xs">{sub.eventName || '—'}</span>
																		{#if sub.source}
																			<span
																				class="w-fit rounded-full bg-slate-200 px-1.5 py-0.5 text-[9px] font-semibold text-slate-500 uppercase tracking-wide"
																			>
																				{sub.source}
																			</span>
																		{/if}
																	</div>
																</td>
																<td
																	data-label="Type"
																	class="px-3 py-1.5 align-middle text-xs text-slate-500"
																>
																	{subTypes.length ? formatDisasterType(subTypes[0]) : '—'}
																	{#if subTypes.length > 1}
																		<span class="text-slate-400"> +{subTypes.length - 1}</span>
																	{/if}
																</td>
																<td
																	data-label="Date"
																	class="px-3 py-1.5 align-middle text-xs whitespace-nowrap text-slate-500"
																>
																	{sub.startDate || '—'}
																</td>
																<td
																	data-label="Locations"
																	class="px-3 py-1.5 align-middle text-xs text-slate-500"
																	style="max-width:180px;"
																>
																	{#if subLocs.length === 0}
																		<span class="text-slate-300">—</span>
																	{:else}
																		{subLocs[0]}{#if subLocs.length > 1}<span
																				class="ml-1 text-blue-400 text-[10px]"
																				>+{subLocs.length - 1}</span
																			>{/if}
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
						<div
							class="flex-shrink-0 border-t border-slate-100 pt-3 pb-2 flex flex-col items-center gap-2"
						>
							<span class="text-xs text-slate-400">Page {page} of {totalPages}</span>
							<div class="pagination-pages flex items-center gap-1">
								<button
									onclick={() => {
										page = Math.max(1, page - 1);
									}}
									disabled={page === 1 || eventQuery.loading}
									class="min-h-11 rounded-lg border border-slate-200 px-3 py-2 text-xs font-medium text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
									>← Prev</button
								>

								{#each paginationPages as p}
									{#if p === '…'}
										<span class="px-1 text-slate-400 text-xs">…</span>
									{:else}
										<button
											onclick={() => {
												page = p;
											}}
											disabled={eventQuery.loading}
											class="page-number flex h-11 w-11 items-center justify-center rounded-lg border text-xs font-medium transition-colors
											{page === p
												? 'current-page bg-slate-800 text-white border-slate-800'
												: 'border-slate-200 text-slate-600 hover:bg-slate-50'}">{p}</button
										>
									{/if}
								{/each}

								<button
									onclick={() => {
										page = Math.min(totalPages, page + 1);
									}}
									disabled={page === totalPages || eventQuery.loading}
									class="min-h-11 rounded-lg border border-slate-200 px-3 py-2 text-xs font-medium text-slate-600 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
									>Next →</button
								>
							</div>
						</div>
					{/if}
				</div>

				<div class="results-spacer flex-1 min-h-0"></div>
			</div>
		{/if}
	</div>
</div>

<style>
	.map-workspace {
		z-index: 1;
		min-height: calc(100dvh - 52px);
		overflow-x: hidden;
	}

	.map-panel {
		position: relative;
		width: 100%;
		height: calc(100dvh - 52px);
	}

	.map-workspace.has-selection .map-panel {
		height: clamp(18rem, 42dvh, 24rem);
	}

	.map-overview {
		flex-direction: column;
		gap: 1rem;
		padding: 4.75rem 1rem 1.5rem;
	}

	.map-canvas-shell {
		height: min(56dvh, 30rem);
		width: min(100%, 26rem);
		aspect-ratio: 7 / 8;
		flex: 0 1 auto;
	}

	.map-guidance {
		width: min(100%, 28rem);
		text-align: center;
	}

	.map-results-panel {
		position: relative;
		width: 100%;
		min-height: calc(58dvh - 52px);
		border-top-width: 1px;
		overflow: visible;
		padding-bottom: env(safe-area-inset-bottom);
	}

	.map-workspace:not(.has-selection) .map-results-panel {
		display: none;
	}

	.results-content {
		max-height: none;
		overflow: visible;
		padding-block: 1rem;
	}

	.results-spacer {
		display: none;
	}

	@media (max-width: 767px) {
		.map-hover-tooltip {
			display: none;
		}

		.map-view-toggle {
			top: 0.75rem;
			width: calc(100% - 2rem);
			justify-content: stretch;
		}

		.map-view-toggle button {
			flex: 1;
		}

		.map-workspace.has-selection .map-detail-canvas {
			padding: 2.75rem 1rem 0.5rem;
		}

		.map-results-table-wrap {
			overflow: visible;
			border: 0;
			box-shadow: none;
		}

		.map-results-table thead {
			display: none;
		}

		.map-results-table,
		.map-results-table tbody,
		.map-results-table tr,
		.map-results-table td {
			display: block;
			width: 100%;
		}

		.map-results-table tbody {
			display: grid;
			gap: 0.75rem;
		}

		.map-results-table tr {
			border: 1px solid #e2e8f0;
			border-radius: 0.75rem;
			background: rgba(255, 255, 255, 0.88);
			padding: 0.5rem 0.75rem;
		}

		.map-results-table td {
			display: grid;
			grid-template-columns: minmax(5.5rem, 0.38fr) minmax(0, 1fr);
			gap: 0.75rem;
			max-width: none !important;
			padding: 0.45rem 0;
			white-space: normal;
		}

		.map-results-table td::before {
			content: attr(data-label);
			font-size: 0.6875rem;
			font-weight: 700;
			letter-spacing: 0.06em;
			text-transform: uppercase;
			color: #64748b;
		}

		.map-results-table td > :global(*) {
			min-width: 0;
		}

		.pagination-pages .page-number,
		.pagination-pages > span {
			display: none;
		}

		.pagination-pages .current-page {
			display: flex;
		}
	}

	@media (min-width: 768px) and (max-width: 1023px) {
		.map-overview {
			padding-inline: 2rem;
		}

		.map-canvas-shell {
			height: min(58dvh, 34rem);
			width: min(100%, 30rem);
		}
	}

	@media (min-width: 1024px) {
		.map-workspace {
			height: calc(100dvh - 52px);
			min-height: 0;
			overflow: hidden;
		}

		.map-panel {
			position: absolute;
			top: 0;
			left: 0;
			height: 100%;
			width: 100%;
		}

		.map-workspace.has-selection .map-panel {
			height: 100%;
			width: 42%;
		}

		.map-overview {
			flex-direction: row;
			gap: 2.5rem;
			padding: 3rem 2rem 0;
		}

		.map-canvas-shell {
			height: calc(100dvh - 120px);
			width: auto;
			max-width: 58%;
			flex: 0 0 auto;
		}

		.map-guidance {
			width: 18.75rem;
			text-align: left;
		}

		.map-results-panel {
			position: absolute;
			top: 0;
			right: 0;
			height: 100%;
			width: 0;
			min-height: 0;
			border-top-width: 0;
			border-left-width: 1px;
			overflow: hidden;
			padding-bottom: 0;
		}

		.map-workspace.has-selection .map-results-panel {
			width: 58%;
		}

		.results-content {
			max-height: 85dvh;
			overflow: hidden;
			padding-block: 0;
		}

		.results-spacer {
			display: block;
		}
	}
</style>
