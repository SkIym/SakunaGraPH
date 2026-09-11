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
	import {
		detailViewBoxFor,
		FULL_MAP_VIEW_BOX,
		loadMapGeometry,
		NCR_REGION_PSGC,
	} from './geometry.js';

	// ── Map data (loaded once) ───────────────────────────────────────────────
	let pathData = $state([]);
	let ncrCityPathData = $state([]);
	let ncrViewBox = $state(FULL_MAP_VIEW_BOX);
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
	const selectedIsNcrLocality = $derived(['city', 'municipality'].includes(selected?.type));
	const detailPathData = $derived(selectedIsNcrLocality ? ncrCityPathData : pathData);
	const detailMapView = $derived(selectedIsNcrLocality ? 'cities' : view);
	const overviewSelection = $derived(
		selectedIsNcrLocality ? { id: NCR_REGION_PSGC, psgc: NCR_REGION_PSGC } : selected,
	);
	const detailViewBox = $derived(
		detailViewBoxFor({
			selected,
			view: detailMapView,
			pathData: detailPathData,
			pathGenerator,
		}),
	);

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
				ncrCityPathData = geometry.ncrCityPathData;
				ncrViewBox = geometry.ncrViewBox;
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
		const province = pathData.find(
			(item) =>
				item.gid === requestedProvinceId ||
				(requestedProvinceId.startsWith('13') && item.gid === NCR_REGION_PSGC),
		);
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
		if (view === 'provinces' && item.areaType === 'ncr') {
			openNcrMagnifier(true);
			return;
		}

		tooltipItem = null;
		closeNcrMagnifier();
		resultMode = 'major';
		page = 1;
		eventQuery.clearResults();

		if (view === 'regions') {
			const psgc = item.regionPsgc;
			const name = REGION_LABELS[psgc] ?? `Region ${psgc}`;
			selected = { type: 'region', psgc, name, id: psgc };
		} else if (['city', 'municipality'].includes(item.areaType)) {
			selected = {
				type: item.areaType,
				psgc: NCR_REGION_PSGC,
				id: item.gid,
				name: item.name,
			};
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
	const mapStrokeColor = $derived(
		view === 'regions' ? 'rgba(55,65,81,0.42)' : 'var(--color-brand)',
	);
	const mapStrokeWidth = $derived(view === 'regions' ? 0.5 : 0.65);

	function deselect() {
		selected = null;
		closeNcrMagnifier();
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
	let ncrMagnifierOpen = $state(false);
	let ncrMagnifierPinned = $state(false);
	let ncrMagnifierHovered = false;
	let ncrCloseTimer = null;

	function areaTypeLabel(item) {
		if (item?.areaType === 'ncr') return 'NCR city detail';
		if (item?.areaType === 'city') return 'City';
		if (item?.areaType === 'municipality') return 'Municipality';
		return view === 'regions' ? 'Region' : 'Province';
	}

	function selectedTypeLabel() {
		if (selected?.type === 'city') return 'City';
		if (selected?.type === 'municipality') return 'Municipality';
		return selected?.type === 'region' ? 'Region' : 'Province';
	}

	function clearNcrCloseTimer() {
		if (ncrCloseTimer === null) return;
		clearTimeout(ncrCloseTimer);
		ncrCloseTimer = null;
	}

	function openNcrMagnifier(pinned = false) {
		clearNcrCloseTimer();
		ncrMagnifierOpen = true;
		if (pinned) ncrMagnifierPinned = true;
	}

	function scheduleNcrMagnifierClose() {
		clearNcrCloseTimer();
		if (ncrMagnifierPinned) return;
		ncrCloseTimer = setTimeout(() => {
			if (!ncrMagnifierHovered) ncrMagnifierOpen = false;
			ncrCloseTimer = null;
		}, 180);
	}

	function closeNcrMagnifier() {
		clearNcrCloseTimer();
		ncrMagnifierOpen = false;
		ncrMagnifierPinned = false;
		ncrMagnifierHovered = false;
	}

	function handleMapHover(item, x, y) {
		tooltipItem = item;
		tooltipX = x;
		tooltipY = y;
		if (view === 'provinces' && item?.areaType === 'ncr') {
			openNcrMagnifier();
		} else if (!item) {
			scheduleNcrMagnifierClose();
		}
	}

	function handleNcrMagnifierEnter() {
		ncrMagnifierHovered = true;
		clearNcrCloseTimer();
	}

	function handleNcrMagnifierLeave() {
		ncrMagnifierHovered = false;
		scheduleNcrMagnifierClose();
	}

	function mapAreaLabel(item) {
		if (view === 'provinces' && item.areaType === 'ncr') {
			return 'Open National Capital Region city map';
		}
		return `Select ${getHoverLabel(item)}`;
	}

	function getHoverLabel(item) {
		if (!item) return '';
		if (view === 'regions') return REGION_LABELS[item.regionPsgc] ?? formatProvName(item.name);
		return item.areaType === 'ncr' ? 'National Capital Region (NCR)' : formatProvName(item.name);
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
	<meta
		name="description"
		content="Explore Philippine disaster records by region, province, or NCR city and inspect the linked events, dates, sources, and locations."
	/>
</svelte:head>

<NodeCanvas />

{#if selectedEvent && EventDetailsComponent}
	<EventDetailsComponent event={selectedEvent} onclose={() => (selectedEvent = '')} />
{/if}

<!-- ── Cursor-following hover tooltip ────────────────────────────────────── -->
{#if tooltipItem && !(tooltipItem.areaType === 'ncr' && ncrMagnifierOpen)}
	<div
		role="tooltip"
		class="map-hover-tooltip pointer-events-none fixed z-50 rounded-lg px-3 py-2 text-xs font-medium"
		style="left:{tooltipX + 16}px; top:{tooltipY}px; transform:translateY(-50%);"
	>
		<span class="tooltip-type">{areaTypeLabel(tooltipItem)}</span>
		{getHoverLabel(tooltipItem)}
	</div>
{/if}

<!-- ── Full-screen layout container ─────────────────────────────────────── -->
<a href="#map-explorer" class="map-skip-link">Skip to map explorer</a>

<main
	id="map-explorer"
	tabindex="-1"
	class="map-workspace relative"
	class:has-selection={Boolean(selected)}
>
	<h1 class="sr-only">Philippine disaster map</h1>
	<!-- ── View toggle — hidden when a region/province is selected ──────────── -->
	{#if !selected}
		<div class="map-view-toggle absolute z-10" role="group" aria-label="Map geography level">
			<span class="view-toggle-label">Explore by</span>
			<button
				type="button"
				onclick={() => switchView('regions')}
				aria-pressed={view === 'regions'}
				class:active-map-view={view === 'regions'}
				class="map-view-button touch-target"
			>
				By Region
			</button>
			<button
				type="button"
				onclick={() => switchView('provinces')}
				aria-pressed={view === 'provinces'}
				class:active-map-view={view === 'provinces'}
				class="map-view-button touch-target"
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
				{#if detailPathData.length > 0}
					<PhilMap
						pathData={detailPathData}
						viewBox={detailViewBox ?? FULL_MAP_VIEW_BOX}
						view={detailMapView}
						{selected}
						interactive={false}
						strokeWidth={1.4}
						strokeColor={mapStrokeColor}
						colorMap={mapColorMap}
					/>
				{/if}
			</div>

			<!-- Compact back button + mini thumbnail — upper-left corner -->
			<div class="map-back-position absolute z-10">
				<button
					type="button"
					class="map-back-control min-h-11 cursor-pointer text-left"
					aria-label="Back to full map"
					onclick={deselect}
				>
					<div class="map-back-label flex items-center gap-2">
						<svg
							aria-hidden="true"
							width="14"
							height="14"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
						>
							<path d="M19 12H5" /><path d="m12 5-7 7 7 7" />
						</svg>
						<span>Full map</span>
					</div>
					<div class="map-thumbnail" aria-hidden="true">
						{#if pathData.length > 0}
							<PhilMap
								{pathData}
								viewBox={tightViewBox}
								{view}
								selected={overviewSelection}
								colorMap={mapColorMap}
								interactive={false}
								strokeWidth={0.15}
								strokeColor="rgba(71,85,105,0.34)"
							/>
						{/if}
					</div>
				</button>
			</div>
		{:else}
			<!-- Full map — row layout: map on left, label on right -->
			<div class="map-overview flex h-full items-center justify-center">
				{#if mapLoading}
					<div class="map-loading" role="status">
						<div class="map-loading-shape" aria-hidden="true">
							<span></span><span></span><span></span><span></span>
						</div>
						<p>Loading Philippine map…</p>
					</div>
				{:else if mapError}
					<div class="map-error" role="alert">
						<p class="map-error-label">Map unavailable</p>
						<p class="mt-2 break-words text-sm leading-6">{mapError}</p>
						<button
							type="button"
							onclick={() => (mapRetryToken += 1)}
							class="map-retry touch-target mt-3"
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
							getAreaLabel={mapAreaLabel}
							getAreaExpanded={(item) => (item.areaType === 'ncr' ? ncrMagnifierOpen : undefined)}
							getAreaControls={(item) =>
								item.areaType === 'ncr' ? 'ncr-city-magnifier' : undefined}
							getAreaHitStrokeWidth={(item) => (item.areaType === 'ncr' ? 12 : 0)}
							onselect={handleMapSelect}
							onhover={handleMapHover}
						/>
					</div>

					{#if view === 'provinces' && ncrMagnifierOpen && ncrCityPathData.length > 0}
						<aside
							id="ncr-city-magnifier"
							class="ncr-magnifier"
							class:is-pinned={ncrMagnifierPinned}
							aria-label="National Capital Region city selector"
							onmouseenter={handleNcrMagnifierEnter}
							onmouseleave={handleNcrMagnifierLeave}
							onfocusin={handleNcrMagnifierEnter}
							onfocusout={handleNcrMagnifierLeave}
						>
							<header class="ncr-magnifier-header">
								<div>
									<h2>Metro Manila</h2>
									<p>16 cities · Pateros</p>
								</div>
								<button
									type="button"
									class="ncr-close touch-target"
									onclick={closeNcrMagnifier}
									aria-label="Close NCR city map"
								>
									<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
										<path d="m7 7 10 10M17 7 7 17" />
									</svg>
								</button>
							</header>
							<div class="ncr-map-canvas">
								<PhilMap
									pathData={ncrCityPathData}
									viewBox={ncrViewBox}
									view="cities"
									selected={null}
									interactive={true}
									strokeWidth={0.8}
									strokeColor="var(--color-brand)"
									hoverFill="var(--color-accent-soft)"
									ariaLabel="Map of National Capital Region cities and Pateros"
									getAreaLabel={(item) => `Select ${areaTypeLabel(item)} ${item.name}`}
									getAreaHitStrokeWidth={() => 5}
									onselect={handleMapSelect}
									onhover={handleMapHover}
								/>
							</div>
							<p class="ncr-magnifier-instruction">
								{ncrMagnifierPinned
									? 'Choose a city or Pateros to inspect its records.'
									: 'Select NCR to keep this city map open.'}
							</p>
						</aside>
					{/if}

					<div class="map-guidance pointer-events-none flex-shrink-0">
						<div class="map-section-marker">
							<span aria-hidden="true">01</span>
							<span class="map-marker-rule" aria-hidden="true"></span>
							<span>Geographic explorer</span>
						</div>
						<p class="map-page-title">Disaster map</p>
						<div class="current-geography">
							<p class="current-geography-label">
								{tooltipItem ? areaTypeLabel(tooltipItem) : 'National view'}
							</p>
							<p class="current-geography-name">
								{tooltipItem ? getHoverLabel(tooltipItem) : 'Philippines'}
							</p>
						</div>
						<p class="map-guidance-copy">
							{view === 'regions'
								? 'Select a region to explore its disaster records.'
								: 'Select a province. NCR opens a detailed city map.'}
						</p>
						<div class="map-legend" role="group" aria-label="Map interaction legend">
							<span><i class="legend-outline" aria-hidden="true"></i>Boundary</span>
							<span><i class="legend-current" aria-hidden="true"></i>Current area</span>
						</div>
					</div>
				{/if}
			</div>
		{/if}
	</div>

	<!-- ── Results panel (right side, appears on selection) ────────────────── -->
	<aside
		class="map-results-panel transition-all duration-500 ease-out"
		aria-label="Selected area records"
	>
		{#if selected}
			<!-- Outer flex: two spacers push content to vertical center -->
			<div class="flex h-full flex-col">
				<div class="results-spacer flex-1 min-h-0"></div>

				<!-- Content block — vertically centered, max 85% of panel height -->
				<div class="results-content mx-4 flex flex-col sm:mx-6">
					<!-- Header -->
					<header class="results-header flex-shrink-0">
						<div class="flex items-start justify-between gap-3">
							<div class="min-w-0">
								<p class="selection-kicker">
									{selectedTypeLabel()}
								</p>
								<h2 class="selection-title">
									{selected.name}
								</h2>

								{#if eventQuery.loading && !eventQuery.results}
									<div class="results-loading mt-3" role="status">
										<span aria-hidden="true"></span><span aria-hidden="true"></span>
										<span class="sr-only">Loading records…</span>
									</div>
								{:else}
									<p class="result-count mt-3">
										{totalCount.toLocaleString()}
										<span class="result-count-label">
											{resultMode === 'major' ? 'major disaster event' : 'incident'}{totalCount ===
											1
												? ''
												: 's'}
										</span>
									</p>
									<p class="secondary-count mt-1">
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

								<div class="result-mode-toggle mt-4" role="group" aria-label="Record type">
									<button
										type="button"
										onclick={() => switchResultMode('major')}
										aria-pressed={resultMode === 'major'}
										class:active-result-mode={resultMode === 'major'}
										class="result-mode-button touch-target">Major Events</button
									>
									<button
										type="button"
										onclick={() => switchResultMode('incidents')}
										aria-pressed={resultMode === 'incidents'}
										class:active-result-mode={resultMode === 'incidents'}
										class="result-mode-button touch-target">Incidents</button
									>
								</div>
							</div>
							<button
								type="button"
								onclick={deselect}
								class="results-close flex h-11 w-11 flex-shrink-0 items-center justify-center"
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
					</header>

					<!-- Divider -->
					<div class="results-rule flex-shrink-0"></div>

					<!-- Results body — scrollable -->
					<div class="results-body min-h-0 flex-1 overflow-y-auto py-4">
						{#if eventQuery.error}
							<div class="result-error" role="alert">
								<p class="result-error-label">Records unavailable</p>
								<p class="break-words leading-6 [overflow-wrap:anywhere]">{eventQuery.error}</p>
								<button
									type="button"
									onclick={() => (resultRetryToken += 1)}
									class="result-retry touch-target mt-2"
								>
									Try again
								</button>
							</div>
						{:else if eventQuery.results}
							{@const rows = eventQuery.results ?? []}
							{#if rows.length === 0}
								<div class="result-empty">
									<span class="empty-symbol" aria-hidden="true">∅</span>
									<p class="empty-title">No matching records</p>
									<p class="empty-copy">
										No {resultMode === 'major' ? 'major disaster events' : 'incidents'} are recorded for
										this area in the current graph.
									</p>
									<button type="button" class="empty-action touch-target" onclick={deselect}>
										Choose another area
									</button>
								</div>
							{:else}
								<div class="map-results-table-wrap overflow-x-auto">
									<table class="map-results-table w-full text-xs">
										<thead>
											<tr>
												{#each DISPLAY_COLS as col}
													<th class="px-3 py-2.5 text-left whitespace-nowrap">
														{COL_LABELS[col]}
													</th>
												{/each}
											</tr>
										</thead>
										<tbody>
											{#if eventQuery.loading}
												{#each Array(MAP_PAGE_SIZE) as _, i}
													<tr class:alternate-row={i % 2 !== 0}>
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
														class:alternate-row={i % 2 !== 0}
														class="event-result-row cursor-pointer align-middle outline-none"
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
																		class="alternate-toggle w-fit"
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
																	<span class="source-chip w-fit">
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
																			class="more-toggle w-fit"
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
																		<span class="more-count ml-1 whitespace-nowrap"
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
																class="alternate-event-row cursor-pointer align-middle outline-none"
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
																			<span class="source-chip w-fit">
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
																		<span class="more-count"> +{subTypes.length - 1}</span>
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
																				class="more-count ml-1">+{subLocs.length - 1}</span
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
						<nav class="results-pagination" aria-label="Results pages">
							<span class="page-status">Page {page} of {totalPages}</span>
							<div class="pagination-pages flex items-center gap-1">
								<button
									type="button"
									onclick={() => {
										page = Math.max(1, page - 1);
									}}
									disabled={page === 1 || eventQuery.loading}
									class="pagination-action touch-target">← Prev</button
								>

								{#each paginationPages as p}
									{#if p === '…'}
										<span class="px-1 text-slate-400 text-xs">…</span>
									{:else}
										<button
											type="button"
											onclick={() => {
												page = p;
											}}
											disabled={eventQuery.loading}
											aria-current={page === p ? 'page' : undefined}
											class:current-page={page === p}
											class="page-number touch-target">{p}</button
										>
									{/if}
								{/each}

								<button
									type="button"
									onclick={() => {
										page = Math.min(totalPages, page + 1);
									}}
									disabled={page === totalPages || eventQuery.loading}
									class="pagination-action touch-target">Next →</button
								>
							</div>
						</nav>
					{/if}
				</div>

				<div class="results-spacer flex-1 min-h-0"></div>
			</div>
		{/if}
	</aside>
</main>

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

	/* Civic data workbench redesign */
	.map-skip-link {
		position: fixed;
		top: 0.4rem;
		left: 0.75rem;
		z-index: 30;
		transform: translateY(-180%);
		border-radius: var(--radius-control);
		background: var(--color-text);
		padding: 0.65rem 0.9rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-canvas);
		transition: transform 180ms ease;
	}

	.map-skip-link:focus {
		transform: translateY(0);
	}

	.map-workspace {
		isolation: isolate;
		min-height: calc(100dvh - var(--app-nav-height));
		background:
			radial-gradient(circle at 71% 42%, rgba(237, 242, 255, 0.82), transparent 25rem),
			radial-gradient(circle at 14% 83%, rgba(255, 248, 207, 0.32), transparent 18rem),
			var(--color-canvas);
	}

	.map-workspace::before {
		position: absolute;
		inset: 0;
		z-index: -1;
		background-image:
			linear-gradient(rgba(0, 56, 168, 0.026) 1px, transparent 1px),
			linear-gradient(90deg, rgba(0, 56, 168, 0.026) 1px, transparent 1px);
		background-size: 3.5rem 3.5rem;
		mask-image: radial-gradient(circle at 58% 48%, black, transparent 74%);
		content: '';
		pointer-events: none;
	}

	.map-hover-tooltip {
		display: grid;
		gap: 0.12rem;
		max-width: 16rem;
		background: rgba(30, 41, 59, 0.95);
		box-shadow: 0 12px 28px -16px rgba(30, 41, 59, 0.72);
		color: var(--color-canvas);
		backdrop-filter: blur(8px);
	}

	.tooltip-type {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.5625rem;
		font-weight: 500;
		letter-spacing: 0.09em;
		text-transform: uppercase;
		color: var(--color-brand-medium);
	}

	.map-view-toggle {
		top: 1.25rem;
		left: max(2rem, calc((100vw - 78rem) / 2 + 2rem));
		display: grid;
		grid-template-columns: auto auto auto;
		align-items: center;
		gap: 0.2rem;
		transform: none;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: rgba(255, 255, 255, 0.9);
		box-shadow:
			var(--shadow-control),
			inset 0 1px 0 rgba(255, 255, 255, 0.96);
		padding: 0.25rem;
		backdrop-filter: blur(10px);
	}

	.view-toggle-label {
		padding-inline: 0.65rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 500;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.map-view-button,
	.result-mode-button {
		border: 0;
		border-radius: 0.55rem;
		background: transparent;
		padding: 0.65rem 0.9rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		transition:
			transform 180ms ease,
			background-color 180ms ease,
			color 180ms ease,
			box-shadow 180ms ease;
	}

	.map-view-button:hover,
	.result-mode-button:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.map-view-button.active-map-view,
	.result-mode-button.active-result-mode {
		background: var(--color-accent);
		box-shadow: inset 0 0 0 1px rgba(30, 41, 59, 0.16);
		color: var(--color-accent-ink);
	}

	.map-view-button:active,
	.result-mode-button:active,
	.map-retry:active,
	.empty-action:active,
	.pagination-action:active,
	.page-number:active,
	.results-close:active,
	.map-back-control:active {
		transform: scale(0.98);
	}

	.map-overview {
		gap: clamp(2rem, 6vw, 5.5rem);
		padding: 5.25rem 1.25rem 2.25rem;
	}

	.map-canvas-shell {
		position: relative;
		height: min(49dvh, 25rem);
		width: min(100%, 23rem);
	}

	.map-canvas-shell::before {
		position: absolute;
		inset: 7% 2%;
		z-index: -1;
		border: 1px solid rgba(0, 56, 168, 0.12);
		border-radius: 50%;
		content: '';
		pointer-events: none;
	}

	.ncr-magnifier {
		position: absolute;
		left: max(1.5rem, calc((100vw - 78rem) / 2 + 2.5rem));
		bottom: 2.25rem;
		z-index: 12;
		display: grid;
		grid-template-rows: auto minmax(0, 1fr) auto;
		width: min(24rem, calc(100% - 3rem));
		height: min(24rem, calc(100dvh - 16rem));
		min-height: 18rem;
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
		animation: ncr-magnifier-in 220ms cubic-bezier(0.16, 1, 0.3, 1) both;
	}

	.ncr-magnifier::before {
		position: absolute;
		inset: 0 auto auto 0;
		width: 6rem;
		height: 3px;
		background: var(--color-accent);
		content: '';
	}

	.ncr-magnifier-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		padding: 1.05rem 0.75rem 0.75rem 1rem;
	}

	.ncr-magnifier-header p,
	.ncr-magnifier-header h2,
	.ncr-magnifier-instruction {
		margin: 0;
	}

	.ncr-magnifier-header p {
		margin-top: 0.25rem;
		font-size: 0.75rem;
		font-weight: 500;
		line-height: 1.4;
		color: var(--color-text-secondary);
	}

	.ncr-magnifier-header h2 {
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.45rem;
		font-weight: 700;
		line-height: 1.05;
		letter-spacing: -0.025em;
		color: var(--color-text);
	}

	.ncr-close {
		display: inline-grid;
		place-items: center;
		flex: none;
		border: 0;
		border-radius: var(--radius-control);
		background: transparent;
		color: var(--color-text-muted);
		transition:
			background-color 160ms ease,
			color 160ms ease;
	}

	.ncr-close:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.ncr-close svg {
		width: 1rem;
		height: 1rem;
	}

	.ncr-close path {
		stroke: currentColor;
		stroke-width: 1.8;
		stroke-linecap: round;
	}

	.ncr-map-canvas {
		min-height: 0;
		margin: 0 0.75rem;
		border-block: 1px solid var(--color-border);
		background: var(--color-brand-soft);
		padding: 0.5rem;
	}

	.ncr-magnifier-instruction {
		padding: 0.75rem 1rem 0.85rem;
		font-size: 0.6875rem;
		line-height: 1.5;
		color: var(--color-text-secondary);
	}

	@keyframes ncr-magnifier-in {
		from {
			opacity: 0;
			clip-path: inset(0 0 16% 0 round var(--radius-surface));
			transform: translateY(0.75rem);
		}
	}

	.map-guidance {
		order: -1;
		width: min(100%, 31rem);
		text-align: left;
	}

	.map-section-marker {
		display: flex;
		align-items: center;
		gap: 0.7rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 500;
		line-height: 1.4;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.map-marker-rule {
		width: 2rem;
		height: 1px;
		background: var(--color-brand);
	}

	.map-page-title {
		margin-top: 1.25rem;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2.65rem, 7vw, 4.7rem);
		font-weight: 900;
		line-height: 0.98;
		letter-spacing: -0.045em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.current-geography {
		min-height: 6rem;
		margin-top: 2rem;
		border-top: 1px solid var(--color-border);
		padding-top: 1rem;
	}

	.current-geography-label,
	.selection-kicker,
	.map-error-label,
	.result-error-label {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 500;
		line-height: 1.4;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.current-geography-name {
		max-width: 18ch;
		margin-top: 0.3rem;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.55rem, 3.2vw, 2.15rem);
		font-weight: 700;
		line-height: 1.08;
		letter-spacing: -0.025em;
		color: var(--color-text);
		overflow-wrap: anywhere;
		text-wrap: balance;
	}

	.map-guidance-copy {
		max-width: 34ch;
		margin-top: 0.9rem;
		font-size: 0.875rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.map-legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.65rem 1rem;
		margin-top: 1.35rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		color: var(--color-text-muted);
	}

	.map-legend span {
		display: inline-flex;
		align-items: center;
		gap: 0.45rem;
	}

	.map-legend i {
		display: inline-block;
		width: 0.8rem;
		height: 0.8rem;
	}

	.legend-outline {
		border: 1px solid var(--color-brand);
		background: var(--color-brand-soft);
	}

	.legend-current {
		border: 1px solid rgba(30, 41, 59, 0.35);
		background: var(--color-accent);
	}

	.map-loading {
		display: grid;
		justify-items: center;
		gap: 1.25rem;
		font-size: 0.75rem;
		font-weight: 500;
		color: var(--color-text-secondary);
	}

	.map-loading-shape {
		position: relative;
		width: 8rem;
		height: 10rem;
	}

	.map-loading-shape span {
		position: absolute;
		border-radius: 42% 58% 54% 46%;
		background: var(--color-brand-soft);
		animation: map-skeleton 1.25s ease-in-out infinite alternate;
	}

	.map-loading-shape span:nth-child(1) {
		top: 0;
		left: 2.35rem;
		width: 3.1rem;
		height: 3.8rem;
	}

	.map-loading-shape span:nth-child(2) {
		top: 3.15rem;
		left: 2.9rem;
		width: 3.4rem;
		height: 3.7rem;
		animation-delay: 100ms;
	}

	.map-loading-shape span:nth-child(3) {
		top: 5.9rem;
		left: 1.1rem;
		width: 4.1rem;
		height: 3rem;
		animation-delay: 180ms;
	}

	.map-loading-shape span:nth-child(4) {
		top: 7.4rem;
		left: 4.6rem;
		width: 2.15rem;
		height: 2.3rem;
		animation-delay: 260ms;
	}

	@keyframes map-skeleton {
		from {
			opacity: 0.5;
			transform: scale(0.98);
		}
		to {
			opacity: 1;
			transform: scale(1);
		}
	}

	.map-error {
		max-width: 24rem;
		border-left: 3px solid var(--color-danger);
		background: var(--color-danger-surface);
		padding: 1.25rem 1.4rem;
		color: #881526;
	}

	.map-error-label,
	.result-error-label {
		color: var(--color-danger);
	}

	.map-retry,
	.result-retry,
	.empty-action {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		border: 0;
		border-radius: 0.5rem;
		background: transparent;
		padding-inline: 0.5rem;
		font-size: 0.75rem;
		font-weight: 600;
		text-decoration: underline;
		text-underline-offset: 0.25rem;
		color: var(--color-danger);
		transition:
			transform 160ms ease,
			background-color 160ms ease;
	}

	.map-retry:hover,
	.result-retry:hover {
		background: rgba(206, 17, 38, 0.08);
	}

	.map-detail-canvas {
		background:
			radial-gradient(circle at 54% 48%, rgba(237, 242, 255, 0.72), transparent 22rem),
			var(--color-canvas);
	}

	.map-back-position {
		top: 1rem;
		left: 1rem;
	}

	.map-back-control {
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: rgba(255, 255, 255, 0.92);
		box-shadow: var(--shadow-control);
		padding: 0.65rem;
		color: var(--color-brand);
		backdrop-filter: blur(10px);
		transition:
			transform 180ms ease,
			border-color 180ms ease,
			box-shadow 180ms ease;
	}

	.map-back-control:hover {
		border-color: var(--color-brand-medium);
		box-shadow: 0 10px 24px -18px rgba(0, 56, 168, 0.62);
	}

	.map-back-label {
		padding: 0.05rem 0.15rem 0.5rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 500;
		letter-spacing: 0.06em;
		text-transform: uppercase;
	}

	.map-thumbnail {
		width: 6rem;
		height: 5.25rem;
		pointer-events: none;
	}

	.map-results-panel {
		border-color: var(--color-border);
		background: rgba(255, 255, 255, 0.96);
		backdrop-filter: blur(14px);
	}

	.results-content {
		margin-inline: clamp(1rem, 3vw, 2.5rem);
	}

	.results-header {
		padding-bottom: 1.15rem;
	}

	.selection-title {
		max-width: 26ch;
		margin-top: 0.35rem;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.65rem, 2.7vw, 2.35rem);
		font-weight: 700;
		line-height: 1.08;
		letter-spacing: -0.025em;
		color: var(--color-text);
		overflow-wrap: anywhere;
		text-wrap: balance;
	}

	.result-count {
		font-size: clamp(1.7rem, 3vw, 2.35rem);
		font-weight: 700;
		font-variant-numeric: tabular-nums;
		line-height: 1;
		letter-spacing: -0.03em;
		color: var(--color-text);
	}

	.result-count-label {
		font-size: clamp(0.8rem, 1.25vw, 0.95rem);
		font-weight: 600;
		letter-spacing: 0;
		color: var(--color-text-secondary);
	}

	.secondary-count,
	.page-status {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-variant-numeric: tabular-nums;
		color: var(--color-text-muted);
	}

	.results-loading {
		display: grid;
		gap: 0.45rem;
		width: min(100%, 15rem);
	}

	.results-loading span:not(.sr-only) {
		height: 0.8rem;
		border-radius: 0.2rem;
		background: var(--color-brand-soft);
		animation: result-skeleton 1.2s ease-in-out infinite alternate;
	}

	.results-loading span:nth-child(2) {
		width: 58%;
		animation-delay: 140ms;
	}

	@keyframes result-skeleton {
		from {
			opacity: 0.48;
		}
		to {
			opacity: 1;
		}
	}

	.result-mode-toggle {
		display: inline-grid;
		grid-template-columns: 1fr 1fr;
		gap: 0.2rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-surface-subtle);
		padding: 0.25rem;
	}

	.results-close {
		border: 0;
		border-radius: 0.6rem;
		background: transparent;
		color: var(--color-text-muted);
		transition:
			transform 160ms ease,
			background-color 160ms ease,
			color 160ms ease;
	}

	.results-close:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.results-rule {
		height: 1px;
		background: var(--color-border);
	}

	.results-body {
		scrollbar-color: var(--color-brand-medium) transparent;
	}

	.result-error {
		border-left: 3px solid var(--color-danger);
		background: var(--color-danger-surface);
		padding: 1rem 1.15rem;
		font-size: 0.875rem;
		color: #881526;
	}

	.result-error-label {
		margin-bottom: 0.35rem;
	}

	.result-empty {
		display: grid;
		justify-items: start;
		max-width: 28rem;
		padding: 2rem 0;
		text-align: left;
	}

	.empty-symbol {
		display: grid;
		width: 2.4rem;
		height: 2.4rem;
		place-items: center;
		border: 1px solid var(--color-brand-medium);
		background: var(--color-brand-soft);
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 1rem;
		color: var(--color-brand);
	}

	.empty-title {
		margin-top: 1.15rem;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.45rem;
		font-weight: 700;
		color: var(--color-text);
	}

	.empty-copy {
		max-width: 48ch;
		margin-top: 0.4rem;
		font-size: 0.875rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.empty-action {
		margin-top: 0.75rem;
		color: var(--color-brand);
	}

	.empty-action:hover {
		background: var(--color-brand-soft);
	}

	.map-results-table-wrap {
		border-top: 1px solid var(--color-border);
		border-bottom: 1px solid var(--color-border);
		border-radius: 0;
		box-shadow: none;
	}

	.map-results-table {
		border-collapse: collapse;
		font-variant-numeric: tabular-nums;
	}

	.map-results-table thead tr {
		border-bottom: 1px solid var(--color-border);
		background: var(--color-surface-subtle);
	}

	.map-results-table th {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 500;
		letter-spacing: 0.07em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.map-results-table tbody tr {
		border-bottom: 1px solid var(--color-border);
	}

	.map-results-table tbody tr:last-child {
		border-bottom: 0;
	}

	.event-result-row,
	.alternate-event-row {
		transition:
			background-color 180ms ease,
			box-shadow 180ms ease;
	}

	.event-result-row:hover,
	.event-result-row:focus,
	.alternate-event-row:hover,
	.alternate-event-row:focus {
		background: var(--color-brand-soft);
		box-shadow: inset 3px 0 0 var(--color-brand);
	}

	.event-result-row:focus-visible,
	.alternate-event-row:focus-visible {
		outline: 3px solid var(--color-focus);
		outline-offset: -3px;
	}

	.alternate-row {
		background: rgba(248, 250, 252, 0.62);
	}

	.alternate-event-row {
		border-left: 2px solid var(--color-brand-medium);
		background: rgba(237, 242, 255, 0.54);
	}

	.alternate-toggle,
	.source-chip,
	.more-toggle,
	.more-count {
		font-size: 0.625rem;
		font-weight: 500;
		color: var(--color-brand);
	}

	.alternate-toggle {
		min-height: 1.75rem;
		border: 0;
		border-radius: 0.3rem;
		background: var(--color-brand-soft);
		padding-inline: 0.45rem;
		transition: background-color 160ms ease;
	}

	.alternate-toggle:hover {
		background: var(--color-brand-medium);
	}

	.source-chip {
		border-radius: 0.25rem;
		background: var(--color-surface-subtle);
		padding: 0.18rem 0.42rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.5625rem;
		letter-spacing: 0.05em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.more-toggle {
		min-height: 1.75rem;
		border: 0;
		background: transparent;
		padding: 0;
		text-decoration: underline;
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.2rem;
	}

	.results-pagination {
		display: flex;
		flex: 0 0 auto;
		flex-direction: column;
		align-items: center;
		gap: 0.55rem;
		border-top: 1px solid var(--color-border);
		padding: 0.85rem 0 0.5rem;
	}

	.pagination-action,
	.page-number {
		border: 1px solid var(--color-border);
		border-radius: 0.55rem;
		background: var(--color-canvas);
		font-size: 0.6875rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		transition:
			transform 160ms ease,
			background-color 160ms ease,
			border-color 160ms ease;
	}

	.pagination-action {
		padding-inline: 0.75rem;
	}

	.page-number {
		display: flex;
		width: 2.75rem;
		height: 2.75rem;
		align-items: center;
		justify-content: center;
		font-variant-numeric: tabular-nums;
	}

	.pagination-action:hover:not(:disabled),
	.page-number:hover:not(:disabled) {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
	}

	.page-number.current-page {
		border-color: rgba(30, 41, 59, 0.2);
		background: var(--color-accent);
		color: var(--color-accent-ink);
	}

	.pagination-action:disabled,
	.page-number:disabled {
		cursor: not-allowed;
		opacity: 0.42;
	}

	@media (max-width: 767px) {
		.map-view-toggle {
			top: 0.75rem;
			left: 1rem;
			grid-template-columns: 1fr 1fr;
			width: calc(100% - 2rem);
			transform: none;
		}

		.view-toggle-label {
			display: none;
		}

		.map-overview {
			gap: 1.5rem;
			padding: 5.25rem 1rem 2rem;
		}

		.map-guidance {
			order: -1;
		}

		.map-page-title {
			font-size: clamp(2.5rem, 13vw, 3.5rem);
		}

		.current-geography {
			min-height: 0;
			margin-top: 1.35rem;
		}

		.current-geography-name {
			font-size: 1.45rem;
		}

		.map-canvas-shell {
			height: min(47dvh, 24rem);
			width: min(100%, 22rem);
		}

		.ncr-magnifier {
			position: fixed;
			inset: auto 0.75rem max(0.75rem, env(safe-area-inset-bottom));
			z-index: 25;
			width: auto;
			height: min(31rem, calc(100dvh - var(--app-nav-height) - 1.5rem));
			min-height: 24rem;
		}

		.map-workspace.has-selection .map-panel {
			height: clamp(17rem, 38dvh, 21rem);
		}

		.map-workspace.has-selection .map-detail-canvas {
			padding: 3rem 1rem 0.35rem;
		}

		.map-results-panel {
			min-height: calc(62dvh - var(--app-nav-height));
		}

		.results-content {
			margin-inline: 1rem;
			padding-top: 1.5rem;
		}

		.map-results-table tr {
			border: 0;
			border-bottom: 1px solid var(--color-border);
			border-radius: 0;
			background: transparent;
			padding: 0.75rem 0.15rem;
		}

		.map-results-table tr:first-child {
			border-top: 1px solid var(--color-border);
		}

		.map-results-table td::before {
			font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
			font-size: 0.625rem;
			font-weight: 500;
			color: var(--color-text-muted);
		}

		.alternate-event-row {
			border-left: 2px solid var(--color-brand);
			padding-left: 0.75rem !important;
		}
	}

	@media (min-width: 768px) and (max-width: 1023px) {
		.map-view-toggle {
			left: 2rem;
		}

		.map-overview {
			gap: 2rem;
			padding-top: 5.5rem;
		}

		.map-guidance {
			order: -1;
			max-width: 32rem;
		}
	}

	@media (min-width: 1024px) {
		.map-workspace {
			height: calc(100dvh - var(--app-nav-height));
		}

		.map-overview {
			flex-direction: row;
			max-width: 78rem;
			margin-inline: auto;
			padding: 5.1rem 2.5rem 1.5rem;
		}

		.map-guidance {
			order: -1;
			width: 21rem;
		}

		.map-canvas-shell {
			height: min(calc(100dvh - 7.5rem), 45rem);
			width: auto;
			max-width: 58%;
		}

		.map-workspace.has-selection .map-panel {
			width: 44%;
		}

		.map-workspace.has-selection .map-results-panel {
			width: 56%;
		}

		.results-content {
			max-height: 88dvh;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.map-loading-shape span,
		.results-loading span:not(.sr-only) {
			animation: none;
		}

		.ncr-magnifier {
			animation: none;
		}

		.map-skip-link,
		.map-view-button,
		.result-mode-button,
		.map-retry,
		.result-retry,
		.empty-action,
		.pagination-action,
		.page-number,
		.results-close,
		.map-back-control,
		.event-result-row,
		.alternate-event-row {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.map-workspace::before,
		.map-canvas-shell::before {
			display: none;
		}

		.map-view-button.active-map-view,
		.result-mode-button.active-result-mode,
		.page-number.current-page {
			background: Highlight;
			color: HighlightText;
		}

		.legend-current,
		.empty-symbol {
			border: 1px solid CanvasText;
		}

		.ncr-magnifier,
		.ncr-map-canvas {
			border-color: CanvasText;
			background: Canvas;
		}
	}
</style>
