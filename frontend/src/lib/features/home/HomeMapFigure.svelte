<script>
	import { goto } from '$app/navigation';
	import { getAnalysisRegionRankings } from '$lib/api/analysis.js';
	import PhilMap from '$lib/components/map/PhilMap.svelte';
	import { formatProvName } from '$lib/mapData.js';
	import { FULL_MAP_VIEW_BOX, loadMapGeometry } from '$lib/features/map/geometry.js';
	import { buildHomeRecordMarkers } from './mapMarkers.js';

	let pathData = $state([]);
	let pathGenerator = $state(null);
	let regionRankings = $state([]);
	let mapLoading = $state(true);
	let mapError = $state('');
	let tooltipItem = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);

	const provinceColors = $derived(
		Object.fromEntries(pathData.map((item) => [item.gid, 'var(--color-brand-medium)'])),
	);
	const recordMarkers = $derived(
		buildHomeRecordMarkers({ rankings: regionRankings, pathData, pathGenerator }),
	);
	const recordMarkerSummary = $derived(
		recordMarkers
			.map(
				(marker) =>
					`${marker.label}: ${marker.count.toLocaleString()} linked ${marker.count === 1 ? 'record' : 'records'}`,
			)
			.join('; '),
	);

	$effect(() => {
		const controller = new AbortController();
		mapLoading = true;
		mapError = '';

		void loadMapGeometry({ signal: controller.signal })
			.then((geometry) => {
				pathData = geometry.pathData;
				pathGenerator = geometry.pathGenerator;
				mapLoading = false;
			})
			.catch((requestError) => {
				if (requestError?.name === 'AbortError') return;
				mapError = 'The province preview could not be loaded.';
				mapLoading = false;
			});

		return () => controller.abort();
	});

	$effect(() => {
		const controller = new AbortController();
		void getAnalysisRegionRankings({}, { signal: controller.signal })
			.then((response) => {
				regionRankings = Array.isArray(response?.items) ? response.items : [];
			})
			.catch(() => {
				if (!controller.signal.aborted) regionRankings = [];
			});
		return () => controller.abort();
	});

	function provinceLabel(item) {
		return formatProvName(item?.name ?? '');
	}

	function provinceActionLabel(item) {
		return `Open ${provinceLabel(item)} in the full map`;
	}

	function openProvinceMap(item) {
		tooltipItem = null;
		void goto(`/map?view=provinces&province=${encodeURIComponent(item.gid)}`);
	}
</script>

{#if tooltipItem}
	<div
		role="tooltip"
		class="home-map-tooltip pointer-events-none fixed z-50 max-w-48 rounded-lg bg-slate-800/90 px-3 py-1.5 text-xs font-medium text-white shadow-lg"
		style="left:clamp(0.75rem,{tooltipX +
			16}px,calc(100vw - 12.75rem)); top:clamp(1.5rem,{tooltipY}px,calc(100vh - 1.5rem)); transform:translateY(-50%); backdrop-filter:blur(4px);"
	>
		{provinceLabel(tooltipItem)}
	</div>
{/if}

<figure class="relative min-w-0" aria-describedby="home-map-instructions">
	<div class="relative h-[32rem] sm:h-[36rem] lg:h-[40rem]">
		{#if mapLoading}
			<div class="absolute inset-0 grid place-items-center" role="status">
				<p class="text-sm font-medium text-slate-600">Loading province map...</p>
			</div>
		{:else if mapError}
			<div class="absolute inset-0 grid place-items-center px-6 text-center" role="alert">
				<div>
					<p class="text-sm font-semibold text-slate-800">{mapError}</p>
					<a
						href="/map?view=provinces"
						class="brand-link touch-target mt-2 inline-flex items-center text-sm font-semibold underline decoration-[var(--color-brand-medium)] underline-offset-4"
						>Open the full map</a
					>
				</div>
			</div>
		{:else}
			<div class="absolute inset-0">
				<PhilMap
					{pathData}
					viewBox={FULL_MAP_VIEW_BOX}
					view="provinces"
					selected={null}
					interactive={true}
					strokeWidth={0.85}
					strokeColor="var(--color-brand)"
					hoverFill="var(--color-accent)"
					colorMap={provinceColors}
					ariaLabel="Province map preview"
					getAreaLabel={provinceActionLabel}
					onselect={openProvinceMap}
					onhover={(item, x, y) => {
						tooltipItem = item;
						tooltipX = x;
						tooltipY = y;
					}}
				/>
				{#if recordMarkers.length > 0}
					<svg
						viewBox={FULL_MAP_VIEW_BOX}
						class="pointer-events-none absolute inset-0 h-full w-full"
						preserveAspectRatio="xMidYMid meet"
						role="img"
						aria-label="Regions with the highest linked disaster-record counts"
					>
						<title>{recordMarkerSummary}</title>
						{#each recordMarkers as marker (marker.id)}
							<circle
								cx={marker.x}
								cy={marker.y}
								r={marker.radius}
								fill="var(--color-action)"
								stroke="white"
								stroke-width="2"
								vector-effect="non-scaling-stroke"
							>
								<title>{marker.label}: {marker.count.toLocaleString()} linked records</title>
							</circle>
						{/each}
					</svg>
				{/if}
			</div>
		{/if}
	</div>
	<figcaption
		id="home-map-instructions"
		class="mt-2 flex flex-wrap items-center justify-center gap-x-4 gap-y-2 text-[0.6875rem] font-medium text-slate-600"
	>
		<span class="inline-flex items-center gap-1.5">
			<span class="h-0 w-4 border-t-2 border-[var(--color-brand)]" aria-hidden="true"></span>
			Province boundaries
		</span>
		<span class="inline-flex items-center gap-1.5">
			<span
				class="h-2.5 w-2.5 rounded-sm bg-[var(--color-accent)] ring-1 ring-slate-800/40"
				aria-hidden="true"
			></span>
			Hover or focus
		</span>
		{#if recordMarkers.length > 0}
			<span class="inline-flex items-center gap-1.5" title={recordMarkerSummary}>
				<span
					class="h-2.5 w-2.5 rounded-full bg-[var(--color-action)] ring-2 ring-white"
					aria-hidden="true"
				></span>
				Highest record-count regions
			</span>
		{/if}
		<span class="sr-only">
			Activate a province to open its disaster records in the full map. {recordMarkerSummary}
		</span>
	</figcaption>
</figure>

<style>
	@media (hover: none), (max-width: 767px) {
		.home-map-tooltip {
			display: none;
		}
	}
</style>
