<script>
	import { goto } from '$app/navigation';
	import PhilMap from '$lib/components/map/PhilMap.svelte';
	import { formatProvName } from '$lib/mapData.js';
	import { FULL_MAP_VIEW_BOX, loadMapGeometry } from '$lib/features/map/geometry.js';

	let pathData = $state([]);
	let mapLoading = $state(true);
	let mapError = $state('');
	let tooltipItem = $state(null);
	let tooltipX = $state(0);
	let tooltipY = $state(0);

	const provinceColors = $derived(
		Object.fromEntries(pathData.map((item) => [item.gid, '#dbeafe'])),
	);

	$effect(() => {
		const controller = new AbortController();
		mapLoading = true;
		mapError = '';

		void loadMapGeometry({ signal: controller.signal })
			.then((geometry) => {
				pathData = geometry.pathData;
				mapLoading = false;
			})
			.catch((requestError) => {
				if (requestError?.name === 'AbortError') return;
				mapError = 'The province preview could not be loaded.';
				mapLoading = false;
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
						class="touch-target mt-2 inline-flex items-center text-sm font-semibold text-blue-800 underline decoration-blue-200 underline-offset-4 hover:text-blue-950"
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
					strokeColor="#305bb2"
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
			</div>
		{/if}
	</div>
	<figcaption id="home-map-instructions" class="sr-only">
		Interactive map of Philippine provinces. Hover to identify a province, or activate one to open
		its disaster records in the full map.
	</figcaption>
</figure>

<style>
	@media (hover: none), (max-width: 767px) {
		.home-map-tooltip {
			display: none;
		}
	}
</style>
