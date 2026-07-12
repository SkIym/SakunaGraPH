<script>
	import { apiJson, withQuery } from '$lib/api.js';
	import { toAnalysisParams } from '$lib/analysis/filters.svelte.js';
	import MetricCards from '$lib/components/analysis/MetricCards.svelte';
	import DisasterTypeDonut from '$lib/components/analysis/DisasterTypeDonut.svelte';
	import VictimTrendLine from '$lib/components/analysis/VictimTrendLine.svelte';
	import RegionRankingBar from '$lib/components/analysis/RegionRankingBar.svelte';
	import DisasterRankingBar from '$lib/components/analysis/DisasterRankingBar.svelte';
	import DamageHistogram from '$lib/components/analysis/DamageHistogram.svelte';
	import DamageAffectedScatter from '$lib/components/analysis/DamageAffectedScatter.svelte';

	let dashboard = $state(null);
	let loading = $state(true);
	let error = $state('');
	let retryToken = $state(0);
	let groupBy = $state('taxonomy');
	const filterQuery = $derived(toAnalysisParams().toString());
	const requestVersion = $derived({ filters: filterQuery, groupBy, retry: retryToken });

	function endpoint(path, filters, extras = {}) {
		const params = new URLSearchParams(filters);
		for (const [key, value] of Object.entries(extras)) params.set(key, value);
		return withQuery(path, params);
	}

	$effect(() => {
		const request = requestVersion;
		const controller = new AbortController();
		loading = true;
		error = '';
		dashboard = null;

		void Promise.all([
			apiJson(endpoint('/api/analysis/summary', request.filters), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/disaster-counts', request.filters, { group_by: request.groupBy }), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/victim-trends', request.filters), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/region-rankings', request.filters), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/disaster-rankings', request.filters), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/damage-histogram', request.filters, { bins: '10' }), { signal: controller.signal }),
			apiJson(endpoint('/api/analysis/damage-vs-affected', request.filters), { signal: controller.signal })
		])
			.then(([summary, disasterCounts, victimTrends, regionRankings, disasterRankings, damageHistogram, damageAffected]) => {
				dashboard = { summary, disasterCounts, victimTrends, regionRankings, disasterRankings, damageHistogram, damageAffected };
			})
			.catch((requestError) => {
				if (requestError.name !== 'AbortError') error = requestError.message || 'Could not load metrics.';
			})
			.finally(() => {
				if (!controller.signal.aborted) loading = false;
			});

		return () => controller.abort();
	});
</script>

<svelte:head>
	<title>Metrics dashboard · SakunaGraPH</title>
</svelte:head>

<section class="mx-auto w-full max-w-[1500px] px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
	<div class="mb-6 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
		<div>
			<p class="text-[10px] font-semibold uppercase text-indigo-600" style="letter-spacing:0.12em;">Analysis</p>
			<h1 class="mt-1 text-xl font-semibold text-slate-800">Metrics dashboard</h1>
			<p class="mt-1 text-xs leading-5 text-slate-500">Totals, distributions, and trends for the current filter scope.</p>
		</div>
		<div class="inline-flex rounded-md border border-slate-200 bg-white p-0.5 shadow-sm">
			<button type="button" onclick={() => (groupBy = 'taxonomy')} class="rounded px-2.5 py-1.5 text-[11px] font-semibold transition {groupBy === 'taxonomy' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:bg-slate-50'}">Taxonomy groups</button>
			<button type="button" onclick={() => (groupBy = 'type')} class="rounded px-2.5 py-1.5 text-[11px] font-semibold transition {groupBy === 'type' ? 'bg-slate-800 text-white' : 'text-slate-500 hover:bg-slate-50'}">Detailed types</button>
		</div>
	</div>

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4" role="alert"><p class="text-sm font-semibold text-red-800">Metrics are unavailable</p><p class="mt-1 text-xs text-red-600">{error}</p><button type="button" onclick={() => (retryToken += 1)} class="mt-3 text-xs font-semibold text-red-700 underline">Try again</button></div>
	{:else if loading}
		<div class="grid gap-4 lg:grid-cols-2">
			{#each [1, 2, 3, 4, 5] as card}<div class="h-72 animate-pulse rounded-xl border border-slate-200 bg-slate-50"></div>{/each}
		</div>
	{:else if dashboard}
		<MetricCards summary={dashboard.summary} />
		<div class="mt-5 grid gap-5 xl:grid-cols-2">
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Event distribution</h2><p class="mt-1 text-xs text-slate-500">Counts by {dashboard.disasterCounts.group_by === 'taxonomy' ? 'taxonomy group' : 'detailed disaster type'}.</p><div class="mt-4"><DisasterTypeDonut items={dashboard.disasterCounts.items} /></div></article>
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Victim trend</h2><p class="mt-1 text-xs text-slate-500">Annual reported deaths, injuries, and missing persons.</p><div class="mt-4"><VictimTrendLine items={dashboard.victimTrends.items} /></div></article>
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Regions with most events</h2><p class="mt-1 text-xs text-slate-500">Deduplicated events ranked by affected PSGC region.</p><div class="mt-5"><RegionRankingBar items={dashboard.regionRankings.items} /></div></article>
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Disaster types by reported deaths</h2><p class="mt-1 text-xs text-slate-500">Casualty totals are not normalized across sources.</p><div class="mt-5"><DisasterRankingBar items={dashboard.disasterRankings.items} /></div></article>
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Damage distribution</h2><p class="mt-1 text-xs text-slate-500">Reported damage values stay separated by unit.</p><div class="mt-5"><DamageHistogram bins={dashboard.damageHistogram.bins} /></div></article>
			<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5"><h2 class="text-sm font-semibold text-slate-800">Damage vs. affected population</h2><p class="mt-1 text-xs text-slate-500">Each point is a reported event damage amount.</p><div class="mt-5"><DamageAffectedScatter items={dashboard.damageAffected.items} /></div></article>
		</div>
	{/if}
</section>
