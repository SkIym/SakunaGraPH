<script>
	import {
		getAnalysisDamageAffected,
		getAnalysisDamageHistogram,
		getAnalysisDisasterCounts,
		getAnalysisDisasterRankings,
		getAnalysisRegionRankings,
		getAnalysisSummary,
		getAnalysisVictimTrends,
	} from '$lib/api/analysis.js';
	import { mergeAnalysisParams } from './params.js';
	import { toAnalysisParams } from './state/filters.svelte.js';
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

	$effect(() => {
		const request = requestVersion;
		const controller = new AbortController();
		loading = true;
		error = '';
		dashboard = null;

		void Promise.all([
			getAnalysisSummary(request.filters, { signal: controller.signal }),
			getAnalysisDisasterCounts(
				mergeAnalysisParams(request.filters, { group_by: request.groupBy }),
				{
					signal: controller.signal,
				},
			),
			getAnalysisVictimTrends(request.filters, {
				signal: controller.signal,
			}),
			getAnalysisRegionRankings(request.filters, {
				signal: controller.signal,
			}),
			getAnalysisDisasterRankings(request.filters, {
				signal: controller.signal,
			}),
			getAnalysisDamageHistogram(mergeAnalysisParams(request.filters, { bins: '10' }), {
				signal: controller.signal,
			}),
			getAnalysisDamageAffected(request.filters, { signal: controller.signal }),
		])
			.then(
				([
					summary,
					disasterCounts,
					victimTrends,
					regionRankings,
					disasterRankings,
					damageHistogram,
					damageAffected,
				]) => {
					dashboard = {
						summary,
						disasterCounts,
						victimTrends,
						regionRankings,
						disasterRankings,
						damageHistogram,
						damageAffected,
					};
				},
			)
			.catch((requestError) => {
				if (requestError.name !== 'AbortError')
					error = requestError.message || 'Could not load metrics.';
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

<section class="analysis-page analysis-page-wide">
	<header class="analysis-page-header">
		<div>
			<p class="workspace-kicker">Analysis / Metrics</p>
			<h1>Metrics dashboard</h1>
			<p class="analysis-page-copy">
				Totals, distributions, and trends for the current filter scope.
			</p>
		</div>
		<div class="analysis-segmented" role="group" aria-label="Disaster count grouping">
			<button
				type="button"
				onclick={() => (groupBy = 'taxonomy')}
				aria-pressed={groupBy === 'taxonomy'}>Taxonomy groups</button
			>
			<button type="button" onclick={() => (groupBy = 'type')} aria-pressed={groupBy === 'type'}
				>Detailed types</button
			>
		</div>
	</header>

	{#if error}
		<div class="analysis-error-panel" role="alert">
			<h2>Metrics are unavailable</h2>
			<p>{error}</p>
			<button type="button" onclick={() => (retryToken += 1)}>Try again</button>
		</div>
	{:else if loading}
		<div class="analysis-loading-grid" aria-label="Loading metrics">
			{#each [1, 2, 3, 4, 5]}<div class="analysis-loading-panel animate-pulse"></div>{/each}
		</div>
	{:else if dashboard}
		<div class="analysis-surface metrics-board">
			<MetricCards summary={dashboard.summary} />
			<div class="analysis-chart-grid">
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Event distribution</h2>
					<p>
						Counts by {dashboard.disasterCounts.group_by === 'taxonomy'
							? 'taxonomy group'
							: 'detailed disaster type'}.
					</p>
					<div class="analysis-chart-body">
						<DisasterTypeDonut items={dashboard.disasterCounts.items} />
					</div>
				</article>
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Victim trend</h2>
					<p>Annual reported deaths, injuries, and missing persons.</p>
					<div class="analysis-chart-body">
						<VictimTrendLine items={dashboard.victimTrends.items} />
					</div>
				</article>
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Regions with most events</h2>
					<p>Deduplicated events ranked by affected PSGC region.</p>
					<div class="analysis-chart-body">
						<RegionRankingBar items={dashboard.regionRankings.items} />
					</div>
				</article>
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Disaster types by reported deaths</h2>
					<p>Casualty totals are not normalized across sources.</p>
					<div class="analysis-chart-body">
						<DisasterRankingBar items={dashboard.disasterRankings.items} />
					</div>
				</article>
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Damage distribution</h2>
					<p>Reported damage values stay separated by unit.</p>
					<div class="analysis-chart-body">
						<DamageHistogram bins={dashboard.damageHistogram.bins} />
					</div>
				</article>
				<article class="analysis-chart-panel deferred-visualization">
					<h2 class="text-sm font-semibold text-slate-800">Damage vs. affected population</h2>
					<p>Each point is a reported event damage amount.</p>
					<div class="analysis-chart-body">
						<DamageAffectedScatter items={dashboard.damageAffected.items} />
					</div>
				</article>
			</div>
		</div>
	{/if}
</section>

<style>
	.metrics-board {
		overflow: hidden;
	}

	.analysis-loading-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1px;
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		background: var(--color-border);
	}

	.analysis-loading-panel {
		height: 18rem;
		background: var(--color-surface-subtle);
	}

	@media (max-width: 47.999rem) {
		.analysis-loading-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
