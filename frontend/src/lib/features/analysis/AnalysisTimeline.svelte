<script>
	import {
		getAnalysisCalendarDays,
		getAnalysisCalendarMonths,
		getAnalysisCalendarYears,
		getAnalysisCategoryStacks,
		getAnalysisDateEvents,
	} from '$lib/api/analysis.js';
	import { mergeAnalysisParams } from './params.js';
	import { toAnalysisParams } from './state/filters.svelte.js';
	import { timelineSelection } from './state/timelineSelection.svelte.js';
	import TimelineCalendarPanel from '$lib/components/analysis/TimelineCalendarPanel.svelte';
	import StackedCategoryTimeline from '$lib/components/analysis/StackedCategoryTimeline.svelte';
	import DateEventDrawer from '$lib/components/analysis/DateEventDrawer.svelte';

	let years = $state([]);
	let months = $state([]);
	let days = $state([]);
	let stacks = $state([]);
	let dateEvents = $state([]);
	let loadingYears = $state(true);
	let loadingMonths = $state(false);
	let loadingDays = $state(false);
	let loadingEvents = $state(false);
	let error = $state('');
	let eventError = $state('');
	let retryToken = $state(0);
	let bucket = $state('month_year');
	let selectedEvent = $state('');
	let EventDetailsComponent = $state(null);
	const filterQuery = $derived(toAnalysisParams().toString());
	const timelineRequest = $derived({ filters: filterQuery, retry: retryToken });
	const selectedYear = $derived(timelineSelection.year);
	const selectedMonth = $derived(timelineSelection.month);
	const selectedPrefix = $derived(timelineSelection.datePrefix);

	$effect(() => {
		const request = timelineRequest;
		const filters = request.filters;
		const controller = new AbortController();
		timelineSelection.reset();
		loadingYears = true;
		error = '';
		years = [];
		months = [];
		days = [];
		void getAnalysisCalendarYears(filters, { signal: controller.signal })
			.then((yearData) => {
				years = yearData.items;
				const latest = yearData.items.at(-1);
				if (latest) timelineSelection.setYear(Number(latest.period));
			})
			.catch((requestError) => {
				if (requestError.name !== 'AbortError')
					error = requestError.message || 'Could not load timeline data.';
			})
			.finally(() => {
				if (!controller.signal.aborted) loadingYears = false;
			});

		return () => controller.abort();
	});

	$effect(() => {
		const filters = filterQuery;
		const currentBucket = bucket;
		const controller = new AbortController();
		stacks = [];
		void getAnalysisCategoryStacks(mergeAnalysisParams(filters, { bucket: currentBucket }), {
			signal: controller.signal,
		})
			.then((data) => (stacks = data.items))
			.catch(() => {
				if (!controller.signal.aborted) stacks = [];
			});
		return () => controller.abort();
	});

	$effect(() => {
		const filters = filterQuery;
		const year = selectedYear;
		if (!year) return;
		const controller = new AbortController();
		loadingMonths = true;
		months = [];
		void getAnalysisCalendarMonths(mergeAnalysisParams(filters, { year }), {
			signal: controller.signal,
		})
			.then((data) => (months = data.items))
			.catch(() => {
				if (!controller.signal.aborted) months = [];
			})
			.finally(() => {
				if (!controller.signal.aborted) loadingMonths = false;
			});
		return () => controller.abort();
	});

	$effect(() => {
		const filters = filterQuery;
		const year = selectedYear;
		const month = selectedMonth;
		if (!year || !month) {
			days = [];
			return;
		}
		const controller = new AbortController();
		loadingDays = true;
		days = [];
		void getAnalysisCalendarDays(mergeAnalysisParams(filters, { year, month }), {
			signal: controller.signal,
		})
			.then((data) => (days = data.items))
			.catch(() => {
				if (!controller.signal.aborted) days = [];
			})
			.finally(() => {
				if (!controller.signal.aborted) loadingDays = false;
			});
		return () => controller.abort();
	});

	$effect(() => {
		const filters = filterQuery;
		const prefix = selectedPrefix;
		if (!prefix) {
			dateEvents = [];
			return;
		}
		const controller = new AbortController();
		loadingEvents = true;
		eventError = '';
		void getAnalysisDateEvents(mergeAnalysisParams(filters, { date_prefix: prefix }), {
			signal: controller.signal,
		})
			.then((data) => (dateEvents = data.items))
			.catch((requestError) => {
				if (!controller.signal.aborted)
					eventError = requestError.message || 'Could not load date events.';
			})
			.finally(() => {
				if (!controller.signal.aborted) loadingEvents = false;
			});
		return () => controller.abort();
	});

	function selectTimelinePeriod(period) {
		if (/^\d{2}$/.test(period) && timelineSelection.year) {
			timelineSelection.selectMonth(Number(period));
			return;
		}
		if (!/^\d{4}-\d{2}$/.test(period)) return;
		const [year, month] = period.split('-').map(Number);
		if (timelineSelection.year !== year) timelineSelection.setYear(year);
		timelineSelection.selectMonth(month);
	}

	async function openEventDetails(event) {
		if (!event) return;
		if (!EventDetailsComponent) {
			EventDetailsComponent = (await import('$lib/components/EventDetails.svelte')).default;
		}
		selectedEvent = event;
	}
</script>

<svelte:head><title>Timeline analysis · SakunaGraPH</title></svelte:head>

<section class="analysis-page analysis-page-wide">
	<header class="analysis-page-header">
		<div>
			<p class="workspace-kicker">Analysis / Timeline</p>
			<h1>Timeline and date analysis</h1>
			<p class="analysis-page-copy">
				Explore the active filter scope by event start date, then open any date’s event records.
			</p>
		</div>
		<a href="/analysis" class="analysis-back-link">← Analysis overview</a>
	</header>

	{#if error}
		<div class="analysis-error-panel" role="alert">
			<h2>Timeline is unavailable</h2>
			<p>{error}</p>
			<button type="button" onclick={() => (retryToken += 1)}>Try again</button>
		</div>
	{:else}
		<div class="timeline-layout">
			<div class="analysis-surface timeline-board">
				<article class="analysis-chart-panel">
					<h2>Calendar drill-down</h2>
					<p>
						Darker cells contain more deduplicated event records. Selecting a cell opens its event
						set.
					</p>
					{#if loadingYears}<div
							class="mt-5 h-72 animate-pulse rounded-lg bg-slate-50"
						></div>{:else}<div class="mt-5">
							<TimelineCalendarPanel
								{years}
								{months}
								{days}
								selectedYear={timelineSelection.year}
								selectedMonth={timelineSelection.month}
								selectedDay={timelineSelection.day}
								onSelectYear={(year) => timelineSelection.selectYear(year)}
								onSelectMonth={(month) => timelineSelection.selectMonth(month)}
								onSelectDay={(day) => timelineSelection.selectDay(day)}
							/>
						</div>{/if}
					{#if loadingMonths || loadingDays}<p class="mt-3 text-[10px] text-slate-400">
							Updating calendar…
						</p>{/if}
				</article>

				<article class="analysis-chart-panel">
					<div class="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
						<div>
							<h2>Category timeline</h2>
							<p>Monthly event assignments grouped by disaster taxonomy.</p>
						</div>
						<div class="analysis-segmented" role="group" aria-label="Timeline grouping">
							<button
								type="button"
								onclick={() => (bucket = 'month_year')}
								aria-pressed={bucket === 'month_year'}>Chronological</button
							><button
								type="button"
								onclick={() => (bucket = 'month_of_year')}
								aria-pressed={bucket === 'month_of_year'}>Seasonal</button
							>
						</div>
					</div>
					<div class="mt-5">
						<StackedCategoryTimeline items={stacks} onselect={selectTimelinePeriod} />
					</div>
				</article>
			</div>

			<div class="xl:sticky xl:top-4 xl:self-start">
				{#if timelineSelection.showEvents && selectedPrefix}
					<DateEventDrawer
						datePrefix={selectedPrefix}
						items={dateEvents}
						loading={loadingEvents}
						error={eventError}
						onclose={() => timelineSelection.closeEvents()}
						onselect={openEventDetails}
					/>
				{:else}
					<div class="analysis-empty-panel">
						<strong>Select a calendar cell</strong>
						<p>The matching event records will appear here.</p>
					</div>
				{/if}
			</div>
		</div>
	{/if}
</section>

{#if selectedEvent && EventDetailsComponent}<EventDetailsComponent
		event={selectedEvent}
		onclose={() => (selectedEvent = '')}
	/>{/if}

<style>
	.timeline-layout {
		display: grid;
		grid-template-columns: minmax(0, 1fr) minmax(19rem, 23rem);
		align-items: start;
		gap: 1.5rem;
	}

	.timeline-board {
		display: grid;
		gap: 1px;
		overflow: hidden;
		background: var(--color-border);
	}

	@media (max-width: 74rem) {
		.timeline-layout {
			grid-template-columns: 1fr;
		}
	}
</style>
