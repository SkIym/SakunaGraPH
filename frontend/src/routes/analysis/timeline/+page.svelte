<script>
	import { apiJson, withQuery } from '$lib/api.js';
	import { toAnalysisParams } from '$lib/analysis/filters.svelte.js';
	import { timelineSelection } from '$lib/analysis/timelineSelection.svelte.js';
	import TimelineCalendarPanel from '$lib/components/analysis/TimelineCalendarPanel.svelte';
	import StackedCategoryTimeline from '$lib/components/analysis/StackedCategoryTimeline.svelte';
	import DateEventDrawer from '$lib/components/analysis/DateEventDrawer.svelte';
	import EventDetails from '$lib/components/EventDetails.svelte';

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
	let bucket = $state('month_year');
	let selectedEvent = $state('');
	const filterQuery = $derived(toAnalysisParams().toString());
	const selectedYear = $derived(timelineSelection.year);
	const selectedMonth = $derived(timelineSelection.month);
	const selectedPrefix = $derived(timelineSelection.datePrefix);

	function endpoint(path, filters, extras = {}) {
		const params = new URLSearchParams(filters);
		for (const [key, value] of Object.entries(extras)) params.set(key, String(value));
		return withQuery(path, params);
	}

	$effect(() => {
		const filters = filterQuery;
		const controller = new AbortController();
		timelineSelection.reset();
		loadingYears = true;
		error = '';
		years = [];
		months = [];
		days = [];
		void apiJson(endpoint('/api/analysis/calendar/years', filters), { signal: controller.signal })
			.then((yearData) => {
				years = yearData.items;
				const latest = yearData.items.at(-1);
				if (latest) timelineSelection.setYear(Number(latest.period));
			})
			.catch((requestError) => {
				if (requestError.name !== 'AbortError') error = requestError.message || 'Could not load timeline data.';
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
		void apiJson(endpoint('/api/analysis/timeline/category-stacks', filters, { bucket: currentBucket }), { signal: controller.signal })
			.then((data) => (stacks = data.items))
			.catch(() => { if (!controller.signal.aborted) stacks = []; });
		return () => controller.abort();
	});

	$effect(() => {
		const filters = filterQuery;
		const year = selectedYear;
		if (!year) return;
		const controller = new AbortController();
		loadingMonths = true;
		months = [];
		void apiJson(endpoint('/api/analysis/calendar/months', filters, { year }), { signal: controller.signal })
			.then((data) => (months = data.items))
			.catch(() => { if (!controller.signal.aborted) months = []; })
			.finally(() => { if (!controller.signal.aborted) loadingMonths = false; });
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
		void apiJson(endpoint('/api/analysis/calendar/days', filters, { year, month }), { signal: controller.signal })
			.then((data) => (days = data.items))
			.catch(() => { if (!controller.signal.aborted) days = []; })
			.finally(() => { if (!controller.signal.aborted) loadingDays = false; });
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
		void apiJson(endpoint('/api/analysis/timeline/date-events', filters, { date_prefix: prefix }), { signal: controller.signal })
			.then((data) => (dateEvents = data.items))
			.catch((requestError) => { if (!controller.signal.aborted) eventError = requestError.message || 'Could not load date events.'; })
			.finally(() => { if (!controller.signal.aborted) loadingEvents = false; });
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
</script>

<svelte:head><title>Timeline analysis · SakunaGraPH</title></svelte:head>

<section class="mx-auto w-full max-w-[1500px] px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
	<div class="mb-6">
		<p class="text-[10px] font-semibold uppercase text-indigo-600" style="letter-spacing:0.12em;">Analysis</p>
		<h1 class="mt-1 text-xl font-semibold text-slate-800">Timeline and date analysis</h1>
		<p class="mt-1 text-xs leading-5 text-slate-500">Explore the active filter scope by event start date, then open any date’s event records.</p>
	</div>

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4" role="alert"><p class="text-sm font-semibold text-red-800">Timeline is unavailable</p><p class="mt-1 text-xs text-red-600">{error}</p></div>
	{:else}
		<div class="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
			<div class="space-y-5">
				<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
					<h2 class="text-sm font-semibold text-slate-800">Calendar drill-down</h2>
					<p class="mt-1 text-xs text-slate-500">Darker cells contain more deduplicated event records. Selecting a cell opens its event set.</p>
					{#if loadingYears}<div class="mt-5 h-72 animate-pulse rounded-lg bg-slate-50"></div>{:else}<div class="mt-5"><TimelineCalendarPanel {years} {months} {days} selectedYear={timelineSelection.year} selectedMonth={timelineSelection.month} selectedDay={timelineSelection.day} onSelectYear={(year) => timelineSelection.selectYear(year)} onSelectMonth={(month) => timelineSelection.selectMonth(month)} onSelectDay={(day) => timelineSelection.selectDay(day)} /></div>{/if}
					{#if loadingMonths || loadingDays}<p class="mt-3 text-[10px] text-slate-400">Updating calendar…</p>{/if}
				</article>

				<article class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm sm:p-5">
					<div class="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between"><div><h2 class="text-sm font-semibold text-slate-800">Category timeline</h2><p class="mt-1 text-xs text-slate-500">Monthly event assignments grouped by disaster taxonomy.</p></div><div class="inline-flex rounded-md border border-slate-200 bg-white p-0.5"><button type="button" onclick={() => (bucket = 'month_year')} class="rounded px-2.5 py-1.5 text-[11px] font-semibold {bucket === 'month_year' ? 'bg-slate-800 text-white' : 'text-slate-500'}">Chronological</button><button type="button" onclick={() => (bucket = 'month_of_year')} class="rounded px-2.5 py-1.5 text-[11px] font-semibold {bucket === 'month_of_year' ? 'bg-slate-800 text-white' : 'text-slate-500'}">Seasonal</button></div></div>
					<div class="mt-5"><StackedCategoryTimeline items={stacks} onselect={selectTimelinePeriod} /></div>
				</article>
			</div>

			<div class="xl:sticky xl:top-4 xl:self-start">
				{#if timelineSelection.showEvents && selectedPrefix}
					<DateEventDrawer datePrefix={selectedPrefix} items={dateEvents} loading={loadingEvents} error={eventError} onclose={() => timelineSelection.closeEvents()} onselect={(event) => (selectedEvent = event)} />
				{:else}
					<div class="rounded-xl border border-dashed border-slate-300 bg-slate-50/60 p-6 text-center"><p class="text-sm font-semibold text-slate-600">Select a calendar cell</p><p class="mt-1 text-xs leading-5 text-slate-400">The matching event records will appear here.</p></div>
				{/if}
			</div>
		</div>
	{/if}
</section>

{#if selectedEvent}<EventDetails event={selectedEvent} onclose={() => (selectedEvent = '')} />{/if}
