<script>
	import { analysisFilters, EVENT_TYPE_OPTIONS } from '$lib/analysis/filters.svelte.js';

	const eventTypeLabels = new Map(EVENT_TYPE_OPTIONS.map((option) => [option.value, option.label]));
	const dateWindow = $derived.by(() => {
		if (analysisFilters.startDate && analysisFilters.endDate) {
			return `${analysisFilters.startDate} to ${analysisFilters.endDate}`;
		}
		if (analysisFilters.startDate) return `From ${analysisFilters.startDate}`;
		if (analysisFilters.endDate) return `Through ${analysisFilters.endDate}`;
		return 'All dates';
	});

	const scopeItems = $derived([
		{
			label: 'Event records',
			value: eventTypeLabels.get(analysisFilters.eventType),
			color: '#6366f1'
		},
		{ label: 'Date window', value: dateWindow, color: '#0284c7' },
		{
			label: 'Locations',
			value: analysisFilters.locationIds.length ? `${analysisFilters.locationIds.length} selected` : 'All locations',
			color: '#0f766e'
		},
		{
			label: 'Disaster types',
			value: analysisFilters.disasterTypes.length ? `${analysisFilters.disasterTypes.length} selected` : 'All types',
			color: '#d97706'
		},
		{
			label: 'Event name',
			value: analysisFilters.q.trim() || 'Any event name',
			color: '#64748b'
		}
	]);
</script>

<section class="mx-auto w-full max-w-6xl px-4 py-8 sm:px-6 lg:px-10 lg:py-10">
	<div class="border-b border-slate-200 pb-5">
		<p class="text-[10px] font-semibold uppercase text-indigo-600" style="letter-spacing:0.12em;">Analysis</p>
		<h2 class="mt-1 text-xl font-semibold text-slate-800">Current scope</h2>
	</div>

	<div class="grid border-b border-slate-200 sm:grid-cols-2 xl:grid-cols-5">
		{#each scopeItems as item, index}
			<div class="min-w-0 border-b border-slate-100 px-1 py-5 sm:px-4 xl:border-b-0 {index % 2 === 0 ? 'sm:border-r' : ''} xl:border-r xl:last:border-r-0">
				<div class="flex items-center gap-2">
					<span class="h-2 w-2 shrink-0 rounded-sm" style="background:{item.color}"></span>
					<p class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.08em;">{item.label}</p>
				</div>
				<p class="mt-2 truncate text-sm font-medium text-slate-700" title={item.value}>{item.value}</p>
			</div>
		{/each}
	</div>

	<div class="grid gap-4 border-b border-slate-200 py-6 lg:grid-cols-2">
		<div class="rounded-lg border border-slate-200 p-4">
			<p class="text-sm font-semibold text-slate-800">Event records</p>
			<p class="mt-1 text-xs leading-5 text-slate-500">Browse the filtered event table or export the complete result set as CSV.</p>
			<a href="/analysis/events" class="mt-4 inline-flex h-8 items-center justify-center rounded-md bg-slate-800 px-3 text-[11px] font-semibold text-white transition hover:bg-slate-700">Open event table</a>
		</div>
		<div class="rounded-lg border border-slate-200 p-4">
			<p class="text-sm font-semibold text-slate-800">Metrics dashboard</p>
			<p class="mt-1 text-xs leading-5 text-slate-500">Compare totals, distributions, regional rankings, and annual victim trends in the same filter scope.</p>
			<a href="/analysis/metrics" class="mt-4 inline-flex h-8 items-center justify-center rounded-md bg-indigo-600 px-3 text-[11px] font-semibold text-white transition hover:bg-indigo-700">Open metrics dashboard</a>
		</div>
	</div>
</section>
