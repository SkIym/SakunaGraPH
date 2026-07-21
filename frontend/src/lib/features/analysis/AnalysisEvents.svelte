<script>
	import { analysisFilters, toAnalysisParams } from './state/filters.svelte.js';
	import { getAnalysisEvents } from '$lib/api/analysis.js';
	import EmptyState from '$lib/components/analysis/EmptyState.svelte';
	import EventTable from '$lib/components/analysis/EventTable.svelte';
	import EventTableToolbar from '$lib/components/analysis/EventTableToolbar.svelte';

	const COLUMNS = Object.freeze([
		{ id: 'eventName', label: 'Event', sortable: true },
		{ id: 'eventType', label: 'Record type', sortable: true },
		{ id: 'startDate', label: 'Start date', sortable: true },
		{ id: 'endDate', label: 'End date', sortable: true },
		{ id: 'locations', label: 'Locations', sortable: false },
		{ id: 'disasterTypes', label: 'Disaster types', sortable: false },
		{ id: 'source', label: 'Source', sortable: true },
		{ id: 'impact', label: 'Reported impact', sortable: false },
	]);

	let page = $state(1);
	let pageSize = $state(25);
	let sortBy = $state('startDate');
	let sortDir = $state('desc');
	let visibleColumns = $state(new Set(COLUMNS.map((column) => column.id)));
	let response = $state(null);
	let loading = $state(true);
	let error = $state('');
	let retryToken = $state(0);
	let previousFilterQuery = null;
	let selectedEvent = $state('');
	let EventDetailsComponent = $state(null);

	async function openEventDetails(event) {
		if (!event) return;
		if (!EventDetailsComponent) {
			EventDetailsComponent = (await import('$lib/components/EventDetails.svelte')).default;
		}
		selectedEvent = event;
	}

	const filterQuery = $derived(toAnalysisParams().toString());
	const requestParams = $derived(
		toAnalysisParams({
			page,
			page_size: pageSize,
			sort_by: sortBy,
			sort_dir: sortDir,
		}).toString(),
	);
	const requestVersion = $derived({ query: requestParams, retry: retryToken });
	const exportParams = $derived(
		toAnalysisParams({ sort_by: sortBy, sort_dir: sortDir }).toString(),
	);
	const items = $derived(response?.items ?? []);
	const total = $derived(response?.total ?? 0);
	const totalPages = $derived(Math.max(1, Math.ceil(total / pageSize)));
	const paginationPages = $derived.by(() => {
		if (totalPages <= 7) return Array.from({ length: totalPages }, (_, index) => index + 1);
		if (page <= 4) return [1, 2, 3, 4, 5, '…', totalPages];
		if (page >= totalPages - 3) {
			return [1, '…', ...Array.from({ length: 5 }, (_, index) => totalPages - 4 + index)];
		}
		return [1, '…', page - 1, page, page + 1, '…', totalPages];
	});

	$effect(() => {
		const current = filterQuery;
		if (previousFilterQuery !== null && previousFilterQuery !== current) page = 1;
		previousFilterQuery = current;
	});

	$effect(() => {
		const request = requestVersion;
		const query = request.query;
		const controller = new AbortController();
		loading = true;
		error = '';
		response = null;

		const timer = window.setTimeout(
			async () => {
				try {
					response = await getAnalysisEvents(query, {
						signal: controller.signal,
					});
				} catch (requestError) {
					if (requestError.name !== 'AbortError') {
						error = requestError.message || 'Could not load event records.';
					}
				} finally {
					if (!controller.signal.aborted) loading = false;
				}
			},
			analysisFilters.q.trim() ? 250 : 0,
		);

		return () => {
			window.clearTimeout(timer);
			controller.abort();
		};
	});

	function changePageSize(value) {
		pageSize = value;
		page = 1;
	}

	function toggleColumn(id) {
		const next = new Set(visibleColumns);
		if (next.has(id)) {
			if (next.size === 1) return;
			next.delete(id);
		} else {
			next.add(id);
		}
		visibleColumns = next;
	}

	function sort(column) {
		if (sortBy === column) {
			sortDir = sortDir === 'asc' ? 'desc' : 'asc';
		} else {
			sortBy = column;
			sortDir = column === 'startDate' || column === 'endDate' ? 'desc' : 'asc';
		}
		page = 1;
	}

	function goToPage(value) {
		if (loading || typeof value !== 'number') return;
		page = Math.min(Math.max(value, 1), totalPages);
	}

	function retry() {
		retryToken += 1;
	}
</script>

<svelte:head>
	<title>Event records · SakunaGraPH</title>
</svelte:head>

<section class="mx-auto w-full max-w-[1500px] px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
	<div class="mb-5 flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
		<div>
			<nav
				class="mb-2 flex items-center gap-1.5 text-[10px] text-slate-400"
				aria-label="Breadcrumb"
			>
				<a href="/analysis" class="transition hover:text-slate-600">Analysis</a>
				<span aria-hidden="true">/</span>
				<span class="text-slate-600">Table</span>
			</nav>
			<h1 class="text-xl font-semibold text-slate-800">Disaster event records</h1>
			<p class="mt-1 text-xs leading-5 text-slate-500">
				Filter, compare, and export deduplicated records from every integrated source.
			</p>
		</div>
		<a
			href="/analysis"
			class="text-xs font-semibold text-indigo-600 transition hover:text-indigo-800"
			>Analysis overview</a
		>
	</div>

	<div class="overflow-visible rounded-lg border border-slate-200 bg-white shadow-sm">
		<EventTableToolbar
			{total}
			{page}
			{pageSize}
			{loading}
			columns={COLUMNS}
			{visibleColumns}
			{exportParams}
			onPageSizeChange={changePageSize}
			onToggleColumn={toggleColumn}
		/>

		{#if error}
			<div class="p-4 sm:p-5">
				<EmptyState
					title="Event records are unavailable"
					description={error}
					actionLabel="Try again"
					onaction={retry}
					tone="error"
				/>
			</div>
		{:else if !loading && items.length === 0}
			<div class="p-4 sm:p-5">
				<EmptyState
					title="No event records match this scope"
					description="Adjust the date, location, disaster type, or event-name filters to broaden the result set."
					actionLabel={analysisFilters.hasActiveFilters ? 'Clear all filters' : ''}
					onaction={() => analysisFilters.reset()}
				/>
			</div>
		{:else}
			<EventTable
				{items}
				{loading}
				columns={COLUMNS}
				{visibleColumns}
				{sortBy}
				{sortDir}
				onSort={sort}
				onSelect={(item) => openEventDetails(item.event)}
			/>
		{/if}

		{#if !error && !loading && totalPages > 1}
			<div
				class="flex flex-col gap-3 border-t border-slate-200 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:px-5"
			>
				<p class="text-[10px] tabular-nums text-slate-400">
					Page {page.toLocaleString()} of {totalPages.toLocaleString()}
				</p>
				<nav class="flex flex-wrap items-center gap-1" aria-label="Event table pagination">
					<button
						type="button"
						onclick={() => goToPage(page - 1)}
						disabled={page === 1}
						class="h-8 rounded-md border border-slate-200 px-2.5 text-[11px] font-medium text-slate-600 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
					>
						Previous
					</button>
					{#each paginationPages as paginationPage, index (`${paginationPage}-${index}`)}
						{#if paginationPage === '…'}
							<span class="flex h-8 w-6 items-center justify-center text-xs text-slate-400">…</span>
						{:else}
							<button
								type="button"
								onclick={() => goToPage(paginationPage)}
								aria-current={page === paginationPage ? 'page' : undefined}
								class="h-8 min-w-8 rounded-md border px-2 text-[11px] font-medium tabular-nums transition
									{page === paginationPage
									? 'border-slate-800 bg-slate-800 text-white'
									: 'border-slate-200 text-slate-600 hover:bg-slate-50'}"
							>
								{paginationPage}
							</button>
						{/if}
					{/each}
					<button
						type="button"
						onclick={() => goToPage(page + 1)}
						disabled={page === totalPages}
						class="h-8 rounded-md border border-slate-200 px-2.5 text-[11px] font-medium text-slate-600 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
					>
						Next
					</button>
				</nav>
			</div>
		{/if}
	</div>
</section>

{#if selectedEvent && EventDetailsComponent}
	<EventDetailsComponent event={selectedEvent} onclose={() => (selectedEvent = '')} />
{/if}
