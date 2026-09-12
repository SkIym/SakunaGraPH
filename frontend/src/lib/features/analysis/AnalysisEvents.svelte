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

<section class="analysis-page analysis-page-wide">
	<header class="analysis-page-header">
		<div>
			<p class="workspace-kicker">Analysis / Table</p>
			<h1>Disaster event records</h1>
			<p class="analysis-page-copy">
				Filter, compare, and export the linked event records available in the current graph.
			</p>
		</div>
		<a href="/analysis" class="analysis-back-link">← Analysis overview</a>
	</header>

	<div class="analysis-surface">
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
			<div class="analysis-pagination">
				<p class="text-[10px] tabular-nums text-slate-400">
					Page {page.toLocaleString()} of {totalPages.toLocaleString()}
				</p>
				<nav class="flex flex-wrap items-center gap-1" aria-label="Event table pagination">
					<button
						type="button"
						onclick={() => goToPage(page - 1)}
						disabled={page === 1}
						class="pagination-step"
					>
						Previous
					</button>
					{#each paginationPages as paginationPage, index (`${paginationPage}-${index}`)}
						{#if paginationPage === '…'}
							<span class="flex h-11 w-6 items-center justify-center text-xs text-slate-400">…</span
							>
						{:else}
							<button
								type="button"
								onclick={() => goToPage(paginationPage)}
								aria-current={page === paginationPage ? 'page' : undefined}
								class="pagination-page"
								class:current={page === paginationPage}
							>
								{paginationPage}
							</button>
						{/if}
					{/each}
					<button
						type="button"
						onclick={() => goToPage(page + 1)}
						disabled={page === totalPages}
						class="pagination-step"
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

<style>
	.analysis-pagination {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		padding: 0.8rem 1.25rem;
		border-top: 1px solid var(--color-border);
	}

	.pagination-step,
	.pagination-page {
		min-height: 2.75rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding-inline: 0.75rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		transition:
			background-color 160ms ease,
			border-color 160ms ease,
			transform 120ms ease;
	}

	.pagination-page {
		min-width: 2.75rem;
		padding-inline: 0.5rem;
		font-variant-numeric: tabular-nums;
	}

	.pagination-step:hover:not(:disabled),
	.pagination-page:hover {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
	}

	.pagination-step:active:not(:disabled),
	.pagination-page:active {
		transform: translateY(1px);
	}

	.pagination-page.current {
		border-color: #e2b800;
		background: var(--color-accent);
		color: var(--color-accent-ink);
	}

	.pagination-step:disabled {
		cursor: not-allowed;
		opacity: 0.45;
	}

	@media (max-width: 40rem) {
		.analysis-pagination {
			align-items: start;
			flex-direction: column;
		}
	}
</style>
