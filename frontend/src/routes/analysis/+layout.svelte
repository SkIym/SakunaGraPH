<script>
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { analysisFilters } from '$lib/analysis/filters.svelte.js';
	import { apiUrl } from '$lib/api.js';
	import FilterPanel from '$lib/components/analysis/FilterPanel.svelte';
	import SelectedFilterChips from '$lib/components/analysis/SelectedFilterChips.svelte';

	let { children } = $props();

	let options = $state(null);
	let loading = $state(true);
	let error = $state('');
	let mobileFiltersOpen = $state(false);
	const ANALYSIS_VIEWS = [
		{ href: '/analysis/events', label: 'Events' },
		{ href: '/analysis/metrics', label: 'Metrics' }
	];

	async function loadFilterOptions(signal) {
		loading = true;
		error = '';
		try {
			const response = await fetch(apiUrl('/api/analysis/filter-options'), { signal });
			const data = await response.json();
			if (!response.ok) throw new Error(data.detail ?? 'Request failed');
			options = data;
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			error = 'Filter metadata is unavailable.';
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		const controller = new AbortController();
		void loadFilterOptions(controller.signal);
		return () => controller.abort();
	});

	function handleKeydown(event) {
		if (event.key === 'Escape') mobileFiltersOpen = false;
	}
</script>

<svelte:head>
	<title>Analysis · SakunaGraPH</title>
</svelte:head>

<svelte:window onkeydown={handleKeydown} />

<div class="relative flex overflow-hidden bg-white" style="height:calc(100vh - 52px);">
	<aside class="hidden h-full w-[304px] shrink-0 border-r border-slate-200 lg:block" aria-label="Analysis filters">
		<FilterPanel
			{options}
			{loading}
			{error}
			onRetry={() => loadFilterOptions()}
		/>
	</aside>

	{#if mobileFiltersOpen}
		<button
			type="button"
			onclick={() => (mobileFiltersOpen = false)}
			aria-label="Close filters"
			class="absolute inset-0 z-30 bg-slate-900/25 lg:hidden"
		></button>
		<div
			role="dialog"
			aria-modal="true"
			aria-label="Analysis filters"
			class="absolute inset-y-0 left-0 z-40 w-[min(88vw,320px)] border-r border-slate-200 bg-white shadow-xl lg:hidden"
		>
			<FilterPanel
				{options}
				{loading}
				{error}
				showClose={true}
				onClose={() => (mobileFiltersOpen = false)}
				onRetry={() => loadFilterOptions()}
			/>
		</div>
	{/if}

	<main class="min-w-0 flex-1 overflow-y-auto">
		<header class="sticky top-0 z-10 border-b border-slate-200 bg-white/95 px-4 py-3 backdrop-blur-sm sm:px-6 lg:px-8">
			<div class="flex min-h-9 min-w-0 items-center gap-3">
				<button
					type="button"
					onclick={() => (mobileFiltersOpen = true)}
					class="flex h-9 shrink-0 items-center gap-1.5 rounded-md border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 lg:hidden"
					aria-label="Open analysis filters"
				>
					Filters
					{#if analysisFilters.activeCount > 0}
						<span class="flex h-4 min-w-4 items-center justify-center rounded-full bg-indigo-600 px-1 text-[9px] text-white">
							{analysisFilters.activeCount}
						</span>
					{/if}
				</button>

				<div class="hidden shrink-0 lg:block">
					<p class="text-[9px] font-semibold uppercase text-slate-400" style="letter-spacing:0.12em;">Workspace</p>
					<h1 class="text-sm font-semibold text-slate-800">Analysis</h1>
				</div>

				<div class="hidden h-8 w-px shrink-0 bg-slate-200 lg:block"></div>
				<nav
					aria-label="Analysis views"
					class="flex shrink-0 items-center gap-0.5 rounded-full border border-slate-200 bg-slate-50 p-0.5"
				>
					{#each ANALYSIS_VIEWS as view}
						{@const active = $page.url.pathname === view.href}
						<a
							href={view.href}
							aria-current={active ? 'page' : undefined}
							class="rounded-full px-3 py-1.5 text-[11px] font-semibold transition {active ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-500 hover:bg-white hover:text-slate-700'}"
						>
							{view.label}
						</a>
					{/each}
				</nav>
				<div class="min-w-0 flex-1">
					{#if analysisFilters.hasActiveFilters}
						<SelectedFilterChips locations={options?.locations} taxonomy={options?.disasterTypes} />
					{:else}
						<span class="text-xs text-slate-400">All records</span>
					{/if}
				</div>
			</div>
		</header>

		{@render children()}
	</main>
</div>
