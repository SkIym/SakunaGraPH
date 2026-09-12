<script>
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	import { analysisFilters } from './state/filters.svelte.js';
	import { getAnalysisFilterOptions } from '$lib/api/analysis.js';
	import FilterPanel from '$lib/components/analysis/FilterPanel.svelte';
	import SelectedFilterChips from '$lib/components/analysis/SelectedFilterChips.svelte';
	import { focusTrap } from '../../actions/focus.js';

	let { children } = $props();

	let options = $state(null);
	let loading = $state(true);
	let error = $state('');
	let mobileFiltersOpen = $state(false);
	let filterRequest = null;
	const ANALYSIS_VIEWS = [
		{ href: '/analysis', label: 'Overview' },
		{ href: '/analysis/events', label: 'Table' },
		{ href: '/analysis/metrics', label: 'Metrics' },
		{ href: '/analysis/timeline', label: 'Timeline' },
	];

	async function loadFilterOptions() {
		filterRequest?.abort();
		const controller = new AbortController();
		filterRequest = controller;
		loading = true;
		error = '';
		try {
			options = await getAnalysisFilterOptions({ signal: controller.signal });
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			error = 'Filter metadata is unavailable.';
		} finally {
			if (!controller.signal.aborted) loading = false;
		}
	}

	onMount(() => {
		void loadFilterOptions();
		return () => filterRequest?.abort();
	});

	function handleKeydown(event) {
		if (event.key === 'Escape') mobileFiltersOpen = false;
	}
</script>

<svelte:head>
	<title>Analysis · SakunaGraPH</title>
</svelte:head>

<svelte:window onkeydown={handleKeydown} />

<div class="analysis-workspace analysis-viewport">
	<aside class="analysis-filter-rail hidden lg:block" aria-label="Analysis filters">
		<FilterPanel {options} {loading} {error} onRetry={() => loadFilterOptions()} />
	</aside>

	{#if mobileFiltersOpen}
		<button
			type="button"
			onclick={() => (mobileFiltersOpen = false)}
			aria-label="Close filters"
			class="analysis-filter-backdrop lg:hidden"
		></button>
		<div
			use:focusTrap
			role="dialog"
			aria-modal="true"
			aria-label="Analysis filters"
			class="analysis-filter-drawer lg:hidden"
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

	<main class="analysis-main">
		<header class="analysis-toolbar">
			<div class="analysis-toolbar-row">
				<button
					type="button"
					onclick={() => (mobileFiltersOpen = true)}
					class="analysis-filter-trigger lg:hidden"
					aria-label="Open analysis filters"
				>
					<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
						<path d="M4 6h16M7 12h10M10 18h4" />
					</svg>
					Filters
					{#if analysisFilters.activeCount > 0}
						<span
							class="brand-count flex h-5 min-w-5 items-center justify-center rounded-full px-1 text-[9px] font-semibold"
						>
							{analysisFilters.activeCount}
						</span>
					{/if}
				</button>

				<a href="/analysis" class="analysis-workspace-id">
					<span>Research desk</span>
					<strong>Analysis</strong>
				</a>

				<nav aria-label="Analysis views" class="analysis-view-nav">
					{#each ANALYSIS_VIEWS as view}
						{@const active = $page.url.pathname === view.href}
						<a href={view.href} aria-current={active ? 'page' : undefined} class:active>
							{view.label}
						</a>
					{/each}
				</nav>
				<div class="analysis-active-scope">
					{#if analysisFilters.hasActiveFilters}
						<SelectedFilterChips locations={options?.locations} taxonomy={options?.disasterTypes} />
					{:else}
						<span class="analysis-all-records">All records</span>
					{/if}
				</div>
			</div>
		</header>

		{@render children()}
	</main>
</div>

<style>
	.analysis-workspace {
		position: relative;
		display: flex;
		overflow: hidden;
		background: var(--color-canvas);
	}

	.analysis-filter-rail {
		width: 18.5rem;
		height: 100%;
		flex: none;
		border-right: 1px solid var(--color-border);
		background: var(--color-canvas);
	}

	.analysis-filter-backdrop {
		position: absolute;
		inset: 0;
		z-index: 30;
		border: 0;
		background: rgb(30 41 59 / 0.32);
	}

	.analysis-filter-drawer {
		position: absolute;
		inset: 0 auto 0 0;
		z-index: 40;
		width: min(90vw, 21rem);
		border-right: 1px solid var(--color-border);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
	}

	.analysis-main {
		min-width: 0;
		flex: 1;
		overflow-y: auto;
		background-color: var(--color-surface-subtle);
		background-image: radial-gradient(rgb(0 56 168 / 0.09) 0.55px, transparent 0.55px);
		background-size: 2rem 2rem;
	}

	.analysis-toolbar {
		position: sticky;
		top: 0;
		z-index: 20;
		border-bottom: 1px solid var(--color-border);
		background: rgb(255 255 255 / 0.96);
		backdrop-filter: blur(0.75rem);
	}

	.analysis-toolbar-row {
		display: flex;
		min-height: 4.5rem;
		min-width: 0;
		align-items: center;
		gap: 1rem;
		padding: 0.75rem clamp(1rem, 2vw, 2rem);
	}

	.analysis-workspace-id {
		display: grid;
		flex: none;
		text-decoration: none;
	}

	.analysis-workspace-id span {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.5625rem;
		font-weight: 600;
		line-height: 1.2;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.analysis-workspace-id strong {
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1rem;
		line-height: 1.25;
		color: var(--color-text);
	}

	.analysis-view-nav {
		display: flex;
		min-width: 0;
		flex: none;
		align-self: stretch;
		align-items: center;
		gap: 0.125rem;
	}

	.analysis-view-nav a {
		position: relative;
		display: inline-flex;
		min-height: 2.75rem;
		align-items: center;
		padding: 0 0.8rem;
		border-radius: var(--radius-control);
		font-size: 0.75rem;
		font-weight: 600;
		text-decoration: none;
		color: var(--color-text-muted);
		transition:
			background-color 180ms ease,
			color 180ms ease,
			transform 120ms ease;
	}

	.analysis-view-nav a:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.analysis-view-nav a:active {
		transform: translateY(1px);
	}

	.analysis-view-nav a.active {
		background: var(--color-accent);
		color: var(--color-accent-ink);
	}

	.analysis-view-nav a.active::after {
		position: absolute;
		inset: auto 0.75rem 0.35rem;
		height: 1px;
		background: var(--color-brand);
		content: '';
	}

	.analysis-active-scope {
		min-width: 0;
		flex: 1;
	}

	.analysis-all-records {
		font-size: 0.75rem;
		color: var(--color-text-muted);
	}

	.analysis-filter-trigger {
		display: inline-flex;
		min-height: 2.75rem;
		flex: none;
		align-items: center;
		gap: 0.45rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 0.75rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		box-shadow: var(--shadow-control);
	}

	.analysis-filter-trigger svg {
		width: 1rem;
		height: 1rem;
		stroke: currentColor;
		stroke-width: 1.7;
		stroke-linecap: round;
	}

	@media (max-width: 63.999rem) {
		.analysis-toolbar-row {
			flex-wrap: wrap;
			gap: 0.5rem;
			padding-block: 0.65rem;
		}

		.analysis-workspace-id {
			display: none;
		}

		.analysis-view-nav {
			min-width: 0;
			flex: 1;
			overflow-x: auto;
			scrollbar-width: none;
		}

		.analysis-view-nav a {
			padding-inline: 0.7rem;
		}

		.analysis-active-scope {
			flex-basis: 100%;
		}
	}

	@media (min-width: 64rem) {
		.analysis-filter-trigger {
			display: none;
		}
	}

	@media (max-width: 27rem) {
		.analysis-view-nav a {
			padding-inline: 0.55rem;
			font-size: 0.6875rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.analysis-view-nav a {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.analysis-main {
			background: Canvas;
		}

		.analysis-view-nav a.active {
			border: 1px solid Highlight;
			background: Highlight;
			color: HighlightText;
		}
	}
</style>
