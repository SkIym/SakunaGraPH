<script>
	import { analysisFilters, EVENT_TYPE_OPTIONS } from '$lib/analysis/filters.svelte.js';
	import DateRangeFilter from '$lib/components/analysis/DateRangeFilter.svelte';
	import DisasterTypeFilterTree from '$lib/components/analysis/DisasterTypeFilterTree.svelte';
	import LocationFilterTree from '$lib/components/analysis/LocationFilterTree.svelte';

	let {
		options = null,
		loading = false,
		error = '',
		onRetry = () => {},
		onClose = () => {},
		showClose = false,
	} = $props();
</script>

<div class="filter-panel">
	<header class="filter-panel-header">
		<div class="flex min-w-0 items-center gap-2">
			<div>
				<p>Analysis scope</p>
				<h2>Refine records</h2>
			</div>
			{#if analysisFilters.activeCount > 0}
				<span
					class="brand-count flex h-5 min-w-5 items-center justify-center rounded-full px-1.5 text-[10px] font-semibold tabular-nums"
				>
					{analysisFilters.activeCount}
				</span>
			{/if}
		</div>
		{#if showClose}
			<button
				type="button"
				data-focus-first
				onclick={onClose}
				aria-label="Close filters"
				title="Close filters"
				class="flex h-11 w-11 items-center justify-center rounded-lg text-xl leading-none text-slate-400 transition hover:bg-slate-50 hover:text-slate-700"
			>
				&times;
			</button>
		{/if}
	</header>

	<div class="min-h-0 flex-1 overflow-y-auto overscroll-contain">
		<section aria-labelledby="analysis-event-type-heading" class="filter-section">
			<h2 id="analysis-event-type-heading" class="text-xs font-semibold text-slate-700">
				Event type
			</h2>
			<div class="filter-mode-grid" role="group" aria-label="Event type">
				{#each EVENT_TYPE_OPTIONS as option}
					<button
						type="button"
						onclick={() => analysisFilters.setEventType(option.value)}
						aria-pressed={analysisFilters.eventType === option.value}
						class:active={analysisFilters.eventType === option.value}
					>
						{option.label}
					</button>
				{/each}
			</div>
		</section>

		<section aria-labelledby="analysis-search-heading" class="filter-section">
			<h2 id="analysis-search-heading" class="text-xs font-semibold text-slate-700">
				Event search
			</h2>
			<label class="sr-only" for="analysis-event-search">Search event names</label>
			<input
				id="analysis-event-search"
				type="search"
				value={analysisFilters.q}
				oninput={(event) => analysisFilters.setQuery(event.currentTarget.value)}
				placeholder="Search event names"
				class="analysis-filter-input"
			/>
		</section>

		<DateRangeFilter />

		{#if error}
			<div class="border-b border-red-100 bg-red-50 px-4 py-3" role="alert">
				<p class="text-xs text-red-700">{error}</p>
				<button
					type="button"
					onclick={onRetry}
					class="mt-1.5 text-[11px] font-semibold text-red-700 underline underline-offset-2"
				>
					Try again
				</button>
			</div>
		{/if}

		<LocationFilterTree locations={options?.locations} {loading} />
		<DisasterTypeFilterTree root={options?.disasterTypes} {loading} />
	</div>

	<footer class="filter-panel-footer">
		<span class="text-[10px] text-slate-400">
			{analysisFilters.activeCount === 0 ? 'All records' : `${analysisFilters.activeCount} active`}
		</span>
		<button
			type="button"
			onclick={() => analysisFilters.reset()}
			disabled={!analysisFilters.hasActiveFilters}
			class="brand-link min-h-11 rounded-lg px-2 text-[11px] font-semibold transition disabled:cursor-default disabled:text-slate-300"
		>
			Clear all
		</button>
	</footer>
</div>

<style>
	.filter-panel {
		display: flex;
		height: 100%;
		min-height: 0;
		flex-direction: column;
		background: var(--color-canvas);
	}

	.filter-panel-header {
		display: flex;
		min-height: 4.5rem;
		flex: none;
		align-items: center;
		justify-content: space-between;
		border-bottom: 1px solid var(--color-border);
		padding: 0.8rem 1rem;
	}

	.filter-panel-header p {
		margin: 0;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.5625rem;
		font-weight: 600;
		line-height: 1.3;
		letter-spacing: 0.09em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.filter-panel-header h2 {
		margin: 0.15rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.1rem;
		font-weight: 700;
		line-height: 1.2;
		color: var(--color-text);
	}

	.filter-section {
		border-bottom: 1px solid var(--color-border);
		padding: 1rem;
	}

	.filter-mode-grid {
		display: grid;
		grid-template-columns: repeat(3, minmax(0, 1fr));
		min-height: 2.75rem;
		margin-top: 0.75rem;
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
	}

	.filter-mode-grid button {
		border: 0;
		border-right: 1px solid var(--color-border);
		background: var(--color-canvas);
		padding: 0 0.5rem;
		font-size: 0.6875rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		transition:
			background-color 180ms ease,
			color 180ms ease;
	}

	.filter-mode-grid button:last-child {
		border-right: 0;
	}

	.filter-mode-grid button:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.filter-mode-grid button.active {
		background: var(--color-accent);
		color: var(--color-accent-ink);
	}

	.analysis-filter-input {
		display: block;
		width: 100%;
		height: 2.75rem;
		margin-top: 0.75rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 0.75rem;
		font-size: 0.75rem;
		color: var(--color-text);
		outline: none;
		transition:
			border-color 180ms ease,
			box-shadow 180ms ease;
	}

	.analysis-filter-input:focus {
		border-color: var(--color-brand);
		box-shadow: 0 0 0 2px var(--color-brand-medium);
	}

	.analysis-filter-input::placeholder {
		color: var(--color-text-muted);
	}

	.filter-panel-footer {
		display: flex;
		min-height: 3.25rem;
		flex: none;
		align-items: center;
		justify-content: space-between;
		border-top: 1px solid var(--color-border);
		background: var(--color-canvas);
		padding: 0 1rem;
	}
</style>
