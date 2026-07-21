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

<div class="flex h-full min-h-0 flex-col bg-white">
	<div class="flex h-14 shrink-0 items-center justify-between border-b border-slate-200 px-4">
		<div class="flex min-w-0 items-center gap-2">
			<h2 class="text-sm font-semibold text-slate-800">Filters</h2>
			{#if analysisFilters.activeCount > 0}
				<span
					class="flex h-5 min-w-5 items-center justify-center rounded-full bg-indigo-50 px-1.5 text-[10px] font-semibold tabular-nums text-indigo-700"
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
				class="flex h-8 w-8 items-center justify-center text-xl leading-none text-slate-400 transition hover:text-slate-700"
			>
				&times;
			</button>
		{/if}
	</div>

	<div class="min-h-0 flex-1 overflow-y-auto overscroll-contain">
		<section
			aria-labelledby="analysis-event-type-heading"
			class="border-b border-slate-200 px-4 py-4"
		>
			<h2 id="analysis-event-type-heading" class="text-xs font-semibold text-slate-700">
				Event type
			</h2>
			<div
				class="mt-3 grid h-9 grid-cols-3 overflow-hidden rounded-md border border-slate-200"
				role="group"
				aria-label="Event type"
			>
				{#each EVENT_TYPE_OPTIONS as option}
					<button
						type="button"
						onclick={() => analysisFilters.setEventType(option.value)}
						aria-pressed={analysisFilters.eventType === option.value}
						class="border-r border-slate-200 px-2 text-[11px] font-medium transition last:border-r-0
						{analysisFilters.eventType === option.value
							? 'bg-indigo-600 text-white'
							: 'bg-white text-slate-500 hover:bg-slate-50 hover:text-slate-700'}"
					>
						{option.label}
					</button>
				{/each}
			</div>
		</section>

		<section aria-labelledby="analysis-search-heading" class="border-b border-slate-200 px-4 py-4">
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
				class="mt-3 block h-9 w-full rounded-md border border-slate-200 bg-white px-3 text-xs text-slate-700 outline-none transition placeholder:text-slate-400 focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100"
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

	<div
		class="flex h-12 shrink-0 items-center justify-between border-t border-slate-200 bg-white px-4"
	>
		<span class="text-[10px] text-slate-400">
			{analysisFilters.activeCount === 0 ? 'All records' : `${analysisFilters.activeCount} active`}
		</span>
		<button
			type="button"
			onclick={() => analysisFilters.reset()}
			disabled={!analysisFilters.hasActiveFilters}
			class="text-[11px] font-semibold text-indigo-600 transition hover:text-indigo-800 disabled:cursor-default disabled:text-slate-300"
		>
			Clear all
		</button>
	</div>
</div>
