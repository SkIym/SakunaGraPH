<script>
	import { analysisFilters, EVENT_TYPE_OPTIONS } from '$lib/analysis/filters.svelte.js';

	let { locations = null, taxonomy = null } = $props();

	function flattenTaxonomy(nodes) {
		return (nodes ?? []).flatMap((node) => [node, ...flattenTaxonomy(node.children)]);
	}

	const locationLabels = $derived(
		new Map((locations?.nodes ?? []).map((node) => [node.id, node.label])),
	);
	const disasterLabels = $derived(
		new Map(flattenTaxonomy(taxonomy?.children).map((node) => [node.id, node.label])),
	);
	const eventTypeLabels = new Map(EVENT_TYPE_OPTIONS.map((option) => [option.value, option.label]));

	const chips = $derived.by(() => {
		const values = [];
		if (analysisFilters.eventType !== 'all') {
			values.push({
				key: 'event-type',
				kind: 'eventType',
				label: `Event: ${eventTypeLabels.get(analysisFilters.eventType)}`,
			});
		}
		if (analysisFilters.q.trim()) {
			values.push({ key: 'query', kind: 'query', label: `Search: ${analysisFilters.q.trim()}` });
		}
		if (analysisFilters.startDate) {
			values.push({
				key: 'start-date',
				kind: 'startDate',
				label: `From ${analysisFilters.startDate}`,
			});
		}
		if (analysisFilters.endDate) {
			values.push({ key: 'end-date', kind: 'endDate', label: `To ${analysisFilters.endDate}` });
		}
		for (const id of analysisFilters.locationIds) {
			values.push({
				key: `location-${id}`,
				kind: 'location',
				id,
				label: locationLabels.get(id) ?? id,
			});
		}
		for (const id of analysisFilters.disasterTypes) {
			values.push({
				key: `disaster-${id}`,
				kind: 'disasterType',
				id,
				label: disasterLabels.get(id) ?? id,
			});
		}
		return values;
	});

	function remove(chip) {
		switch (chip.kind) {
			case 'eventType':
				analysisFilters.setEventType('all');
				break;
			case 'query':
				analysisFilters.setQuery('');
				break;
			case 'startDate':
				analysisFilters.setStartDate('');
				break;
			case 'endDate':
				analysisFilters.setEndDate('');
				break;
			case 'location':
				analysisFilters.removeLocation(chip.id);
				break;
			case 'disasterType':
				analysisFilters.removeDisasterType(chip.id);
				break;
		}
	}
</script>

{#if chips.length > 0}
	<div class="flex min-w-0 flex-wrap items-center gap-1.5" aria-label="Selected analysis filters">
		{#each chips as chip (chip.key)}
			<span class="selected-filter-chip">
				<span class="filter-chip-mark" aria-hidden="true"></span>
				<span class="max-w-48 truncate" title={chip.label}>{chip.label}</span>
				<button
					type="button"
					onclick={() => remove(chip)}
					aria-label="Remove {chip.label} filter"
					title="Remove filter"
					class="flex h-11 w-11 shrink-0 items-center justify-center rounded-lg text-sm leading-none text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
				>
					&times;
				</button>
			</span>
		{/each}
		<button
			type="button"
			onclick={() => analysisFilters.reset()}
			class="brand-link min-h-11 rounded-lg px-2 text-[11px] font-semibold transition"
		>
			Clear all
		</button>
	</div>
{/if}

<style>
	.selected-filter-chip {
		display: flex;
		min-height: 2.75rem;
		max-width: 100%;
		align-items: center;
		gap: 0.5rem;
		border: 1px solid #ead26a;
		border-radius: var(--radius-control);
		background: var(--color-accent-soft);
		padding-left: 0.75rem;
		font-size: 0.6875rem;
		color: var(--color-text-secondary);
	}

	.filter-chip-mark {
		width: 2px;
		height: 1rem;
		flex: none;
		background: var(--color-brand);
	}
</style>
