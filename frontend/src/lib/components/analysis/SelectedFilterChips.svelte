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
			<span
				class="flex h-7 max-w-full items-center gap-1.5 rounded-md border border-slate-200 bg-slate-50 px-2 text-[11px] text-slate-600"
			>
				<span
					class="h-1.5 w-1.5 shrink-0 rounded-full {chip.kind === 'location'
						? 'bg-teal-500'
						: chip.kind === 'disasterType'
							? 'bg-amber-500'
							: 'bg-indigo-500'}"
				></span>
				<span class="max-w-48 truncate" title={chip.label}>{chip.label}</span>
				<button
					type="button"
					onclick={() => remove(chip)}
					aria-label="Remove {chip.label} filter"
					title="Remove filter"
					class="flex h-5 w-5 shrink-0 items-center justify-center text-sm leading-none text-slate-400 transition hover:text-slate-700"
				>
					&times;
				</button>
			</span>
		{/each}
		<button
			type="button"
			onclick={() => analysisFilters.reset()}
			class="h-7 px-1.5 text-[11px] font-medium text-indigo-600 transition hover:text-indigo-800"
		>
			Clear all
		</button>
	</div>
{/if}
