<script>
	import { analysisFilters } from '$lib/analysis/filters.svelte.js';

	let { root = null, loading = false } = $props();

	let sectionOpen = $state(true);
	let search = $state('');
	let expanded = $state(new Set());

	const normalizedSearch = $derived(search.trim().toLocaleLowerCase());

	function flatten(nodes, depth = 0, ancestors = []) {
		const rows = [];
		for (const node of nodes ?? []) {
			rows.push({ node, depth, ancestors });
			rows.push(...flatten(node.children, depth + 1, [...ancestors, node.id]));
		}
		return rows;
	}

	const rows = $derived(flatten(root?.children ?? []));
	const matchingIds = $derived.by(() => {
		if (!normalizedSearch) return null;
		const ids = new Set();
		for (const row of rows) {
			const matched = [row.node.label, row.node.id]
				.filter(Boolean)
				.some((value) => value.toLocaleLowerCase().includes(normalizedSearch));
			if (!matched) continue;
			ids.add(row.node.id);
			row.ancestors.forEach((id) => ids.add(id));
		}
		return ids;
	});
	const visibleRows = $derived(
		rows.filter((row) => {
			if (matchingIds) return matchingIds.has(row.node.id);
			return row.ancestors.every((id) => expanded.has(id));
		})
	);

	const groupColors = {
		natural: '#0f766e',
		biological: '#16a34a',
		climatological: '#d97706',
		extraterrestrial: '#6366f1',
		geophysical: '#b45309',
		hydrological: '#0284c7',
		meteorological: '#2563eb',
		tech: '#dc2626',
		armedconflict: '#7c3aed',
		industrial: '#ea580c',
		miscellaneous: '#64748b',
		transport: '#ca8a04'
	};

	function isOpen(id) {
		return Boolean(normalizedSearch) || expanded.has(id);
	}

	function toggleExpanded(id) {
		const next = new Set(expanded);
		if (next.has(id)) next.delete(id);
		else next.add(id);
		expanded = next;
	}

	function colorFor(group) {
		return groupColors[group] ?? '#64748b';
	}
</script>

<section aria-labelledby="analysis-disaster-heading">
	<button
		type="button"
		onclick={() => (sectionOpen = !sectionOpen)}
		aria-expanded={sectionOpen}
		aria-controls="analysis-disaster-content"
		class="flex w-full items-center justify-between gap-3 px-4 py-4 text-left transition hover:bg-slate-50/70"
	>
		<span id="analysis-disaster-heading" class="text-xs font-semibold text-slate-700">Disaster type</span>
		<span class="flex items-center gap-2">
			{#if analysisFilters.disasterTypes.length > 0}
				<span class="text-[10px] font-semibold tabular-nums text-indigo-600">
					{analysisFilters.disasterTypes.length} selected
				</span>
			{/if}
			<span class="flex h-5 w-5 items-center justify-center text-sm text-slate-400 transition-transform {sectionOpen ? 'rotate-90' : ''}" aria-hidden="true">&rsaquo;</span>
		</span>
	</button>

	{#if sectionOpen}
		<div id="analysis-disaster-content" class="px-4 pb-4">
			<label class="sr-only" for="analysis-disaster-search">Search disaster types</label>
			<input
				id="analysis-disaster-search"
				type="search"
				bind:value={search}
				placeholder="Search disaster types"
				class="block h-9 w-full rounded-md border border-slate-200 bg-white px-3 text-xs text-slate-700 outline-none transition placeholder:text-slate-400 focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100"
			/>

			<div class="mt-3 space-y-0.5">
		{#if loading}
			{#each [1, 2, 3, 4] as row}
				<div class="flex h-8 items-center gap-2 px-1" aria-hidden="true">
					<span class="h-2.5 w-2.5 animate-pulse rounded-sm bg-slate-100"></span>
					<span class="h-3 animate-pulse rounded bg-slate-100" style="width:{45 + row * 9}%"></span>
				</div>
			{/each}
		{:else if rows.length === 0}
			<p class="py-3 text-xs text-slate-400">No disaster types available.</p>
		{:else}
			{#each visibleRows as row (row.node.id)}
				{@const hasChildren = (row.node.children?.length ?? 0) > 0}
				{@const open = hasChildren && isOpen(row.node.id)}
				<div
					class="flex min-h-8 items-center gap-1 rounded pr-1 hover:bg-slate-50"
					style="padding-left:{Math.min(row.depth, 5) * 14}px"
				>
					{#if hasChildren}
						<button
							type="button"
							onclick={() => toggleExpanded(row.node.id)}
							aria-label="{open ? 'Collapse' : 'Expand'} {row.node.label}"
							aria-expanded={open}
							class="flex h-6 w-6 shrink-0 items-center justify-center text-sm text-slate-400 transition hover:text-slate-700"
						>
							<span class="transition-transform {open ? 'rotate-90' : ''}">&rsaquo;</span>
						</button>
					{:else}
						<span class="h-6 w-6 shrink-0"></span>
					{/if}
					<input
						id="disaster-type-{row.node.id}"
						type="checkbox"
						checked={analysisFilters.disasterTypes.includes(row.node.id)}
						onchange={() => analysisFilters.toggleDisasterType(row.node.id)}
						class="h-3.5 w-3.5 shrink-0 cursor-pointer accent-indigo-600"
					/>
					<span class="h-2 w-2 shrink-0 rounded-sm" style="background:{colorFor(row.node.group)}"></span>
					<label
						for="disaster-type-{row.node.id}"
						title={row.node.label}
						class="min-w-0 flex-1 cursor-pointer truncate text-xs {hasChildren ? 'font-medium text-slate-700' : 'text-slate-600'}"
					>
						{row.node.label}
					</label>
				</div>
			{/each}

			{#if normalizedSearch && visibleRows.length === 0}
				<p class="py-3 text-xs text-slate-400">No matching disaster types.</p>
			{/if}
		{/if}
			</div>
		</div>
	{/if}
</section>
