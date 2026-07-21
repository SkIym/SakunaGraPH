<script>
	import { analysisFilters } from '$lib/analysis/filters.svelte.js';

	let { locations = null, loading = false } = $props();

	let sectionOpen = $state(true);
	let search = $state('');
	let expanded = $state(new Set());

	const nodes = $derived(locations?.nodes ?? []);
	const regions = $derived(
		nodes
			.filter((node) => node.level === 'Region')
			.toSorted((a, b) => a.label.localeCompare(b.label)),
	);
	const normalizedSearch = $derived(search.trim().toLocaleLowerCase());

	const provincesByRegion = $derived.by(() => {
		const groups = new Map();
		for (const node of nodes) {
			if (node.level !== 'Province') continue;
			if (!groups.has(node.regionId)) groups.set(node.regionId, []);
			groups.get(node.regionId).push(node);
		}
		for (const values of groups.values()) values.sort((a, b) => a.label.localeCompare(b.label));
		return groups;
	});

	const localitiesByParent = $derived.by(() => {
		const groups = new Map();
		for (const node of nodes) {
			if (node.level !== 'City' && node.level !== 'Municipality') continue;
			const parent = node.parentId || node.regionId;
			if (!groups.has(parent)) groups.set(parent, []);
			groups.get(parent).push(node);
		}
		for (const values of groups.values()) values.sort((a, b) => a.label.localeCompare(b.label));
		return groups;
	});

	function matches(node) {
		if (!normalizedSearch) return true;
		return [node.label, node.fullName, node.psgcCode]
			.filter(Boolean)
			.some((value) => value.toLocaleLowerCase().includes(normalizedSearch));
	}

	function visibleLocalities(parentId) {
		const values = localitiesByParent.get(parentId) ?? [];
		return normalizedSearch ? values.filter(matches) : values;
	}

	function visibleProvinces(regionId) {
		const values = provincesByRegion.get(regionId) ?? [];
		if (!normalizedSearch) return values;
		return values.filter(
			(province) => matches(province) || visibleLocalities(province.id).length > 0,
		);
	}

	function visibleDirectLocalities(regionId) {
		return visibleLocalities(regionId);
	}

	function visibleRegion(region) {
		return (
			!normalizedSearch ||
			matches(region) ||
			visibleProvinces(region.id).length > 0 ||
			visibleDirectLocalities(region.id).length > 0
		);
	}

	function isOpen(id) {
		return Boolean(normalizedSearch) || expanded.has(id);
	}

	function toggleExpanded(id) {
		const next = new Set(expanded);
		if (next.has(id)) next.delete(id);
		else next.add(id);
		expanded = next;
	}
</script>

<section aria-labelledby="analysis-location-heading" class="border-b border-slate-200">
	<button
		type="button"
		onclick={() => (sectionOpen = !sectionOpen)}
		aria-expanded={sectionOpen}
		aria-controls="analysis-location-content"
		class="flex w-full items-center justify-between gap-3 px-4 py-4 text-left transition hover:bg-slate-50/70"
	>
		<span id="analysis-location-heading" class="text-xs font-semibold text-slate-700">Location</span
		>
		<span class="flex items-center gap-2">
			{#if analysisFilters.locationIds.length > 0}
				<span class="text-[10px] font-semibold tabular-nums text-indigo-600">
					{analysisFilters.locationIds.length} selected
				</span>
			{/if}
			<span
				class="flex h-5 w-5 items-center justify-center text-sm text-slate-400 transition-transform {sectionOpen
					? 'rotate-90'
					: ''}"
				aria-hidden="true">&rsaquo;</span
			>
		</span>
	</button>

	{#if sectionOpen}
		<div id="analysis-location-content" class="px-4 pb-4">
			<label class="sr-only" for="analysis-location-search">Search locations</label>
			<input
				id="analysis-location-search"
				type="search"
				bind:value={search}
				placeholder="Search locations"
				class="block h-9 w-full rounded-md border border-slate-200 bg-white px-3 text-xs text-slate-700 outline-none transition placeholder:text-slate-400 focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100"
			/>

			<div class="mt-3 space-y-0.5">
				{#if loading}
					{#each [1, 2, 3, 4] as row}
						<div class="flex h-8 items-center gap-2 px-1" aria-hidden="true">
							<span class="h-3 w-3 animate-pulse rounded-sm bg-slate-100"></span>
							<span class="h-3 animate-pulse rounded bg-slate-100" style="width:{48 + row * 8}%"
							></span>
						</div>
					{/each}
				{:else if regions.length === 0}
					<p class="py-3 text-xs text-slate-400">No locations available.</p>
				{:else}
					{#each regions.filter(visibleRegion) as region (region.id)}
						{@const provinces = visibleProvinces(region.id)}
						{@const directLocalities = visibleDirectLocalities(region.id)}
						{@const hasChildren = provinces.length > 0 || directLocalities.length > 0}
						{@const regionOpen = hasChildren && isOpen(region.id)}
						<div>
							<div class="group flex min-h-8 items-center gap-1 rounded px-0.5 hover:bg-slate-50">
								{#if hasChildren}
									<button
										type="button"
										onclick={() => toggleExpanded(region.id)}
										aria-label="{regionOpen ? 'Collapse' : 'Expand'} {region.label}"
										aria-expanded={regionOpen}
										class="flex h-6 w-6 shrink-0 items-center justify-center text-sm text-slate-400 transition hover:text-slate-700"
									>
										<span class="transition-transform {regionOpen ? 'rotate-90' : ''}"
											>&rsaquo;</span
										>
									</button>
								{:else}
									<span class="h-6 w-6 shrink-0"></span>
								{/if}
								<input
									id="location-{region.id}"
									type="checkbox"
									checked={analysisFilters.locationIds.includes(region.id)}
									onchange={() => analysisFilters.toggleLocation(region.id)}
									class="h-3.5 w-3.5 shrink-0 cursor-pointer accent-indigo-600"
								/>
								<label
									for="location-{region.id}"
									title={region.fullName || region.label}
									class="min-w-0 flex-1 cursor-pointer truncate text-xs font-medium text-slate-700"
								>
									{region.label}
								</label>
							</div>

							{#if regionOpen}
								<div class="ml-4 border-l border-slate-100 pl-2">
									{#each provinces as province (province.id)}
										{@const localities = visibleLocalities(province.id)}
										{@const provinceOpen = localities.length > 0 && isOpen(province.id)}
										<div>
											<div class="flex min-h-8 items-center gap-1 rounded px-0.5 hover:bg-slate-50">
												{#if localities.length > 0}
													<button
														type="button"
														onclick={() => toggleExpanded(province.id)}
														aria-label="{provinceOpen ? 'Collapse' : 'Expand'} {province.label}"
														aria-expanded={provinceOpen}
														class="flex h-6 w-6 shrink-0 items-center justify-center text-sm text-slate-400 transition hover:text-slate-700"
													>
														<span class="transition-transform {provinceOpen ? 'rotate-90' : ''}"
															>&rsaquo;</span
														>
													</button>
												{:else}
													<span class="h-6 w-6 shrink-0"></span>
												{/if}
												<input
													id="location-{province.id}"
													type="checkbox"
													checked={analysisFilters.locationIds.includes(province.id)}
													onchange={() => analysisFilters.toggleLocation(province.id)}
													class="h-3.5 w-3.5 shrink-0 cursor-pointer accent-indigo-600"
												/>
												<label
													for="location-{province.id}"
													title={province.label}
													class="min-w-0 flex-1 cursor-pointer truncate text-xs text-slate-600"
												>
													{province.label}
												</label>
											</div>

											{#if provinceOpen}
												<div class="ml-7 border-l border-slate-100 pl-2">
													{#each localities as locality (locality.id)}
														<label
															for="location-{locality.id}"
															class="flex min-h-8 cursor-pointer items-center gap-2 rounded px-1.5 hover:bg-slate-50"
														>
															<input
																id="location-{locality.id}"
																type="checkbox"
																checked={analysisFilters.locationIds.includes(locality.id)}
																onchange={() => analysisFilters.toggleLocation(locality.id)}
																class="h-3.5 w-3.5 shrink-0 cursor-pointer accent-indigo-600"
															/>
															<span
																title={locality.label}
																class="min-w-0 flex-1 truncate text-[11px] text-slate-500"
																>{locality.label}</span
															>
														</label>
													{/each}
												</div>
											{/if}
										</div>
									{/each}

									{#each directLocalities as locality (locality.id)}
										<label
											for="location-{locality.id}"
											class="ml-7 flex min-h-8 cursor-pointer items-center gap-2 rounded px-1.5 hover:bg-slate-50"
										>
											<input
												id="location-{locality.id}"
												type="checkbox"
												checked={analysisFilters.locationIds.includes(locality.id)}
												onchange={() => analysisFilters.toggleLocation(locality.id)}
												class="h-3.5 w-3.5 shrink-0 cursor-pointer accent-indigo-600"
											/>
											<span
												title={locality.label}
												class="min-w-0 flex-1 truncate text-[11px] text-slate-500"
												>{locality.label}</span
											>
										</label>
									{/each}
								</div>
							{/if}
						</div>
					{/each}

					{#if normalizedSearch && regions.filter(visibleRegion).length === 0}
						<p class="py-3 text-xs text-slate-400">No matching locations.</p>
					{/if}
				{/if}
			</div>
		</div>
	{/if}
</section>
