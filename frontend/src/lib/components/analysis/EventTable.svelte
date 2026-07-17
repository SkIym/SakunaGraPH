<script>
	let {
		items = [],
		loading = false,
		columns = [],
		visibleColumns = new Set(),
		sortBy = 'startDate',
		sortDir = 'desc',
		onSort = () => {},
		onSelect = () => {}
	} = $props();

	const visible = $derived(columns.filter((column) => visibleColumns.has(column.id)));
	const numberFormat = new Intl.NumberFormat('en-PH', { maximumFractionDigits: 2 });
	const dateFormat = new Intl.DateTimeFormat('en-PH', {
		year: 'numeric',
		month: 'short',
		day: 'numeric'
	});

	function formatDate(value) {
		if (!value) return '—';
		const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
		return Number.isNaN(parsed.valueOf()) ? value : dateFormat.format(parsed);
	}

	function unitLabel(unit) {
		return {
			PHP_millions: 'PHP millions',
			USD_thousands: 'USD thousands',
			PHP: 'PHP',
			USD: 'USD'
		}[unit] ?? unit?.replaceAll('_', ' ') ?? '';
	}

	function damageValues(impact) {
		if (impact?.damageByUnit?.length) return impact.damageByUnit;
		if (impact?.damageUnit && impact?.damageAmount !== null && impact?.damageAmount !== undefined) {
			return [{ amount: impact.damageAmount, unit: impact.damageUnit }];
		}
		return [];
	}

	function impactValues(impact) {
		const values = [];
		if (impact?.dead) values.push(`${numberFormat.format(impact.dead)} dead`);
		if (impact?.injured) values.push(`${numberFormat.format(impact.injured)} injured`);
		if (impact?.missing) values.push(`${numberFormat.format(impact.missing)} missing`);
		if (impact?.affectedPersons) values.push(`${numberFormat.format(impact.affectedPersons)} people affected`);
		if (impact?.affectedFamilies) values.push(`${numberFormat.format(impact.affectedFamilies)} families affected`);
		for (const damage of damageValues(impact)) {
			values.push(`${numberFormat.format(damage.amount)} ${unitLabel(damage.unit)} damage`);
		}
		return values;
	}

	function ariaSort(column) {
		if (!column.sortable || sortBy !== column.id) return 'none';
		return sortDir === 'asc' ? 'ascending' : 'descending';
	}
</script>

<div class="overflow-x-auto">
	<table class="w-full min-w-[980px] border-collapse text-left text-xs">
		<thead>
			<tr class="border-b border-slate-200 bg-slate-50/80">
				{#each visible as column (column.id)}
					<th
					scope="col"
					aria-sort={ariaSort(column)}
					class="whitespace-nowrap px-3 py-3 text-[10px] font-semibold uppercase text-slate-500 first:pl-5 last:pr-5"
					style="letter-spacing:0.08em;"
					>
						{#if column.sortable}
							<button
								type="button"
								onclick={() => onSort(column.id)}
								class="flex items-center gap-1.5 transition hover:text-slate-800"
								aria-label="Sort by {column.label}"
							>
								{column.label}
								<span class="text-[10px] {sortBy === column.id ? 'text-indigo-600' : 'text-slate-300'}" aria-hidden="true">
									{sortBy === column.id ? (sortDir === 'asc' ? '↑' : '↓') : '↕'}
								</span>
							</button>
						{:else}
							{column.label}
						{/if}
					</th>
				{/each}
			</tr>
		</thead>
		<tbody class="divide-y divide-slate-100 bg-white">
			{#if loading}
				{#each Array(8) as _, row}
					<tr aria-hidden="true" class={row % 2 ? 'bg-slate-50/30' : ''}>
						{#each visible as column, index (column.id)}
							<td class="px-3 py-4 first:pl-5 last:pr-5">
								<div class="h-3 animate-pulse rounded bg-slate-100" style="width:{index === 0 ? 150 : 62 + ((row + index) % 4) * 18}px"></div>
							</td>
						{/each}
					</tr>
				{/each}
			{:else}
				{#each items as item, row (item.event)}
					<tr
						role="button"
						tabindex="0"
						aria-label="View details for {item.eventName || 'unnamed event'}"
						onclick={() => onSelect(item)}
						onkeydown={(keyboardEvent) => {
							if (keyboardEvent.key === 'Enter' || keyboardEvent.key === ' ') {
								keyboardEvent.preventDefault();
								onSelect(item);
							}
						}}
						class="cursor-pointer align-top transition hover:bg-indigo-50/50 focus:bg-indigo-50/50 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-indigo-300 {row % 2 ? 'bg-slate-50/20' : ''}"
					>
						{#each visible as column (column.id)}
							<td class="px-3 py-3.5 text-slate-600 first:pl-5 last:pr-5">
								{#if column.id === 'eventName'}
									<div class="max-w-64">
										<p class="line-clamp-2 font-medium leading-5 text-slate-800" title={item.eventName}>{item.eventName || '(unnamed event)'}</p>
										<p class="mt-1 truncate font-mono text-[9px] text-slate-400" title={item.event}>{item.event}</p>
										{#if item.alternates?.length}
											<span class="mt-1.5 inline-flex rounded bg-violet-50 px-1.5 py-0.5 text-[9px] font-semibold text-violet-600">
												{item.alternates.length} alternate{item.alternates.length === 1 ? '' : 's'}
											</span>
										{/if}
									</div>
								{:else if column.id === 'eventType'}
									<span class="inline-flex rounded px-2 py-1 text-[10px] font-semibold {item.eventType === 'MajorEvent' ? 'bg-indigo-50 text-indigo-700' : 'bg-amber-50 text-amber-700'}">
										{item.eventType === 'MajorEvent' ? 'Major event' : 'Incident'}
									</span>
								{:else if column.id === 'startDate'}
									<span class="whitespace-nowrap tabular-nums">{formatDate(item.startDate)}</span>
								{:else if column.id === 'endDate'}
									<span class="whitespace-nowrap tabular-nums">{formatDate(item.endDate)}</span>
								{:else if column.id === 'locations'}
									{#if item.locations?.length}
										<div class="flex max-w-56 flex-wrap gap-1">
											{#each item.locations.slice(0, 2) as location (location.id)}
												<span class="rounded border border-slate-200 bg-white px-1.5 py-0.5 text-[10px]" title={location.id}>{location.label}</span>
											{/each}
											{#if item.locations.length > 2}
												<span class="px-1 py-0.5 text-[10px] font-medium text-slate-400" title={item.locations.slice(2).map((value) => value.label).join(', ')}>+{item.locations.length - 2} more</span>
											{/if}
										</div>
									{:else}<span class="text-slate-300">—</span>{/if}
								{:else if column.id === 'disasterTypes'}
									{#if item.disasterTypes?.length}
										<div class="flex max-w-52 flex-wrap gap-1">
											{#each item.disasterTypes as disasterType (disasterType.id)}
												<span class="rounded bg-sky-50 px-1.5 py-0.5 text-[10px] text-sky-700" title={disasterType.id}>{disasterType.label}</span>
											{/each}
										</div>
									{:else}<span class="text-slate-300">—</span>{/if}
								{:else if column.id === 'source'}
									{#if item.source}
										<span class="inline-flex rounded bg-slate-100 px-2 py-1 text-[10px] font-semibold uppercase text-slate-600">{item.source}</span>
									{:else}<span class="text-slate-300">—</span>{/if}
								{:else if column.id === 'impact'}
									{@const values = impactValues(item.impact)}
									{#if values.length}
										<ul class="min-w-44 space-y-0.5 text-[10px] leading-4">
											{#each values as value}
												<li>{value}</li>
											{/each}
										</ul>
									{:else}<span class="text-slate-300">—</span>{/if}
								{/if}
							</td>
						{/each}
					</tr>
				{/each}
			{/if}
		</tbody>
	</table>
</div>
