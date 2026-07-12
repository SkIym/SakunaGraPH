<script>
	let { bins = [] } = $props();
	let selectedUnit = $state('');
	const units = $derived([...new Set(bins.map((item) => item.unit))]);
	const activeUnit = $derived(units.includes(selectedUnit) ? selectedUnit : (units[0] ?? ''));
	const activeBins = $derived(bins.filter((item) => item.unit === activeUnit));
	const maximum = $derived(Math.max(1, ...activeBins.map((item) => item.count)));
	$effect(() => {
		if (!units.includes(selectedUnit)) selectedUnit = units[0] ?? '';
	});

	function formatAmount(value) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 1, notation: 'compact' }).format(value ?? 0);
	}
</script>

{#if units.length}
	<div>
		{#if units.length > 1}
			<label class="mb-3 block text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.08em;">Unit
				<select bind:value={selectedUnit} class="ml-2 rounded border border-slate-200 bg-white px-2 py-1 text-[11px] normal-case text-slate-600 outline-none focus:border-indigo-400">
					{#each units as unit}<option value={unit}>{unit}</option>{/each}
				</select>
			</label>
		{/if}
		<div class="flex h-48 items-end gap-1.5 border-b border-l border-slate-200 px-2 pb-1 pt-3">
			{#each activeBins as bin, index (`${bin.lowerBound}-${index}`)}
				<div class="group relative flex min-w-0 flex-1 items-end" style="height:100%;">
					<div class="w-full rounded-t bg-orange-400 transition group-hover:bg-orange-500" style="height:{Math.max(2, bin.count / maximum * 100)}%"><title>{formatAmount(bin.lowerBound)}–{formatAmount(bin.upperBound)} {activeUnit}: {bin.count} events</title></div>
				</div>
			{/each}
		</div>
		<div class="mt-2 flex justify-between text-[9px] text-slate-400"><span>{formatAmount(activeBins[0]?.lowerBound)} {activeUnit}</span><span>{formatAmount(activeBins.at(-1)?.upperBound)} {activeUnit}</span></div>
	</div>
{:else}
	<p class="flex h-48 items-center justify-center text-xs text-slate-400">No reported damage amounts in this scope.</p>
{/if}
