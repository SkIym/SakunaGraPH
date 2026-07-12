<script>
	let { summary = null } = $props();

	function formatNumber(value) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 0 }).format(value ?? 0);
	}

	function formatAmount(amount) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 1 }).format(amount ?? 0);
	}

	const cards = $derived([
		{ label: 'Event records', value: summary?.record_count ?? 0, tone: 'indigo' },
		{ label: 'Reported deaths', value: summary?.dead ?? 0, tone: 'rose' },
		{ label: 'Injured', value: summary?.injured ?? 0, tone: 'amber' },
		{ label: 'Missing', value: summary?.missing ?? 0, tone: 'violet' },
		{ label: 'Affected families', value: summary?.affectedFamilies ?? 0, tone: 'sky' },
		{ label: 'Affected persons', value: summary?.affectedPersons ?? 0, tone: 'teal' }
	]);
</script>

<div class="grid gap-px overflow-hidden rounded-xl border border-slate-200 bg-slate-200 sm:grid-cols-2 xl:grid-cols-3">
	{#each cards as card}
		<div class="bg-white px-4 py-4">
			<p class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.08em;">{card.label}</p>
			<p class="mt-2 text-2xl font-semibold tabular-nums text-slate-800">{formatNumber(card.value)}</p>
		</div>
	{/each}
	<div class="bg-white px-4 py-4 sm:col-span-2 xl:col-span-3">
		<div class="flex flex-wrap items-baseline justify-between gap-x-5 gap-y-2">
			<p class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.08em;">Reported damage</p>
			{#if summary?.damage?.length}
				<div class="flex flex-wrap gap-x-5 gap-y-1">
					{#each summary.damage as damage (damage.unit)}
						<p class="text-sm font-semibold tabular-nums text-slate-700">{formatAmount(damage.amount)} <span class="text-[10px] font-medium text-slate-400">{damage.unit}</span></p>
					{/each}
				</div>
			{:else}
				<p class="text-xs text-slate-400">No reported damage amounts</p>
			{/if}
		</div>
	</div>
</div>
