<script>
	let { items = [], selectedMonth = null, onselect = () => {} } = $props();
	const MONTHS = [
		'Jan',
		'Feb',
		'Mar',
		'Apr',
		'May',
		'Jun',
		'Jul',
		'Aug',
		'Sep',
		'Oct',
		'Nov',
		'Dec',
	];
	const counts = $derived(
		Object.fromEntries(items.map((item) => [Number(item.period.slice(5, 7)), item.count])),
	);
	const maximum = $derived(Math.max(1, ...Object.values(counts)));
	function shade(count) {
		return count
			? `rgba(0, 56, 168, ${0.08 + (count / maximum) * 0.38})`
			: 'var(--color-surface-subtle)';
	}
</script>

<div class="grid grid-cols-4 gap-1.5 sm:grid-cols-6 xl:grid-cols-12">
	{#each MONTHS as label, index}
		{@const month = index + 1}
		{@const count = counts[month] ?? 0}
		<button
			type="button"
			onclick={() => onselect(month)}
			aria-pressed={selectedMonth === month}
			class="heat-cell min-h-11 border px-2 py-1.5 text-left transition focus:outline-none focus:ring-2 focus:ring-[var(--color-focus)] {selectedMonth ===
			month
				? 'is-selected border-[var(--color-brand)] ring-1 ring-[var(--color-brand)]'
				: 'border-[var(--color-border)] hover:border-[var(--color-brand)]'}"
			style="background:{selectedMonth === month
				? 'var(--color-accent)'
				: shade(count)}; color:var(--color-text)"
		>
			<span class="block text-[11px] font-semibold">{label}</span>
			<span class="mt-0.5 block text-[9px]">{count.toLocaleString()}</span>
		</button>
	{/each}
</div>

<style>
	.heat-cell {
		border-radius: 0.25rem;
	}

	.heat-cell.is-selected {
		box-shadow: inset 3px 0 0 var(--color-brand);
	}
</style>
