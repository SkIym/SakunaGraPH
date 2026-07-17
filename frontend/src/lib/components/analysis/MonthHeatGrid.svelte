<script>
	let { items = [], selectedMonth = null, onselect = () => {} } = $props();
	const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
	const counts = $derived(Object.fromEntries(items.map((item) => [Number(item.period.slice(5, 7)), item.count])));
	const maximum = $derived(Math.max(1, ...Object.values(counts)));
	function shade(count) {
		return count ? `rgba(14, 165, 233, ${0.16 + (count / maximum) * 0.72})` : '#f8fafc';
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
			class="rounded-md border px-1.5 py-1.5 text-left transition focus:outline-none focus:ring-2 focus:ring-sky-400 {selectedMonth === month ? 'border-slate-800 ring-1 ring-slate-800' : 'border-slate-200 hover:border-sky-300'}"
			style="background:{shade(count)}; color:{count / maximum > 0.52 ? 'white' : '#334155'}"
		>
			<span class="block text-[11px] font-semibold">{label}</span>
			<span class="mt-0.5 block text-[9px] opacity-80">{count.toLocaleString()}</span>
		</button>
	{/each}
</div>
