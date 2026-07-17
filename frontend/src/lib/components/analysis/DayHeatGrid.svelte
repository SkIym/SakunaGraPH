<script>
	let { items = [], year = null, month = null, selectedDay = null, onselect = () => {} } = $props();
	const counts = $derived(Object.fromEntries(items.map((item) => [Number(item.period.slice(8, 10)), item.count])));
	const days = $derived(year && month ? new Date(year, month, 0).getDate() : 0);
	const maximum = $derived(Math.max(1, ...Object.values(counts)));
	function shade(count) {
		return count ? `rgba(20, 184, 166, ${0.16 + (count / maximum) * 0.72})` : '#f8fafc';
	}
</script>

{#if days}
	<div class="grid grid-cols-10 gap-1 sm:grid-cols-12 xl:grid-cols-[repeat(16,minmax(0,1fr))]">
		{#each Array.from({ length: days }, (_, index) => index + 1) as day}
			{@const count = counts[day] ?? 0}
			<button
				type="button"
				onclick={() => onselect(day)}
				aria-pressed={selectedDay === day}
				class="aspect-square rounded border text-center text-[9px] font-semibold transition focus:outline-none focus:ring-2 focus:ring-teal-400 {selectedDay === day ? 'border-slate-800 ring-1 ring-slate-800' : 'border-slate-200 hover:border-teal-300'}"
				style="background:{shade(count)}; color:{count / maximum > 0.52 ? 'white' : '#334155'}"
				title={`${day}: ${count.toLocaleString()} events`}
			>
				{day}
			</button>
		{/each}
	</div>
{:else}
	<p class="py-8 text-center text-xs text-slate-400">Choose a month to inspect its days.</p>
{/if}
