<script>
	let { items = [], selectedYear = null, onselect = () => {} } = $props();
	const maximum = $derived(Math.max(1, ...items.map((item) => item.count)));
	function shade(count) {
		const alpha = 0.14 + (count / maximum) * 0.76;
		return `rgba(79, 70, 229, ${alpha})`;
	}
</script>

{#if items.length}
	<div class="grid grid-cols-4 gap-1.5 sm:grid-cols-6 xl:grid-cols-8">
		{#each items as item (item.period)}
			<button
				type="button"
				onclick={() => onselect(Number(item.period))}
				aria-pressed={selectedYear === Number(item.period)}
				class="rounded-md border px-2 py-1.5 text-left transition focus:outline-none focus:ring-2 focus:ring-indigo-400 {selectedYear === Number(item.period) ? 'border-slate-800 ring-1 ring-slate-800' : 'border-slate-200 hover:border-indigo-300'}"
				style="background:{shade(item.count)}; color:{item.count / maximum > 0.5 ? 'white' : '#334155'}"
			>
				<span class="block text-xs font-semibold">{item.period}</span>
				<span class="mt-0.5 block text-[9px] opacity-80">{item.count.toLocaleString()} events</span>
			</button>
		{/each}
	</div>
{:else}
	<p class="py-8 text-center text-xs text-slate-400">No years match the current scope.</p>
{/if}
