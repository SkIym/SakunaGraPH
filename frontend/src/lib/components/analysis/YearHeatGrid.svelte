<script>
	let { items = [], selectedYear = null, onselect = () => {} } = $props();
	const maximum = $derived(Math.max(1, ...items.map((item) => item.count)));
	function shade(count) {
		const alpha = 0.08 + (count / maximum) * 0.38;
		return `rgba(0, 56, 168, ${alpha})`;
	}
</script>

{#if items.length}
	<div class="grid grid-cols-4 gap-1.5 sm:grid-cols-6 xl:grid-cols-8">
		{#each items as item (item.period)}
			<button
				type="button"
				onclick={() => onselect(Number(item.period))}
				aria-pressed={selectedYear === Number(item.period)}
				class="heat-cell min-h-11 border px-2 py-1.5 text-left transition focus:outline-none focus:ring-2 focus:ring-[var(--color-focus)] {selectedYear ===
				Number(item.period)
					? 'is-selected border-[var(--color-brand)] ring-1 ring-[var(--color-brand)]'
					: 'border-[var(--color-border)] hover:border-[var(--color-brand)]'}"
				style="background:{selectedYear === Number(item.period)
					? 'var(--color-accent)'
					: shade(item.count)}; color:var(--color-text)"
			>
				<span class="block text-xs font-semibold">{item.period}</span>
				<span class="mt-0.5 block text-[9px]">{item.count.toLocaleString()} events</span>
			</button>
		{/each}
	</div>
{:else}
	<p class="py-8 text-center text-xs text-slate-400">No years match the current scope.</p>
{/if}

<style>
	.heat-cell {
		border-radius: 0.25rem;
	}

	.heat-cell.is-selected {
		box-shadow: inset 3px 0 0 var(--color-brand);
	}
</style>
