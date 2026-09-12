<script>
	let { items = [], year = null, month = null, selectedDay = null, onselect = () => {} } = $props();
	const counts = $derived(
		Object.fromEntries(items.map((item) => [Number(item.period.slice(8, 10)), item.count])),
	);
	const days = $derived(year && month ? new Date(year, month, 0).getDate() : 0);
	const maximum = $derived(Math.max(1, ...Object.values(counts)));
	function shade(count) {
		return count
			? `rgba(0, 56, 168, ${0.08 + (count / maximum) * 0.38})`
			: 'var(--color-surface-subtle)';
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
				class="heat-cell min-h-11 border text-center text-[9px] font-semibold transition focus:outline-none focus:ring-2 focus:ring-[var(--color-focus)] {selectedDay ===
				day
					? 'is-selected border-[var(--color-brand)] ring-1 ring-[var(--color-brand)]'
					: 'border-[var(--color-border)] hover:border-[var(--color-brand)]'}"
				style="background:{selectedDay === day
					? 'var(--color-accent)'
					: shade(count)}; color:var(--color-text)"
				title={`${day}: ${count.toLocaleString()} events`}
			>
				{day}
			</button>
		{/each}
	</div>
{:else}
	<p class="py-8 text-center text-xs text-slate-400">Choose a month to inspect its days.</p>
{/if}

<style>
	.heat-cell {
		border-radius: 0.2rem;
	}

	.heat-cell.is-selected {
		box-shadow: inset 3px 0 0 var(--color-brand);
	}
</style>
