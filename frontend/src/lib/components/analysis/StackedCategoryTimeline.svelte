<script>
	import TimelineBrush from './TimelineBrush.svelte';

	let { items = [], onselect = () => {} } = $props();
	let windowEnd = $state('');
	const COLORS = ['#6366f1', '#0ea5e9', '#14b8a6', '#f59e0b', '#f97316', '#ec4899', '#64748b'];
	const periods = $derived(items.map((item) => item.period));
	const activeEnd = $derived(periods.includes(windowEnd) ? windowEnd : (periods.at(-1) ?? ''));
	const endIndex = $derived(Math.max(0, periods.indexOf(activeEnd)));
	const visibleItems = $derived(items.slice(Math.max(0, endIndex - 15), endIndex + 1));
	const categoryIds = $derived([...new Set(items.flatMap((item) => item.categories.map((category) => category.id)))]);
	const colorFor = (id) => COLORS[Math.max(0, categoryIds.indexOf(id)) % COLORS.length];
	const maximum = $derived(Math.max(1, ...visibleItems.map((item) => item.categories.reduce((sum, category) => sum + category.count, 0))));
	function formatPeriod(period) {
		if (!period) return '';
		if (/^\d{4}-\d{2}$/.test(period)) return new Intl.DateTimeFormat('en-PH', { month: 'short', year: '2-digit' }).format(new Date(`${period}-01T00:00:00`));
		return new Intl.DateTimeFormat('en-PH', { month: 'short' }).format(new Date(`2024-${period}-01T00:00:00`));
	}
</script>

{#if items.length}
	<div class="space-y-3">
		<TimelineBrush periods={periods} value={activeEnd} onchange={(period) => (windowEnd = period)} />
		<div class="flex h-56 items-end gap-1.5 border-b border-slate-200 px-1 pb-5">
			{#each visibleItems as item (item.period)}
				{@const total = item.categories.reduce((sum, category) => sum + category.count, 0)}
				<button
					type="button"
					onclick={() => onselect(item.period)}
					class="group relative flex h-full min-w-0 flex-1 flex-col-reverse justify-start overflow-hidden rounded-t-sm bg-slate-100 text-left transition hover:ring-2 hover:ring-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-500"
					title={`${item.period}: ${total.toLocaleString()} category assignments`}
				>
					{#each item.categories as category (category.id)}
						<span class="w-full" style="height:{category.count / maximum * 100}%; background:{colorFor(category.id)}"></span>
					{/each}
					<span class="absolute bottom-[-19px] left-1/2 -translate-x-1/2 whitespace-nowrap text-[8px] font-medium text-slate-400">{formatPeriod(item.period)}</span>
				</button>
			{/each}
		</div>
		<div class="flex flex-wrap gap-x-3 gap-y-1.5">
			{#each categoryIds as categoryId}
				{@const category = items.flatMap((item) => item.categories).find((item) => item.id === categoryId)}
				{#if category}<span class="flex items-center gap-1.5 text-[10px] text-slate-500"><i class="h-2 w-2 rounded-full" style="background:{colorFor(categoryId)}"></i>{category.label}</span>{/if}
			{/each}
		</div>
	</div>
{:else}
	<p class="flex h-56 items-center justify-center text-xs text-slate-400">No category timeline data in this scope.</p>
{/if}
