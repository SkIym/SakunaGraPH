<script>
	let { items = [] } = $props();
	const topItems = $derived(items.slice(0, 8));
	const maximum = $derived(Math.max(1, ...topItems.map((item) => item.dead)));
</script>

{#if topItems.length}
	<div class="space-y-3">
		{#each topItems as item (item.id)}
			<div>
				<div class="mb-1 flex justify-between gap-3 text-[11px]">
					<span class="truncate text-slate-600" title={item.label}>{item.label}</span><span
						class="font-semibold tabular-nums text-slate-700">{item.dead.toLocaleString()}</span
					>
				</div>
				<div class="h-2 overflow-hidden rounded-full bg-slate-100">
					<div
						class="h-full rounded-full bg-rose-500"
						style="width:{Math.max(3, (item.dead / maximum) * 100)}%"
					></div>
				</div>
			</div>
		{/each}
	</div>
{:else}
	<p class="flex h-56 items-center justify-center text-xs text-slate-400">
		No casualty rankings in this scope.
	</p>
{/if}
