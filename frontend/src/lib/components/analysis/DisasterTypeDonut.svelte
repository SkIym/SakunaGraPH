<script>
	import ChartTooltip from './ChartTooltip.svelte';

	let { items = [] } = $props();
	let hovered = $state(null);
	const COLORS = ['#6366f1', '#0ea5e9', '#14b8a6', '#f59e0b', '#f97316', '#ef4444', '#a855f7', '#64748b'];
	const total = $derived(items.reduce((sum, item) => sum + item.count, 0));
	const slices = $derived.by(() => {
		let cursor = -Math.PI / 2;
		return items.map((item, index) => {
			const angle = total ? (item.count / total) * Math.PI * 2 : 0;
			const slice = { ...item, start: cursor, end: cursor + angle, color: COLORS[index % COLORS.length] };
			cursor += angle;
			return slice;
		});
	});

	function polar(angle, radius) {
		return [120 + Math.cos(angle) * radius, 120 + Math.sin(angle) * radius];
	}

	function arc(slice) {
		if (slice.end - slice.start >= Math.PI * 2 - 0.001) return 'M 120 22 A 98 98 0 1 1 119.99 22 Z';
		const [x1, y1] = polar(slice.start, 98);
		const [x2, y2] = polar(slice.end, 98);
		return `M 120 120 L ${x1} ${y1} A 98 98 0 ${slice.end - slice.start > Math.PI ? 1 : 0} 1 ${x2} ${y2} Z`;
	}
</script>

<div class="relative h-full">
	{#if items.length}
		<div class="flex flex-col gap-4 sm:flex-row sm:items-center">
			<svg viewBox="0 0 240 240" class="mx-auto h-48 w-48 shrink-0" aria-label="Event counts by disaster category" role="img">
				{#each slices as slice (slice.id)}
					<path d={arc(slice)} fill={slice.color} stroke="white" stroke-width="2" class="cursor-pointer transition-opacity" opacity={hovered && hovered.id !== slice.id ? 0.35 : 1} role="img" aria-label={`${slice.label}: ${slice.count.toLocaleString()} events`} onmouseenter={() => (hovered = slice)} onmouseleave={() => (hovered = null)}>
						<title>{slice.label}: {slice.count.toLocaleString()} events</title>
					</path>
				{/each}
				<circle cx="120" cy="120" r="55" fill="white"></circle>
				<text x="120" y="114" text-anchor="middle" class="fill-slate-400 text-[10px] font-semibold uppercase">Events</text>
				<text x="120" y="136" text-anchor="middle" class="fill-slate-800 text-xl font-semibold">{total.toLocaleString()}</text>
			</svg>
			<div class="min-w-0 space-y-1.5 sm:max-h-48 sm:overflow-y-auto sm:pr-1">
				{#each slices as slice (slice.id)}
					<div class="flex items-center justify-between gap-3 text-[11px]">
						<span class="flex min-w-0 items-center gap-1.5"><i class="h-2 w-2 shrink-0 rounded-full" style="background:{slice.color}"></i><span class="truncate">{slice.label}</span></span>
						<span class="font-semibold tabular-nums text-slate-600">{slice.count.toLocaleString()}</span>
					</div>
				{/each}
			</div>
		</div>
		<ChartTooltip visible={Boolean(hovered)} x={120} y={115} title={hovered?.label ?? ''} lines={[`${hovered?.count?.toLocaleString() ?? 0} events`]} />
	{:else}
		<p class="flex h-48 items-center justify-center text-xs text-slate-400">No disaster-type records in this scope.</p>
	{/if}
</div>
