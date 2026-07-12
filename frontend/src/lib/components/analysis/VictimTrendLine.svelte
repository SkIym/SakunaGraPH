<script>
	let { items = [] } = $props();
	const WIDTH = 540;
	const HEIGHT = 220;
	const PAD = { left: 42, right: 16, top: 16, bottom: 34 };
	const series = [
		{ key: 'dead', label: 'Deaths', color: '#e11d48' },
		{ key: 'injured', label: 'Injured', color: '#f59e0b' },
		{ key: 'missing', label: 'Missing', color: '#8b5cf6' }
	];
	const maximum = $derived(Math.max(1, ...items.flatMap((item) => series.map((line) => item[line.key] ?? 0))));
	const x = (index) => PAD.left + (items.length < 2 ? (WIDTH - PAD.left - PAD.right) / 2 : index * (WIDTH - PAD.left - PAD.right) / (items.length - 1));
	const y = (value) => HEIGHT - PAD.bottom - (value / maximum) * (HEIGHT - PAD.top - PAD.bottom);
	function linePath(key) {
		return items.map((item, index) => `${index ? 'L' : 'M'} ${x(index)} ${y(item[key] ?? 0)}`).join(' ');
	}
</script>

{#if items.length}
	<div>
		<div class="mb-3 flex flex-wrap gap-x-4 gap-y-1">
			{#each series as line}
				<span class="flex items-center gap-1.5 text-[10px] text-slate-500"><i class="h-2 w-2 rounded-full" style="background:{line.color}"></i>{line.label}</span>
			{/each}
		</div>
		<svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} class="h-56 w-full" role="img" aria-label="Annual casualty trend">
			{#each [0, 0.5, 1] as tick}
				<line x1={PAD.left} x2={WIDTH - PAD.right} y1={y(maximum * tick)} y2={y(maximum * tick)} stroke="#e2e8f0" stroke-dasharray="3 3"></line>
				<text x={PAD.left - 7} y={y(maximum * tick) + 4} text-anchor="end" class="fill-slate-400 text-[9px]">{Math.round(maximum * tick).toLocaleString()}</text>
			{/each}
			{#each series as line}
				<path d={linePath(line.key)} fill="none" stroke={line.color} stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"></path>
				{#each items as item, index (`${line.key}-${item.year}`)}
					<circle cx={x(index)} cy={y(item[line.key] ?? 0)} r="3" fill="white" stroke={line.color} stroke-width="2"><title>{item.year}: {line.label} {item[line.key].toLocaleString()}</title></circle>
				{/each}
			{/each}
			{#each items as item, index (item.year)}
				<text x={x(index)} y={HEIGHT - 12} text-anchor="middle" class="fill-slate-400 text-[9px]">{item.year}</text>
			{/each}
		</svg>
	</div>
{:else}
	<p class="flex h-56 items-center justify-center text-xs text-slate-400">No casualty trend data in this scope.</p>
{/if}
