<script>
	let { items = [] } = $props();
	let selectedUnit = $state('');
	let affectedMeasure = $state('affectedPersons');
	const WIDTH = 520;
	const HEIGHT = 230;
	const PAD = { left: 44, right: 18, top: 18, bottom: 36 };
	const units = $derived([...new Set(items.map((item) => item.unit))]);
	const activeUnit = $derived(units.includes(selectedUnit) ? selectedUnit : (units[0] ?? ''));
	const points = $derived(items.filter((item) => item.unit === activeUnit));
	const maxDamage = $derived(Math.max(1, ...points.map((item) => item.damage)));
	const maxAffected = $derived(Math.max(1, ...points.map((item) => item[affectedMeasure] ?? 0)));
	$effect(() => {
		if (!units.includes(selectedUnit)) selectedUnit = units[0] ?? '';
	});
	const x = (value) => PAD.left + (value / maxDamage) * (WIDTH - PAD.left - PAD.right);
	const y = (value) =>
		HEIGHT - PAD.bottom - (value / maxAffected) * (HEIGHT - PAD.top - PAD.bottom);

	function compact(value) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 1, notation: 'compact' }).format(
			value ?? 0,
		);
	}
</script>

{#if units.length}
	<div>
		<div class="mb-3 flex flex-wrap gap-2">
			{#if units.length > 1}
				<select
					bind:value={selectedUnit}
					aria-label="Damage unit"
					class="rounded border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-600 outline-none focus:border-indigo-400"
					>{#each units as unit}<option value={unit}>{unit}</option>{/each}</select
				>
			{/if}
			<select
				bind:value={affectedMeasure}
				aria-label="Affected population measure"
				class="rounded border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-600 outline-none focus:border-indigo-400"
				><option value="affectedPersons">Affected persons</option><option value="affectedFamilies"
					>Affected families</option
				></select
			>
		</div>
		<svg
			viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
			class="h-56 w-full"
			role="img"
			aria-label="Reported damage compared with affected population"
		>
			<line
				x1={PAD.left}
				x2={WIDTH - PAD.right}
				y1={HEIGHT - PAD.bottom}
				y2={HEIGHT - PAD.bottom}
				stroke="#cbd5e1"
			></line>
			<line x1={PAD.left} x2={PAD.left} y1={PAD.top} y2={HEIGHT - PAD.bottom} stroke="#cbd5e1"
			></line>
			<text x={PAD.left} y={HEIGHT - 12} class="fill-slate-400 text-[9px]">0</text>
			<text
				x={WIDTH - PAD.right}
				y={HEIGHT - 12}
				text-anchor="end"
				class="fill-slate-400 text-[9px]">{compact(maxDamage)} {activeUnit}</text
			>
			<text x={PAD.left - 6} y={PAD.top + 3} text-anchor="end" class="fill-slate-400 text-[9px]"
				>{compact(maxAffected)}</text
			>
			{#each points as point (point.event)}
				<circle
					cx={x(point.damage)}
					cy={y(point[affectedMeasure] ?? 0)}
					r="4"
					fill="#6366f1"
					fill-opacity="0.65"
					stroke="white"
					stroke-width="1"
					><title
						>{point.eventName}: {compact(point.damage)}
						{point.unit}, {compact(point[affectedMeasure])}
						{affectedMeasure === 'affectedPersons' ? 'persons' : 'families'}</title
					></circle
				>
			{/each}
		</svg>
	</div>
{:else}
	<p class="flex h-56 items-center justify-center text-xs text-slate-400">
		No damage and affected-population pairs in this scope.
	</p>
{/if}
