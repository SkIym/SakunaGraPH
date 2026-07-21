<script>
	let { periods = [], value = '', onchange = () => {} } = $props();
	const selectedIndex = $derived(Math.max(0, periods.indexOf(value)));
	function update(event) {
		const period = periods[Number(event.currentTarget.value)];
		if (period) onchange(period);
	}
</script>

{#if periods.length > 1}
	<div class="flex items-center gap-3 rounded-md bg-slate-50 px-3 py-2">
		<span
			class="shrink-0 text-[10px] font-semibold uppercase text-slate-400"
			style="letter-spacing:0.08em;">Window</span
		>
		<input
			type="range"
			min="0"
			max={periods.length - 1}
			value={selectedIndex}
			oninput={update}
			class="h-1 flex-1 accent-indigo-600"
			aria-label="Timeline window end"
		/>
		<span class="w-14 text-right text-[10px] font-medium text-slate-500"
			>{periods[selectedIndex]}</span
		>
	</div>
{/if}
