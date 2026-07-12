<script>
	let { visible = false, x = 0, y = 0, title = '', lines = [] } = $props();
</script>

{#if visible}
	<div
		role="status"
		class="pointer-events-none absolute z-10 min-w-28 rounded-md bg-slate-800 px-2.5 py-2 text-[10px] text-white shadow-lg"
		style="left:{x}px; top:{y}px; transform:translate(-50%, -110%);"
	>
		<p class="font-semibold">{title}</p>
		{#each lines as line}
			<p class="mt-0.5 text-slate-300">{line}</p>
		{/each}
	</div>
{/if}
