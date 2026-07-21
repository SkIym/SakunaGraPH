<script>
	import OntologyLoading from './OntologyLoading.svelte';

	let {
		active = false,
		loading = false,
		selectedNode = null,
		colors = {},
		svgElement = $bindable(null),
	} = $props();

	const LEGEND = Object.freeze([
		['natural', 'Natural'],
		['biological', '· Biological'],
		['climatological', '· Climatological'],
		['extraterrestrial', '· Extraterrestrial'],
		['geophysical', '· Geophysical'],
		['hydrological', '· Hydrological'],
		['meteorological', '· Meteorological'],
		['tech', 'Technological'],
		['armedconflict', '· Armed Conflict'],
		['industrial', '· Industrial'],
		['miscellaneous', '· Miscellaneous'],
		['transport', '· Transport'],
	]);
</script>

{#if active}
	<div class="absolute inset-0">
		{#if loading}<OntologyLoading label="Building taxonomy tree…" />{/if}

		<svg
			bind:this={svgElement}
			class="h-full w-full"
			style="cursor:default;"
			aria-label="Interactive disaster taxonomy graph"
		></svg>

		<p class="pointer-events-none absolute top-16 right-5 text-[11px] font-medium text-slate-500">
			Scroll to zoom · Drag canvas · Click node
		</p>

		<div
			class="absolute bottom-6 left-6 rounded-2xl bg-white/85 px-4 py-4 shadow-2xl"
			style="backdrop-filter:blur(12px); max-width:300px; min-width: 200px;"
		>
			<p class="mb-3 text-[10px] font-bold tracking-widest text-slate-400 uppercase">Category</p>
			<div class="flex flex-col gap-1.5">
				{#each LEGEND as [key, label]}
					<div class="flex items-center gap-2">
						<div
							class="h-2.5 w-2.5 flex-shrink-0 rounded-full"
							style="background:{colors[key]};"
						></div>
						<span class="text-[12px] text-slate-600">{label}</span>
					</div>
				{/each}
			</div>
		</div>

		{#if selectedNode}
			<div
				class="absolute right-6 bottom-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
				style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
			>
				<div class="px-8 py-7">
					<p
						class="mb-2 text-[13px] font-bold tracking-widest uppercase"
						style="color:{colors[selectedNode.group]};"
					>
						{selectedNode.group}
					</p>
					<h2
						class="leading-tight font-black text-slate-800"
						style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;"
					>
						{selectedNode.label}
					</h2>
					<div
						class="mt-3 h-0.5 rounded-full"
						style="width:40px; background:{colors[selectedNode.group]};"
					></div>
					<p class="mt-4 text-[15px] leading-relaxed text-slate-500">{selectedNode.definition}</p>
					<p class="mt-5 text-[12px] tracking-widest text-slate-300 uppercase">
						Click node or canvas to deselect
					</p>
				</div>
			</div>
		{/if}
	</div>
{/if}
