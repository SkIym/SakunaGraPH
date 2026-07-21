<script>
	import OntologyLoading from './OntologyLoading.svelte';

	let {
		active = false,
		loading = false,
		selectedNode = null,
		groupColors = {},
		groupLabels = {},
		svgElement = $bindable(null),
	} = $props();
</script>

<div class="absolute inset-0" class:hidden={!active}>
	{#if loading}<OntologyLoading label="Rendering ontology graph…" />{/if}

	<svg
		bind:this={svgElement}
		class="h-full w-full"
		style="cursor:default;"
		aria-label="Interactive core ontology class graph"
	></svg>

	<p class="pointer-events-none absolute top-16 right-5 text-[11px] font-medium text-slate-500">
		Hover · Click · Drag
	</p>

	<div
		class="absolute bottom-6 left-6 rounded-2xl bg-white/85 px-5 py-4 shadow-2xl"
		style="backdrop-filter:blur(12px);"
	>
		<p class="mb-3 text-[10px] font-bold tracking-widest text-slate-400 uppercase">Legend</p>
		<div class="flex flex-col gap-2">
			{#each Object.entries(groupLabels) as [group, label]}
				<div class="flex items-center gap-2.5">
					<div
						class="h-3.5 w-3.5 flex-shrink-0 rounded-full"
						style="background:{groupColors[group]}33; border:2.5px solid {groupColors[group]};"
					></div>
					<span class="text-xs font-medium text-slate-600">{label}</span>
				</div>
			{/each}
			<div class="mt-2 flex flex-col gap-1.5 border-t border-slate-100 pt-2">
				<div class="flex items-center gap-2.5">
					<div class="h-px w-7 flex-shrink-0 bg-slate-300"></div>
					<span class="text-xs text-slate-400">subClassOf</span>
				</div>
				<div class="flex items-center gap-2.5">
					<svg width="28" height="7" style="flex-shrink:0;" aria-hidden="true">
						<line
							x1="0"
							y1="3.5"
							x2="28"
							y2="3.5"
							stroke="#93c5fd"
							stroke-width="1.8"
							stroke-dasharray="5 3"
						></line>
					</svg>
					<span class="text-xs text-slate-400">object property</span>
				</div>
			</div>
		</div>
		<p class="mt-3 text-[10px] text-slate-300">Scroll · Drag canvas · Drag nodes</p>
	</div>

	{#if selectedNode}
		<div
			class="absolute right-6 bottom-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
			style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
		>
			<div class="px-8 py-7">
				<p
					class="mb-2 text-[13px] font-bold tracking-widest uppercase"
					style="color:{groupColors[selectedNode.group]};"
				>
					{groupLabels[selectedNode.group]}
				</p>
				<h2
					class="leading-tight font-black text-slate-800"
					style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;"
				>
					{selectedNode.label}
				</h2>
				<div
					class="mt-3 h-0.5 rounded-full"
					style="width:40px; background:{groupColors[selectedNode.group]};"
				></div>
				<p class="mt-4 text-[15px] leading-relaxed text-slate-500">{selectedNode.definition}</p>
				{#if selectedNode.dataProperties?.length}
					<div class="mt-5 border-t border-slate-100 pt-4">
						<p class="mb-2 text-[10px] font-bold tracking-widest text-slate-400 uppercase">
							Data Properties
						</p>
						<div class="flex flex-col gap-1.5">
							{#each selectedNode.dataProperties as property}
								<div class="flex items-center justify-between gap-3">
									<span class="text-[13px] text-slate-600">{property.label || property.range}</span>
									<span
										class="flex-shrink-0 rounded-md bg-slate-50 px-2 py-0.5 font-mono text-[11px] text-slate-400"
										>{property.range}</span
									>
								</div>
							{/each}
						</div>
					</div>
				{/if}
				<p class="mt-5 text-[12px] tracking-widest text-slate-300 uppercase">
					Click node or background to deselect
				</p>
			</div>
		</div>
	{/if}
</div>
