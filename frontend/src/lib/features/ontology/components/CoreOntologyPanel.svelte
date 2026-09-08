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
	let legendOpen = $state(false);
</script>

<div class="absolute inset-0" class:hidden={!active}>
	{#if loading}<OntologyLoading label="Rendering ontology graph…" />{/if}

	<svg
		bind:this={svgElement}
		class="h-full w-full"
		style="cursor:default;"
		aria-label="Interactive core ontology class graph"
	></svg>

	<p class="ontology-gesture pointer-events-none absolute text-[11px] font-medium text-slate-600">
		Select · Drag · Zoom
	</p>

	<div
		class="ontology-legend absolute rounded-2xl bg-white/95 px-4 py-3 shadow-2xl"
		style="backdrop-filter:blur(12px);"
	>
		<button
			type="button"
			aria-expanded={legendOpen}
			onclick={() => (legendOpen = !legendOpen)}
			class="legend-toggle min-h-11 cursor-pointer py-3 text-[11px] font-bold tracking-widest text-slate-600 uppercase"
			>Legend</button
		>
		<div class="legend-content mt-2 flex-col gap-2" class:mobile-open={legendOpen}>
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
			<p class="mt-3 text-[11px] text-slate-500">Zoom · Drag canvas · Drag nodes</p>
		</div>
	</div>

	{#if selectedNode}
		<div
			class="ontology-detail absolute rounded-2xl border border-slate-200/60 bg-white/95 shadow-xl"
			style="backdrop-filter:blur(18px);"
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
				<p class="mt-5 text-[12px] tracking-widest text-slate-500 uppercase">
					Select another node for details · Select the canvas to clear
				</p>
			</div>
		</div>
	{/if}
</div>

<style>
	.ontology-gesture {
		top: 4.75rem;
		right: 1rem;
	}
	.ontology-legend {
		left: 0.75rem;
		bottom: calc(0.75rem + env(safe-area-inset-bottom));
		max-width: calc(100vw - 1.5rem);
	}
	.legend-content {
		display: none;
	}
	.legend-content.mobile-open {
		display: flex;
	}
	.ontology-detail {
		left: 0.75rem;
		right: 0.75rem;
		bottom: calc(0.75rem + env(safe-area-inset-bottom));
		max-height: 52dvh;
		overflow-y: auto;
	}
	.ontology-detail > div {
		padding: 1.25rem;
	}

	@media (min-width: 768px) {
		.ontology-gesture {
			top: 4rem;
			right: 1.25rem;
		}
		.ontology-legend {
			left: 1.5rem;
			bottom: 1.5rem;
			padding: 1rem 1.25rem;
		}
		.legend-toggle {
			min-height: 0;
			cursor: default;
			padding-block: 0 0.75rem;
			pointer-events: none;
		}
		.ontology-legend > .legend-content {
			display: flex;
			margin-top: 0;
		}
		.ontology-detail {
			left: auto;
			right: 1.5rem;
			bottom: 1.5rem;
			width: min(26.25rem, calc(100vw - 3rem));
			max-height: 72dvh;
		}
		.ontology-detail > div {
			padding: 1.75rem 2rem;
		}
	}
</style>
