<script>
	import OntologyLoading from './OntologyLoading.svelte';

	let {
		active = false,
		loading = false,
		selectedNode = null,
		islandColors = {},
		svgElement = $bindable(null),
	} = $props();
	let legendOpen = $state(false);
</script>

{#if active}
	<div class="absolute inset-0">
		{#if loading}<OntologyLoading label="Loading PSGC graph…" />{/if}

		<svg
			bind:this={svgElement}
			class="h-full w-full"
			style="cursor:default;"
			aria-label="Interactive PSGC location graph"
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
				>Island Group</button
			>
			<div class="legend-content mt-2 flex-col gap-2" class:mobile-open={legendOpen}>
				{#each Object.entries(islandColors) as [island, color]}
					<div class="flex items-center gap-2.5">
						<div
							class="h-3.5 w-3.5 flex-shrink-0 rounded-full"
							style="background:{color}30; border:2.5px solid {color};"
						></div>
						<span class="text-xs font-medium text-slate-600">{island}</span>
					</div>
				{/each}
				<div class="mt-3 flex flex-col gap-1.5 border-t border-slate-100 pt-3">
					<div class="flex items-center gap-2.5">
						<div
							class="h-5 w-5 flex-shrink-0 rounded-full border-2 border-slate-400 bg-slate-100"
						></div>
						<span class="text-xs text-slate-400">Region</span>
					</div>
					<div class="flex items-center gap-2.5">
						<div
							class="h-3 w-3 flex-shrink-0 rounded-full border border-slate-400 bg-slate-100"
						></div>
						<span class="text-xs text-slate-400">Province</span>
					</div>
					<div class="flex items-center gap-2.5">
						<div
							class="h-2.5 w-2.5 flex-shrink-0 rounded-full"
							style="background:transparent; border: 1.2px dashed #94a3b8;"
						></div>
						<span class="text-xs text-slate-500 italic">Independent city (HUC / ICC)</span>
					</div>
				</div>
				<p class="mt-3 text-[11px] text-slate-500">HUCs link directly to region</p>
			</div>
		</div>

		{#if selectedNode}
			<div
				class="ontology-detail absolute rounded-2xl border border-slate-200/60 bg-white/95 shadow-xl"
				style="backdrop-filter:blur(18px);"
			>
				<div class="flex flex-col gap-3 px-8 py-7">
					<p
						class="text-[13px] font-bold tracking-widest uppercase"
						style="color:{islandColors[selectedNode.island]};"
					>
						{selectedNode.cityType ?? selectedNode.level} · {selectedNode.regionLabel ??
							selectedNode.island}
					</p>
					<h2
						class="leading-tight font-black text-slate-800"
						style="font-family:'Playfair Display', Georgia,serif; font-weight:900; font-size:1.8rem;"
					>
						{selectedNode.fullName ?? selectedNode.label}
					</h2>
					<div
						class="h-0.5 rounded-full"
						style="width:40px; background:{islandColors[selectedNode.island]};"
					></div>
					<div class="mt-1 flex flex-col gap-3">
						<div>
							<p class="text-[10px] font-bold tracking-wider text-slate-400 uppercase">PSGC Code</p>
							<p class="font-mono text-[14px] text-slate-600">{selectedNode.psgcCode}</p>
						</div>
						<div>
							<p class="text-[10px] font-bold tracking-wider text-slate-400 uppercase">
								Geographic Level
							</p>
							<p class="text-[14px] text-slate-600">{selectedNode.level}</p>
						</div>
						{#if selectedNode.cityType}
							<div>
								<p class="text-[10px] font-bold tracking-wider text-slate-400 uppercase">
									City Classification
								</p>
								<p class="text-[14px] text-slate-600">
									{selectedNode.cityType === 'HUC'
										? 'Highly Urbanized City'
										: selectedNode.cityType === 'ICC'
											? 'Independent Component City'
											: selectedNode.cityType}
								</p>
							</div>
						{/if}
						{#if selectedNode.incomeClass}
							<div>
								<p class="text-[10px] font-bold tracking-wider text-slate-400 uppercase">
									Income Classification
								</p>
								<p class="text-[14px] text-slate-600">{selectedNode.incomeClass} class</p>
							</div>
						{/if}
						<div>
							<p class="text-[10px] font-bold tracking-wider text-slate-400 uppercase">
								Population (2020)
							</p>
							<p class="text-[14px] text-slate-600">{selectedNode.population.toLocaleString()}</p>
						</div>
					</div>
					{#if selectedNode.note}
						<div class="border-t border-slate-100 pt-2">
							<p class="mb-0.5 text-[10px] font-bold tracking-wider text-slate-400 uppercase">
								Note
							</p>
							<p class="text-[14px] leading-snug text-slate-500 italic">{selectedNode.note}</p>
						</div>
					{/if}
					<p class="mt-1 text-[12px] tracking-widest text-slate-500 uppercase">
						Select another place for details · Select the canvas to clear
					</p>
				</div>
			</div>
		{/if}
	</div>
{/if}

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
