<script>
	import OntologyLoading from './OntologyLoading.svelte';

	let {
		active = false,
		loading = false,
		selectedNode = null,
		colors = {},
		svgElement = $bindable(null),
	} = $props();
	let legendOpen = $state(false);

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
				>Category</button
			>
			<div class="legend-content mt-2 flex-col gap-1.5" class:mobile-open={legendOpen}>
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
				class="ontology-detail absolute rounded-2xl border border-slate-200/60 bg-white/95 shadow-xl"
				style="backdrop-filter:blur(18px);"
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
					<p class="mt-5 text-[12px] tracking-widest text-slate-500 uppercase">
						Select another type for details · Select the canvas to clear
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
			min-width: 12.5rem;
			max-width: 18.75rem;
			padding: 1rem;
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
