<script>
	import OntologyLoading from './OntologyLoading.svelte';

	let {
		active = false,
		loading = false,
		selectedNode = null,
		islandColors = {},
		svgElement = $bindable(null),
	} = $props();
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

		<p class="pointer-events-none absolute top-16 right-5 text-[11px] font-medium text-slate-500">
			Hover · Click · Drag · Scroll
		</p>

		<div
			class="absolute bottom-6 left-6 rounded-2xl bg-white/85 px-5 py-4 shadow-2xl"
			style="backdrop-filter:blur(12px);"
		>
			<p class="mb-3 text-[10px] font-bold tracking-widest text-slate-400 uppercase">
				Island Group
			</p>
			<div class="flex flex-col gap-2">
				{#each Object.entries(islandColors) as [island, color]}
					<div class="flex items-center gap-2.5">
						<div
							class="h-3.5 w-3.5 flex-shrink-0 rounded-full"
							style="background:{color}30; border:2.5px solid {color};"
						></div>
						<span class="text-xs font-medium text-slate-600">{island}</span>
					</div>
				{/each}
			</div>
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
					<span class="text-xs text-slate-400 italic">HUC / ICC (independent)</span>
				</div>
			</div>
			<p class="mt-3 text-[10px] text-slate-300">HUCs link directly to region</p>
		</div>

		{#if selectedNode}
			<div
				class="absolute right-6 bottom-6 rounded-2xl border border-slate-200/60 bg-white/92 shadow-xl"
				style="backdrop-filter:blur(18px); width:420px; max-height:72vh; overflow-y:auto;"
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
					<p class="mt-1 text-[12px] tracking-widest text-slate-300 uppercase">
						Click node or canvas to deselect
					</p>
				</div>
			</div>
		{/if}
	</div>
{/if}
