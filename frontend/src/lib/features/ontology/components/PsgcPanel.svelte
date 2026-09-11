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

	function cityClassification(value) {
		if (value === 'HUC') return 'Highly Urbanized City';
		if (value === 'ICC') return 'Independent Component City';
		return value;
	}
</script>

{#if active}
	<div class="ontology-panel">
		{#if loading}<OntologyLoading label="Loading PSGC graph…" />{/if}

		<svg
			bind:this={svgElement}
			class="graph-canvas"
			style="cursor:default;"
			aria-label="Interactive PSGC location graph"
		></svg>

		<aside class="ontology-legend">
			<button
				type="button"
				aria-expanded={legendOpen}
				onclick={() => (legendOpen = !legendOpen)}
				class="legend-toggle touch-target"
			>
				<span>Island Group</span>
				<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" class:open={legendOpen}>
					<path d="m6 9 6 6 6-6" />
				</svg>
			</button>
			<div class="legend-content" class:open={legendOpen}>
				{#each Object.entries(islandColors) as [island, color]}
					<div class="legend-row">
						<i class="island-node" style="background:{color}22; border-color:{color};"></i>
						<span>{island}</span>
					</div>
				{/each}
				<div class="level-legend">
					<div class="legend-row">
						<i class="region-node"></i>
						<span>Region</span>
					</div>
					<div class="legend-row">
						<i class="province-node"></i>
						<span>Province</span>
					</div>
					<div class="legend-row">
						<i class="city-node"></i>
						<span>Independent city</span>
					</div>
				</div>
				<p class="legend-note">HUC and ICC records link directly to their region.</p>
			</div>
		</aside>

		{#if selectedNode}
			<aside class="ontology-detail" aria-live="polite">
				<div class="detail-body">
					<p class="selection-label"><span aria-hidden="true"></span> Selected place</p>
					<p class="detail-group" style="color:{islandColors[selectedNode.island]};">
						{selectedNode.cityType ?? selectedNode.level} · {selectedNode.regionLabel ??
							selectedNode.island}
					</p>
					<h2>{selectedNode.fullName ?? selectedNode.label}</h2>
					<dl class="place-facts">
						<div>
							<dt>PSGC code</dt>
							<dd class="mono">{selectedNode.psgcCode ?? selectedNode.id}</dd>
						</div>
						<div>
							<dt>Geographic level</dt>
							<dd>{selectedNode.level ?? 'Not recorded'}</dd>
						</div>
						{#if selectedNode.cityType}
							<div>
								<dt>City classification</dt>
								<dd>{cityClassification(selectedNode.cityType)}</dd>
							</div>
						{/if}
						{#if selectedNode.incomeClass}
							<div>
								<dt>Income classification</dt>
								<dd>{selectedNode.incomeClass} class</dd>
							</div>
						{/if}
						<div>
							<dt>Population (2020)</dt>
							<dd class="mono">{selectedNode.population?.toLocaleString?.() ?? 'Not recorded'}</dd>
						</div>
					</dl>
					{#if selectedNode.note}
						<div class="place-note">
							<h3>Note</h3>
							<p>{selectedNode.note}</p>
						</div>
					{/if}
					<p class="detail-hint">Select another place, or select the canvas to clear.</p>
				</div>
			</aside>
		{/if}
	</div>
{/if}

<style>
	.ontology-panel {
		position: absolute;
		inset: 0;
	}

	.graph-canvas {
		display: block;
		width: 100%;
		height: 100%;
	}

	.ontology-legend,
	.ontology-detail {
		position: absolute;
		z-index: 20;
		border: 1px solid var(--color-border);
		background: var(--color-canvas);
	}

	.ontology-legend {
		left: 1rem;
		bottom: 1rem;
		max-width: calc(100% - 2rem);
		border-radius: var(--radius-control);
		box-shadow: var(--shadow-control);
	}

	.legend-toggle {
		display: flex;
		width: 100%;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		border: 0;
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0.6rem 0.8rem;
		font: inherit;
		font-size: 0.6875rem;
		font-weight: 700;
		line-height: 1;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-text-secondary);
	}

	.legend-toggle svg {
		width: 1rem;
		height: 1rem;
		color: var(--color-brand);
		transition: transform 180ms ease;
	}

	.legend-toggle svg.open {
		transform: rotate(180deg);
	}

	.legend-toggle path {
		stroke: currentColor;
		stroke-width: 1.8;
		stroke-linecap: round;
		stroke-linejoin: round;
	}

	.legend-content {
		display: none;
		width: 14rem;
		max-height: min(25rem, 62dvh);
		overflow-y: auto;
		border-top: 1px solid var(--color-border);
		padding: 0.75rem 0.85rem 0.85rem;
	}

	.legend-content.open {
		display: grid;
		gap: 0.5rem;
	}

	.legend-row {
		display: flex;
		align-items: center;
		gap: 0.65rem;
		font-size: 0.6875rem;
		line-height: 1.35;
		color: var(--color-text-secondary);
	}

	.legend-row i {
		display: block;
		flex: none;
		border-color: var(--color-text-muted);
	}

	.island-node {
		width: 0.8rem;
		height: 0.8rem;
		border: 2px solid;
		border-radius: 50%;
	}

	.level-legend {
		display: grid;
		gap: 0.5rem;
		margin-top: 0.35rem;
		border-top: 1px solid var(--color-border);
		padding-top: 0.7rem;
	}

	.region-node {
		width: 1rem;
		height: 1rem;
		border: 2px solid;
		border-radius: 50%;
		background: var(--color-surface-subtle);
	}

	.province-node {
		width: 0.7rem;
		height: 0.7rem;
		border: 1px solid;
		border-radius: 50%;
		background: var(--color-surface-subtle);
	}

	.city-node {
		width: 0.6rem;
		height: 0.6rem;
		border: 1px dashed;
		border-radius: 50%;
	}

	.legend-note {
		margin: 0.4rem 0 0;
		font-size: 0.625rem;
		line-height: 1.45;
		color: var(--color-text-muted);
	}

	.ontology-detail {
		right: 1rem;
		bottom: 1rem;
		width: min(25rem, calc(100% - 2rem));
		max-height: calc(100% - 2rem);
		overflow-y: auto;
		border-radius: var(--radius-surface);
		box-shadow: var(--shadow-surface);
	}

	.ontology-detail::before {
		position: absolute;
		inset: 0 auto auto 0;
		width: 5rem;
		height: 3px;
		background: var(--color-accent);
		content: '';
	}

	.detail-body {
		padding: 1.5rem;
	}

	.selection-label,
	.detail-group,
	.detail-hint {
		margin: 0;
	}

	.selection-label {
		display: flex;
		align-items: center;
		gap: 0.45rem;
		font-size: 0.625rem;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-text-secondary);
	}

	.selection-label span {
		width: 0.4rem;
		height: 0.4rem;
		border-radius: 50%;
		background: var(--color-accent);
		box-shadow: 0 0 0 1px #caa600;
	}

	.detail-group {
		margin-top: 1rem;
		font-size: 0.6875rem;
		font-weight: 750;
		letter-spacing: 0.1em;
		text-transform: uppercase;
	}

	.ontology-detail h2 {
		margin: 0.25rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.9rem;
		font-weight: 900;
		line-height: 1.05;
		letter-spacing: -0.025em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.place-facts {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 0;
		margin: 1.1rem 0 0;
		border-top: 1px solid var(--color-border);
	}

	.place-facts > div {
		border-bottom: 1px solid var(--color-border);
		padding: 0.75rem 0;
	}

	.place-facts > div:nth-child(even) {
		border-left: 1px solid var(--color-border);
		padding-left: 0.85rem;
	}

	.place-facts dt,
	.place-note h3 {
		font-size: 0.625rem;
		font-weight: 700;
		line-height: 1.35;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.place-facts dd {
		margin: 0.25rem 0 0;
		font-size: 0.75rem;
		line-height: 1.45;
		color: var(--color-text-secondary);
	}

	.place-facts dd.mono {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
	}

	.place-note {
		margin-top: 0.9rem;
		border-left: 2px solid var(--color-brand-medium);
		padding-left: 0.75rem;
	}

	.place-note h3,
	.place-note p {
		margin: 0;
	}

	.place-note p {
		margin-top: 0.25rem;
		font-size: 0.75rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	.detail-hint {
		margin-top: 1.1rem;
		font-size: 0.625rem;
		line-height: 1.45;
		color: var(--color-text-muted);
	}

	@media (hover: hover) {
		.legend-toggle:hover {
			background: var(--color-brand-soft);
			color: var(--color-brand-hover);
		}
	}

	@media (max-width: 639px) {
		.ontology-legend {
			left: 0.75rem;
			bottom: max(0.75rem, env(safe-area-inset-bottom));
		}

		.ontology-detail {
			right: 0.75rem;
			bottom: max(0.75rem, env(safe-area-inset-bottom));
			width: calc(100% - 1.5rem);
			max-height: 62%;
		}

		.detail-body {
			padding: 1.25rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.legend-toggle svg {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.ontology-legend,
		.ontology-detail,
		.legend-row i {
			border-color: CanvasText;
		}
	}
</style>
