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

<div class="ontology-panel" class:hidden={!active}>
	{#if loading}<OntologyLoading label="Rendering ontology graph…" />{/if}

	<svg
		bind:this={svgElement}
		class="graph-canvas"
		style="cursor:default;"
		aria-label="Interactive core ontology class graph"
	></svg>

	<aside class="ontology-legend">
		<button
			type="button"
			aria-expanded={legendOpen}
			onclick={() => (legendOpen = !legendOpen)}
			class="legend-toggle touch-target"
		>
			<span>Legend</span>
			<svg aria-hidden="true" viewBox="0 0 24 24" fill="none" class:open={legendOpen}>
				<path d="m6 9 6 6 6-6" />
			</svg>
		</button>
		<div class="legend-content" class:open={legendOpen}>
			{#each Object.entries(groupLabels) as [group, label]}
				<div class="legend-row">
					<i
						class="legend-node"
						style="background:{groupColors[group]}22; border-color:{groupColors[group]};"
					></i>
					<span>{label}</span>
				</div>
			{/each}
			<div class="relation-legend">
				<div class="legend-row">
					<i class="legend-line"></i>
					<span>subClassOf</span>
				</div>
				<div class="legend-row">
					<svg width="28" height="7" aria-hidden="true">
						<line x1="0" y1="3.5" x2="28" y2="3.5"></line>
					</svg>
					<span>object property</span>
				</div>
			</div>
			<p class="legend-note">Drag nodes to inspect dense relationships.</p>
		</div>
	</aside>

	{#if selectedNode}
		<aside class="ontology-detail" aria-live="polite">
			<div class="detail-body">
				<p class="selection-label"><span aria-hidden="true"></span> Selected node</p>
				<p class="detail-group" style="color:{groupColors[selectedNode.group]};">
					{groupLabels[selectedNode.group]}
				</p>
				<h2>{selectedNode.label}</h2>
				<p class="detail-definition">{selectedNode.definition}</p>
				{#if selectedNode.dataProperties?.length}
					<div class="property-list">
						<h3>Data properties</h3>
						<dl>
							{#each selectedNode.dataProperties as property}
								<div>
									<dt>{property.label || property.range}</dt>
									<dd>{property.range}</dd>
								</div>
							{/each}
						</dl>
					</div>
				{/if}
				<p class="detail-hint">Select another node, or select the canvas to clear.</p>
			</div>
		</aside>
	{/if}
</div>

<style>
	.ontology-panel {
		position: absolute;
		inset: 0;
	}

	.hidden {
		display: none;
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
		box-shadow: var(--shadow-control);
	}

	.ontology-legend {
		left: 1rem;
		bottom: 1rem;
		max-width: calc(100% - 2rem);
		border-radius: var(--radius-control);
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
		width: 13rem;
		max-height: min(24rem, 60dvh);
		overflow-y: auto;
		border-top: 1px solid var(--color-border);
		padding: 0.75rem 0.85rem 0.85rem;
	}

	.legend-content.open {
		display: grid;
		gap: 0.45rem;
	}

	.legend-row {
		display: flex;
		align-items: center;
		gap: 0.6rem;
		font-size: 0.6875rem;
		line-height: 1.35;
		color: var(--color-text-secondary);
	}

	.legend-node {
		width: 0.75rem;
		height: 0.75rem;
		flex: none;
		border: 2px solid;
		border-radius: 50%;
	}

	.relation-legend {
		display: grid;
		gap: 0.45rem;
		margin-top: 0.35rem;
		border-top: 1px solid var(--color-border);
		padding-top: 0.65rem;
	}

	.legend-line {
		width: 1.75rem;
		height: 1px;
		flex: none;
		background: #cbd5e1;
	}

	.relation-legend line {
		stroke: var(--color-brand-medium);
		stroke-width: 1.8;
		stroke-dasharray: 5 3;
	}

	.legend-note {
		margin: 0.35rem 0 0;
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
	.detail-hint,
	.detail-definition {
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

	.detail-definition {
		margin-top: 0.9rem;
		font-size: 0.875rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
	}

	.property-list {
		margin-top: 1rem;
		border-top: 1px solid var(--color-border);
		padding-top: 1rem;
	}

	.property-list h3 {
		margin: 0 0 0.5rem;
		font-size: 0.625rem;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.property-list dl {
		display: grid;
		gap: 0.4rem;
		margin: 0;
	}

	.property-list dl > div {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 1rem;
	}

	.property-list dt {
		font-size: 0.75rem;
		color: var(--color-text-secondary);
	}

	.property-list dd {
		margin: 0;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		color: var(--color-text-muted);
	}

	.detail-hint {
		margin-top: 1.1rem;
		border-top: 1px solid var(--color-border);
		padding-top: 0.8rem;
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
			max-height: 58%;
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
		.legend-node {
			border-color: CanvasText;
		}
	}
</style>
