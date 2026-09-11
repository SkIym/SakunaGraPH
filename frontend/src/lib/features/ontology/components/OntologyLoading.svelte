<script>
	let { label } = $props();
</script>

<div class="ontology-loading" role="status" aria-live="polite">
	<div class="loading-glyph" aria-hidden="true">
		<i class="link link-a"></i>
		<i class="link link-b"></i>
		<span class="node node-a"></span>
		<span class="node node-b"></span>
		<span class="node node-c"></span>
	</div>
	<p>{label}</p>
	<small>Preparing linked entities and relationships</small>
</div>

<style>
	.ontology-loading {
		position: absolute;
		inset: 0;
		z-index: 10;
		display: flex;
		align-items: center;
		justify-content: center;
		flex-direction: column;
		background: rgba(252, 253, 255, 0.94);
		text-align: center;
	}

	.loading-glyph {
		position: relative;
		width: 7rem;
		height: 3.5rem;
		margin-bottom: 1rem;
	}

	.node,
	.link {
		position: absolute;
		display: block;
	}

	.node {
		z-index: 2;
		width: 0.875rem;
		height: 0.875rem;
		border: 2px solid var(--color-brand);
		border-radius: 50%;
		background: var(--color-canvas);
		animation: node-breathe 1.2s ease-in-out infinite alternate;
	}

	.node-a {
		left: 0.25rem;
		top: 2rem;
	}

	.node-b {
		left: 3rem;
		top: 0.25rem;
		animation-delay: 120ms;
	}

	.node-c {
		right: 0.25rem;
		top: 2rem;
		animation-delay: 240ms;
	}

	.link {
		z-index: 1;
		top: 1.8rem;
		width: 3.25rem;
		height: 1px;
		background: var(--color-brand-medium);
	}

	.link-a {
		left: 0.8rem;
		transform: rotate(-30deg);
	}

	.link-b {
		right: 0.8rem;
		transform: rotate(30deg);
	}

	p,
	small {
		margin: 0;
	}

	p {
		font-size: 0.8125rem;
		font-weight: 700;
		color: var(--color-text-secondary);
	}

	small {
		margin-top: 0.3rem;
		font-size: 0.6875rem;
		line-height: 1.4;
		color: var(--color-text-muted);
	}

	@media (prefers-reduced-motion: reduce) {
		.node {
			animation: none;
		}
	}

	@media (forced-colors: active) {
		.ontology-loading {
			background: Canvas;
		}

		.node,
		.link {
			border-color: CanvasText;
			background: CanvasText;
		}
	}

	@keyframes node-breathe {
		to {
			transform: scale(1.16);
			background: var(--color-brand-soft);
		}
	}
</style>
