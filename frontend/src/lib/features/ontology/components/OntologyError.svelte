<script>
	let { message = 'This ontology view could not be loaded.' } = $props();
</script>

<div class="ontology-error" role="alert">
	<span aria-hidden="true">×</span>
	<p>
		<strong>Visualization unavailable</strong>{message} Check the API connection, then try again.
	</p>
	<a href="/ontology">Try again</a>
</div>

<style>
	.ontology-error {
		position: absolute;
		z-index: 25;
		top: 50%;
		left: 50%;
		display: grid;
		width: min(28rem, calc(100% - 2rem));
		grid-template-columns: auto minmax(0, 1fr);
		gap: 0.3rem 0.85rem;
		border: 1px solid var(--color-danger-border);
		border-radius: 0.35rem 1rem 0.35rem 1rem;
		background: rgb(255 241 242 / 0.96);
		padding: 1rem;
		color: #881323;
		box-shadow: 0 1.5rem 3rem -2rem rgb(107 25 38 / 0.45);
		transform: translate(-50%, -50%);
		backdrop-filter: blur(0.75rem);
	}

	.ontology-error > span {
		display: grid;
		width: 1.8rem;
		height: 1.8rem;
		grid-row: 1 / 3;
		place-items: center;
		border: 1px solid var(--color-danger-border);
		border-radius: 50%;
		font-family: var(--font-mono);
		font-size: 1rem;
	}

	.ontology-error p {
		margin: 0;
		font-size: 0.75rem;
		line-height: 1.55;
	}

	.ontology-error strong {
		display: block;
		margin-bottom: 0.2rem;
		font-size: 0.8125rem;
	}

	.ontology-error a {
		grid-column: 2;
		width: fit-content;
		min-height: 2.75rem;
		display: inline-flex;
		align-items: center;
		font-size: 0.75rem;
		font-weight: 750;
		color: #881323;
		text-underline-offset: 0.25rem;
	}

	@media (forced-colors: active) {
		.ontology-error,
		.ontology-error > span {
			border-color: CanvasText;
			background: Canvas;
			color: CanvasText;
		}
	}
</style>
