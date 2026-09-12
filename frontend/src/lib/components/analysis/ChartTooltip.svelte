<script>
	let { visible = false, x = 0, y = 0, title = '', lines = [] } = $props();
</script>

{#if visible}
	<div
		role="status"
		class="chart-tooltip"
		style="left:{x}px; top:{y}px; transform:translate(-50%, -110%);"
	>
		<p class="font-semibold">{title}</p>
		{#each lines as line}
			<p>{line}</p>
		{/each}
	</div>
{/if}

<style>
	.chart-tooltip {
		position: absolute;
		z-index: 10;
		min-width: 7rem;
		pointer-events: none;
		border: 1px solid rgb(255 255 255 / 0.15);
		border-radius: 0.5rem;
		background: var(--color-text);
		padding: 0.55rem 0.7rem;
		font-size: 0.625rem;
		color: var(--color-canvas);
		box-shadow: 0 10px 24px -14px rgb(0 43 130 / 0.6);
	}

	.chart-tooltip p {
		margin: 0.2rem 0 0;
		color: #d7e3fb;
	}

	.chart-tooltip p:first-child {
		margin-top: 0;
		font-weight: 700;
		color: var(--color-canvas);
	}
</style>
