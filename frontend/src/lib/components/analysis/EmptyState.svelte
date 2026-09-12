<script>
	let {
		title = 'No records found',
		description = 'Try changing or clearing the active filters.',
		actionLabel = '',
		onaction = () => {},
		tone = 'neutral',
	} = $props();
</script>

<div
	class="analysis-empty-state {tone === 'error' ? 'error' : ''}"
	role={tone === 'error' ? 'alert' : 'status'}
>
	{#if tone === 'error'}
		<div class="empty-state-symbol" aria-hidden="true">
			<svg viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="1.8">
				<circle cx="12" cy="12" r="9"></circle>
				<path d="M12 7v6"></path>
				<path d="M12 17h.01"></path>
			</svg>
		</div>
	{:else}
		<div class="empty-state-symbol" aria-hidden="true">
			<svg viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="1.8">
				<ellipse cx="12" cy="5" rx="8" ry="3"></ellipse>
				<path d="M4 5v7c0 1.7 3.6 3 8 3s8-1.3 8-3V5"></path>
				<path d="M4 12v7c0 1.7 3.6 3 8 3s8-1.3 8-3v-7"></path>
			</svg>
		</div>
	{/if}
	<h2>
		{title}
	</h2>
	<p>
		{description}
	</p>
	{#if actionLabel}
		<button type="button" onclick={onaction} class="empty-state-action">
			{actionLabel}
		</button>
	{/if}
</div>

<style>
	.analysis-empty-state {
		display: grid;
		min-height: 18rem;
		place-items: start;
		align-content: center;
		border: 1px dashed #b8c5d8;
		border-radius: var(--radius-surface);
		background: var(--color-surface-subtle);
		padding: 3rem clamp(1.5rem, 5vw, 4rem);
		text-align: left;
	}

	.analysis-empty-state.error {
		border-color: var(--color-danger-border);
		background: var(--color-danger-surface);
	}

	.empty-state-symbol {
		display: grid;
		width: 2.75rem;
		height: 2.75rem;
		place-items: center;
		border: 1px solid var(--color-brand-medium);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		color: var(--color-brand);
	}

	.error .empty-state-symbol {
		border-color: var(--color-danger-border);
		color: var(--color-danger);
	}

	h2 {
		margin: 1.25rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.35rem;
		font-weight: 700;
		line-height: 1.2;
		color: var(--color-text);
	}

	.error h2 {
		color: #881323;
	}

	p {
		max-width: 32rem;
		margin: 0.5rem 0 0;
		font-size: 0.75rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
	}

	.error p {
		color: #a12636;
	}

	.empty-state-action {
		min-height: 2.75rem;
		margin-top: 1.25rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 1rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-brand);
		transition:
			background-color 180ms ease,
			transform 120ms ease;
	}

	.empty-state-action:hover {
		background: var(--color-brand-soft);
	}

	.empty-state-action:active {
		transform: translateY(1px);
	}
</style>
