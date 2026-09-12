<script>
	let {
		input = $bindable(''),
		sending = false,
		error = '',
		maxLength = 1_000,
		onSend = () => {},
		onCancel = () => {},
	} = $props();

	const showCount = $derived(input.length >= Math.floor(maxLength * 0.8));

	function handleKeydown(event) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			onSend();
		}
	}

	function handleSubmit(event) {
		event.preventDefault();
		if (sending) onCancel();
		else onSend();
	}
</script>

<section class="composer-dock" aria-label="Ask a question">
	<div class="composer-inner">
		<form onsubmit={handleSubmit}>
			<div class:has-error={error} class="composer-control">
				<textarea
					bind:value={input}
					onkeydown={handleKeydown}
					aria-label="Question"
					aria-describedby="ask-input-help ask-input-limit"
					aria-invalid={error ? 'true' : undefined}
					maxlength={maxLength}
					rows="1"
					placeholder="Ask a question about Philippine disaster data…"></textarea>
				<button type="submit" disabled={!sending && !input.trim()} class:stop-control={sending}>
					{#if sending}
						<span aria-hidden="true"></span>
						Stop answer
					{:else}
						Send
						<svg
							aria-hidden="true"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="1.8"
							stroke-linecap="round"
							stroke-linejoin="round"
						>
							<path d="m5 12 14-7-4 14-3-6-7-1Z" />
							<path d="m12 13 7-8" />
						</svg>
					{/if}
				</button>
			</div>
		</form>

		<div class="composer-meta">
			<div>
				<p id="ask-input-help">
					{sending
						? 'Enter to replace the current question · Stop answer ends the current response'
						: 'Enter to send · Shift+Enter for a new line'}
				</p>
				{#if error}
					<p class="input-error" role="alert">{error}</p>
				{/if}
			</div>
			<p id="ask-input-limit" class:visible={showCount} class="input-limit">
				{input.length.toLocaleString()} / {maxLength.toLocaleString()}
			</p>
		</div>
	</div>
</section>

<style>
	.composer-dock {
		flex: 0 0 auto;
		border-block-start: 1px solid var(--color-border);
		background: rgb(255 255 255 / 0.94);
		padding: 0.85rem clamp(1rem, 4vw, 2.5rem) max(0.85rem, env(safe-area-inset-bottom));
		backdrop-filter: blur(0.75rem);
	}

	.composer-inner {
		width: min(100%, 52rem);
		margin-inline: auto;
	}

	.composer-control {
		display: grid;
		grid-template-columns: minmax(0, 1fr) auto;
		align-items: end;
		border: 1px solid var(--color-border);
		border-block-start: 3px solid var(--color-brand);
		border-radius: var(--radius-surface);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
		transition:
			border-color 180ms ease,
			box-shadow 180ms ease;
	}

	.composer-control:focus-within {
		border-color: var(--color-brand);
		box-shadow:
			0 0 0 2px var(--color-brand-medium),
			var(--shadow-surface);
	}

	.composer-control.has-error {
		border-color: var(--color-danger-border);
		border-block-start-color: var(--color-danger);
		background: var(--color-danger-surface);
	}

	textarea {
		field-sizing: content;
		width: 100%;
		min-width: 0;
		min-height: 3.35rem;
		max-height: 9rem;
		resize: none;
		overflow-y: auto;
		border: 0;
		border-radius: var(--radius-surface);
		background: transparent;
		padding: 0.9rem 1rem;
		font-family: 'Inter', system-ui, sans-serif;
		font-size: 0.875rem;
		line-height: 1.55;
		color: var(--color-text);
		outline: 0;
	}

	textarea::placeholder {
		color: var(--color-text-muted);
	}

	button {
		display: inline-flex;
		min-width: 5.5rem;
		min-height: 2.75rem;
		align-items: center;
		justify-content: center;
		gap: 0.45rem;
		margin: 0.35rem;
		border: 0;
		border-radius: var(--radius-control);
		background: var(--color-text);
		padding: 0 1rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-canvas);
		box-shadow: var(--shadow-control);
		transition:
			background-color 180ms ease,
			transform 120ms ease,
			opacity 180ms ease;
	}

	button svg {
		width: 0.95rem;
		height: 0.95rem;
	}

	button:hover:not(:disabled) {
		background: #0f1a2b;
	}

	button:active:not(:disabled) {
		transform: translateY(1px);
	}

	button:disabled {
		cursor: not-allowed;
		background: var(--color-border);
		color: var(--color-text-secondary);
	}

	button.stop-control {
		background: var(--color-danger);
	}

	button.stop-control:hover {
		background: var(--color-action-hover);
	}

	button.stop-control span {
		width: 0.55rem;
		height: 0.55rem;
		background: currentColor;
	}

	.composer-meta {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 1rem;
		padding: 0.45rem 0.25rem 0;
		font-size: 0.65rem;
		line-height: 1.45;
		color: var(--color-text-secondary);
	}

	.composer-meta p {
		margin: 0;
	}

	.input-error {
		margin-top: 0.25rem !important;
		font-weight: 700;
		color: #881323;
	}

	.input-limit {
		visibility: hidden;
		flex: none;
		font-family: var(--font-mono);
		font-variant-numeric: tabular-nums;
		color: var(--color-text-muted);
	}

	.input-limit.visible {
		visibility: visible;
	}

	@media (max-width: 36rem) {
		.composer-dock {
			padding-inline: 0.75rem;
		}

		textarea {
			font-size: 1rem;
		}

		button {
			min-width: 4.75rem;
			padding-inline: 0.8rem;
		}

		.composer-meta {
			font-size: 0.625rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.composer-control,
		button {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.composer-control {
			border: 2px solid CanvasText;
		}
	}
</style>
