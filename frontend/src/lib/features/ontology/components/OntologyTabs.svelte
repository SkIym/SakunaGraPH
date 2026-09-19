<script>
	let { tabs = [], active = '', orientation = 'inline', onChange = () => {} } = $props();
</script>

<nav class="ontology-tabs" class:rail={orientation === 'rail'} aria-label="Ontology views">
	<div class="tab-list">
		{#each tabs as tab, index}
			<button
				type="button"
				aria-pressed={active === tab.id}
				onclick={() => onChange(tab.id)}
				class:active={active === tab.id}
			>
				<span aria-hidden="true">{String(index + 1).padStart(2, '0')}</span>
				{tab.label}
			</button>
		{/each}
	</div>
</nav>

<style>
	.ontology-tabs {
		max-width: 100%;
		overflow-x: auto;
		border-bottom: 1px solid var(--color-border);
		scrollbar-width: none;
	}

	.ontology-tabs::-webkit-scrollbar {
		display: none;
	}

	.tab-list {
		display: flex;
		width: max-content;
		min-width: 100%;
		align-items: stretch;
	}

	button {
		position: relative;
		display: inline-flex;
		min-height: 2.75rem;
		align-items: center;
		gap: 0.45rem;
		border: 0;
		background: transparent;
		padding: 0.55rem 0.875rem;
		font: inherit;
		font-size: 0.6875rem;
		font-weight: 650;
		line-height: 1.35;
		white-space: nowrap;
		color: var(--color-text-muted);
		transition:
			background 150ms ease,
			color 150ms ease,
			transform 120ms ease;
	}

	button::after {
		position: absolute;
		inset: auto 0.875rem -1px;
		height: 3px;
		background: var(--color-accent);
		content: '';
		transform: scaleX(0);
		transform-origin: center;
		transition: transform 180ms ease;
	}

	button > span {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.5625rem;
		font-weight: 600;
		color: var(--color-brand);
	}

	button.active {
		background: var(--color-accent-soft);
		color: var(--color-text);
	}

	button.active::after {
		transform: scaleX(1);
	}

	button:active {
		transform: translateY(1px);
	}

	.ontology-tabs.rail {
		overflow: visible;
		border: 1px solid var(--color-border);
		border-radius: 0.35rem;
		background: rgb(255 255 255 / 0.72);
	}

	.rail .tab-list {
		width: 100%;
		min-width: 0;
		flex-direction: column;
	}

	.rail button {
		width: 100%;
		justify-content: flex-start;
		border-bottom: 1px solid var(--color-border);
		padding: 0.75rem 0.85rem;
		font-size: 0.75rem;
	}

	.rail button:last-child {
		border-bottom: 0;
	}

	.rail button::after {
		inset: 0 auto 0 -1px;
		width: 3px;
		height: auto;
		transform: scaleY(0);
	}

	.rail button.active::after {
		transform: scaleY(1);
	}

	@media (hover: hover) {
		button:not(.active):hover {
			background: var(--color-brand-soft);
			color: var(--color-brand-hover);
		}
	}

	@media (max-width: 639px) {
		button {
			flex: 1;
			justify-content: center;
			padding-inline: 0.6rem;
			font-size: 0.625rem;
		}

		button > span {
			display: none;
		}
	}

	@media (min-width: 640px) and (max-width: 1050px) {
		.ontology-tabs.rail {
			overflow-x: auto;
			border: 0;
			border-bottom: 1px solid var(--color-border);
			border-radius: 0;
		}

		.rail .tab-list {
			width: max-content;
			min-width: 100%;
			flex-direction: row;
		}

		.rail button {
			width: auto;
			border-bottom: 0;
			padding: 0.6rem 0.8rem;
		}

		.rail button::after {
			inset: auto 0.8rem -1px;
			width: auto;
			height: 3px;
			transform: scaleX(0);
		}

		.rail button.active::after {
			transform: scaleX(1);
		}
	}

	@media (max-width: 639px) {
		.ontology-tabs.rail {
			overflow-x: auto;
			border: 0;
			border-bottom: 1px solid var(--color-border);
			border-radius: 0;
		}

		.rail .tab-list {
			width: max-content;
			min-width: 100%;
			flex-direction: row;
		}

		.rail button {
			width: auto;
			flex: 1;
			justify-content: center;
			border-bottom: 0;
		}

		.rail button::after {
			inset: auto 0.6rem -1px;
			width: auto;
			height: 3px;
			transform: scaleX(0);
		}

		.rail button.active::after {
			transform: scaleX(1);
		}
	}

	@media (prefers-reduced-motion: reduce) {
		button,
		button::after {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		button.active {
			border-bottom: 3px solid Highlight;
		}
	}
</style>
