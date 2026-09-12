<script>
	let { summary = null } = $props();

	function formatNumber(value) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 0 }).format(value ?? 0);
	}

	function formatAmount(amount) {
		return new Intl.NumberFormat('en-PH', { maximumFractionDigits: 1 }).format(amount ?? 0);
	}

	const cards = $derived([
		{ code: '01', label: 'Event records', value: summary?.record_count ?? 0, primary: true },
		{ code: '02', label: 'Reported deaths', value: summary?.dead ?? 0 },
		{ code: '03', label: 'Injured', value: summary?.injured ?? 0 },
		{ code: '04', label: 'Missing', value: summary?.missing ?? 0 },
		{ code: '05', label: 'Affected families', value: summary?.affectedFamilies ?? 0 },
		{ code: '06', label: 'Affected persons', value: summary?.affectedPersons ?? 0 },
	]);
</script>

<dl class="metric-ledger">
	{#each cards as card}
		<div class:primary={card.primary}>
			<dt><span>{card.code}</span>{card.label}</dt>
			<dd>{formatNumber(card.value)}</dd>
		</div>
	{/each}
	<div class="damage-summary">
		<dt><span>07</span>Reported damage</dt>
		{#if summary?.damage?.length}
			<dd class="damage-values">
				{#each summary.damage as damage (damage.unit)}
					<p>
						{formatAmount(damage.amount)}
						<span>{damage.unit}</span>
					</p>
				{/each}
			</dd>
		{:else}
			<dd class="damage-empty">No reported damage amounts</dd>
		{/if}
	</div>
</dl>

<style>
	.metric-ledger {
		display: grid;
		grid-template-columns: repeat(4, minmax(0, 1fr));
		margin: 0;
		border-bottom: 1px solid var(--color-border);
		background: var(--color-border);
		gap: 1px;
	}

	.metric-ledger > div {
		min-width: 0;
		background: var(--color-canvas);
		padding: 1.25rem;
	}

	.metric-ledger > div.primary {
		grid-column: span 2;
		background: var(--color-brand-soft);
	}

	dt {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		font-size: 0.625rem;
		font-weight: 700;
		line-height: 1.3;
		letter-spacing: 0.075em;
		text-transform: uppercase;
		color: var(--color-text-secondary);
	}

	dt span {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		color: var(--color-brand);
	}

	dd {
		margin: 0.7rem 0 0;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: clamp(1.6rem, 3vw, 2.35rem);
		font-weight: 500;
		line-height: 1;
		font-variant-numeric: tabular-nums;
		color: var(--color-text);
	}

	.primary dd {
		font-size: clamp(2.25rem, 5vw, 3.75rem);
	}

	.damage-values {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem 1.5rem;
		margin-top: 0.8rem;
	}

	.damage-values p {
		margin: 0;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 1rem;
		font-weight: 600;
		font-variant-numeric: tabular-nums;
		color: var(--color-text);
	}

	.damage-values span {
		font-size: 0.625rem;
		font-weight: 500;
		color: var(--color-text-muted);
	}

	.damage-empty {
		margin: 0.8rem 0 0;
		font-size: 0.75rem;
		color: var(--color-text-muted);
	}

	@media (max-width: 47.999rem) {
		.metric-ledger {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}

		.metric-ledger > div.primary {
			grid-column: span 2;
		}
	}
</style>
