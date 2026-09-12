<script>
	let { periods = [], value = '', onchange = () => {} } = $props();
	const selectedIndex = $derived(Math.max(0, periods.indexOf(value)));
	function update(event) {
		const period = periods[Number(event.currentTarget.value)];
		if (period) onchange(period);
	}
</script>

{#if periods.length > 1}
	<div class="timeline-brush">
		<span>Window</span>
		<input
			type="range"
			min="0"
			max={periods.length - 1}
			value={selectedIndex}
			oninput={update}
			class="h-1 flex-1"
			aria-label="Timeline window end"
		/>
		<output>{periods[selectedIndex]}</output>
	</div>
{/if}

<style>
	.timeline-brush {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		border: 1px solid var(--color-border);
		border-inline-start: 3px solid var(--color-brand);
		background: var(--color-surface-subtle);
		padding: 0.6rem 0.75rem;
	}

	.timeline-brush > span,
	output {
		color: var(--color-text-muted);
		font-family: var(--font-mono);
		font-size: 0.625rem;
	}

	.timeline-brush > span {
		font-weight: 700;
		text-transform: uppercase;
		letter-spacing: 0.08em;
	}

	input {
		accent-color: var(--color-brand);
	}

	input:focus-visible {
		outline: 2px solid var(--color-focus);
		outline-offset: 4px;
	}

	output {
		width: 4rem;
		text-align: right;
	}
</style>
