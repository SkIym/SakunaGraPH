<script>
	import TimelineBrush from './TimelineBrush.svelte';

	let { items = [], onselect = () => {} } = $props();
	let windowEnd = $state('');
	const COLORS = ['#0038a8', '#3768bd', '#6b8fd0', '#d6a900', '#59786d', '#6b7280', '#ce1126'];
	const periods = $derived(items.map((item) => item.period));
	const activeEnd = $derived(periods.includes(windowEnd) ? windowEnd : (periods.at(-1) ?? ''));
	const endIndex = $derived(Math.max(0, periods.indexOf(activeEnd)));
	const visibleItems = $derived(items.slice(Math.max(0, endIndex - 15), endIndex + 1));
	const categoryIds = $derived([
		...new Set(items.flatMap((item) => item.categories.map((category) => category.id))),
	]);
	const colorFor = (id) => COLORS[Math.max(0, categoryIds.indexOf(id)) % COLORS.length];
	const maximum = $derived(
		Math.max(
			1,
			...visibleItems.map((item) =>
				item.categories.reduce((sum, category) => sum + category.count, 0),
			),
		),
	);
	function formatPeriod(period) {
		if (!period) return '';
		if (/^\d{4}-\d{2}$/.test(period))
			return new Intl.DateTimeFormat('en-PH', { month: 'short', year: '2-digit' }).format(
				new Date(`${period}-01T00:00:00`),
			);
		return new Intl.DateTimeFormat('en-PH', { month: 'short' }).format(
			new Date(`2024-${period}-01T00:00:00`),
		);
	}
</script>

{#if items.length}
	<div class="stacked-timeline">
		<TimelineBrush {periods} value={activeEnd} onchange={(period) => (windowEnd = period)} />
		<div class="timeline-bars">
			{#each visibleItems as item (item.period)}
				{@const total = item.categories.reduce((sum, category) => sum + category.count, 0)}
				<div class="period-column">
					<button
						type="button"
						onclick={() => onselect(item.period)}
						class="period-bar"
						aria-label={`${item.period}: ${total.toLocaleString()} category assignments`}
						title={`${item.period}: ${total.toLocaleString()} category assignments`}
					>
						{#each item.categories as category (category.id)}
							<span
								style="height:{(category.count / maximum) * 100}%; background:{colorFor(
									category.id,
								)}"
							></span>
						{/each}
					</button>
					<span class="period-label">{formatPeriod(item.period)}</span>
				</div>
			{/each}
		</div>
		<div class="timeline-legend">
			{#each categoryIds as categoryId}
				{@const category = items
					.flatMap((item) => item.categories)
					.find((item) => item.id === categoryId)}
				{#if category}
					<span><i style="background:{colorFor(categoryId)}"></i>{category.label}</span>
				{/if}
			{/each}
		</div>
	</div>
{:else}
	<p class="flex h-56 items-center justify-center text-xs text-slate-400">
		No category timeline data in this scope.
	</p>
{/if}

<style>
	.stacked-timeline {
		display: grid;
		gap: 0.9rem;
	}

	.timeline-bars {
		display: flex;
		align-items: stretch;
		gap: 0.35rem;
		height: 14rem;
		border-block-end: 1px solid var(--color-border);
		padding: 0 0.25rem 1.55rem;
	}

	.period-column {
		display: grid;
		min-width: 0;
		max-width: 3.75rem;
		flex: 1;
		grid-template-rows: minmax(0, 1fr) auto;
		gap: 0.35rem;
	}

	.period-bar {
		display: flex;
		min-width: 0;
		height: 100%;
		flex-direction: column-reverse;
		justify-content: flex-start;
		overflow: hidden;
		border: 1px solid rgba(0, 56, 168, 0.16);
		border-block-end: 0;
		background: var(--color-brand-soft);
		text-align: left;
		transition:
			border-color 160ms ease,
			transform 160ms ease;
	}

	.period-bar:hover {
		border-color: var(--color-brand);
		transform: translateY(-2px);
	}

	.period-bar:focus-visible {
		outline: 2px solid var(--color-focus);
		outline-offset: 2px;
	}

	.period-bar span {
		display: block;
		width: 100%;
	}

	.period-label {
		overflow: hidden;
		color: var(--color-text-muted);
		font-family: var(--font-mono);
		font-size: 0.5rem;
		text-align: center;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.timeline-legend {
		display: flex;
		flex-wrap: wrap;
		gap: 0.45rem 1rem;
	}

	.timeline-legend span {
		display: inline-flex;
		align-items: center;
		gap: 0.4rem;
		color: var(--color-text-muted);
		font-size: 0.625rem;
	}

	.timeline-legend i {
		display: block;
		width: 0.55rem;
		height: 0.55rem;
	}

	@media (prefers-reduced-motion: reduce) {
		.period-bar {
			transition: none;
		}
	}
</style>
