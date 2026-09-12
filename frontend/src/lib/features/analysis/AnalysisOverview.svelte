<script>
	import { analysisFilters, EVENT_TYPE_OPTIONS } from './state/filters.svelte.js';

	const eventTypeLabels = new Map(EVENT_TYPE_OPTIONS.map((option) => [option.value, option.label]));
	const dateWindow = $derived.by(() => {
		if (analysisFilters.startDate && analysisFilters.endDate) {
			return `${analysisFilters.startDate} to ${analysisFilters.endDate}`;
		}
		if (analysisFilters.startDate) return `From ${analysisFilters.startDate}`;
		if (analysisFilters.endDate) return `Through ${analysisFilters.endDate}`;
		return 'All dates';
	});

	const scopeItems = $derived([
		{
			code: '01',
			label: 'Records',
			value: eventTypeLabels.get(analysisFilters.eventType),
		},
		{ code: '02', label: 'Date window', value: dateWindow },
		{
			code: '03',
			label: 'Locations',
			value: analysisFilters.locationIds.length
				? `${analysisFilters.locationIds.length} selected`
				: 'All locations',
		},
		{
			code: '04',
			label: 'Disaster types',
			value: analysisFilters.disasterTypes.length
				? `${analysisFilters.disasterTypes.length} selected`
				: 'All types',
		},
		{
			code: '05',
			label: 'Event name',
			value: analysisFilters.q.trim() || 'Any event name',
		},
	]);

	const routes = [
		{
			code: '01',
			title: 'Event records',
			description:
				'Browse the filtered graph record by record, compare provenance, and export the complete scope as CSV.',
			action: 'Open table',
			href: '/analysis/events',
			primary: true,
		},
		{
			code: '02',
			title: 'Metrics dashboard',
			description: 'Read totals, distributions, regional rankings, and reported impact trends.',
			action: 'Open metrics',
			href: '/analysis/metrics',
		},
		{
			code: '03',
			title: 'Timeline analysis',
			description:
				'Move from years to individual dates and inspect the records behind each period.',
			action: 'Open timeline',
			href: '/analysis/timeline',
		},
	];
</script>

<section class="analysis-overview">
	<header class="overview-intro">
		<div>
			<p class="workspace-kicker">Analysis desk</p>
			<h1 class="overview-title">Current scope</h1>
			<p class="overview-description">
				One shared filter scope controls every table, metric, and timeline below.
			</p>
		</div>
		<p class="overview-note"><span>Live scope</span> Changes carry across all three views.</p>
	</header>

	<dl class="scope-ledger" aria-label="Current analysis scope">
		{#each scopeItems as item}
			<div>
				<dt><span>{item.code}</span>{item.label}</dt>
				<dd title={item.value}>{item.value}</dd>
			</div>
		{/each}
	</dl>

	<nav class="analysis-route-grid" aria-label="Analysis tools">
		{#each routes as route}
			<a href={route.href} class:primary={route.primary}>
				<div class="route-heading">
					<span>{route.code}</span>
					<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
						<path d="M5 12h13M14 7l5 5-5 5" />
					</svg>
				</div>
				<h2>{route.title}</h2>
				<p>{route.description}</p>
				<strong>{route.action}</strong>
				{#if route.primary}
					<div class="route-ledger-lines" aria-hidden="true">
						<span></span><span></span><span></span><span></span>
					</div>
				{/if}
			</a>
		{/each}
	</nav>
</section>

<style>
	.analysis-overview {
		width: min(100%, 72rem);
		margin-inline: auto;
		padding: clamp(2rem, 6vw, 5rem) clamp(1rem, 4vw, 3.5rem) 5rem;
	}

	.overview-intro {
		display: flex;
		align-items: end;
		justify-content: space-between;
		gap: 3rem;
		padding-bottom: 1.75rem;
		border-bottom: 1px solid var(--color-border);
	}

	.overview-title {
		margin: 0.2rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2.35rem, 6vw, 4.75rem);
		font-weight: 900;
		line-height: 0.98;
		letter-spacing: -0.045em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.overview-description {
		max-width: 34rem;
		margin: 1rem 0 0;
		font-size: 0.875rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.overview-note {
		max-width: 13rem;
		margin: 0 0 0.2rem;
		font-size: 0.75rem;
		line-height: 1.55;
		color: var(--color-text-muted);
	}

	.overview-note span {
		display: block;
		margin-bottom: 0.2rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		font-weight: 600;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.scope-ledger {
		display: grid;
		grid-template-columns: repeat(5, minmax(0, 1fr));
		margin: 0;
		border-bottom: 1px solid var(--color-border);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
	}

	.scope-ledger > div {
		min-width: 0;
		padding: 1.25rem 1rem 1.4rem;
		border-right: 1px solid var(--color-border);
	}

	.scope-ledger > div:last-child {
		border-right: 0;
	}

	.scope-ledger dt {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		font-size: 0.625rem;
		font-weight: 700;
		line-height: 1.3;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.scope-ledger dt span {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		color: var(--color-brand);
	}

	.scope-ledger dd {
		overflow: hidden;
		margin: 0.75rem 0 0;
		font-size: 0.875rem;
		font-weight: 600;
		line-height: 1.4;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--color-text);
	}

	.analysis-route-grid {
		display: grid;
		grid-template-columns: minmax(0, 1.35fr) minmax(17rem, 0.65fr);
		grid-template-rows: repeat(2, minmax(13rem, auto));
		gap: 1px;
		margin-top: 2.5rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		overflow: hidden;
		background: var(--color-border);
		box-shadow: var(--shadow-surface);
	}

	.analysis-route-grid a {
		position: relative;
		display: flex;
		min-width: 0;
		min-height: 13rem;
		flex-direction: column;
		align-items: flex-start;
		padding: 1.5rem;
		background: var(--color-canvas);
		text-decoration: none;
		color: var(--color-text);
		transition:
			background-color 200ms ease,
			transform 150ms ease;
	}

	.analysis-route-grid a.primary {
		grid-row: 1 / -1;
		padding: clamp(1.75rem, 4vw, 3.25rem);
		background: var(--color-brand-soft);
	}

	.analysis-route-grid a:hover {
		background: var(--color-accent-soft);
	}

	.analysis-route-grid a:active {
		transform: translateY(1px);
	}

	.route-heading {
		display: flex;
		width: 100%;
		align-items: center;
		justify-content: space-between;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 600;
		letter-spacing: 0.08em;
		color: var(--color-brand);
	}

	.route-heading svg {
		width: 1.25rem;
		height: 1.25rem;
		stroke: currentColor;
		stroke-width: 1.6;
		stroke-linecap: round;
		stroke-linejoin: round;
		transition: transform 180ms ease;
	}

	.analysis-route-grid a:hover .route-heading svg {
		transform: translateX(0.2rem);
	}

	.analysis-route-grid h2 {
		max-width: 12ch;
		margin: 1.4rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.5rem, 3vw, 2.75rem);
		font-weight: 900;
		line-height: 1.02;
		letter-spacing: -0.035em;
		text-wrap: balance;
	}

	.analysis-route-grid a:not(.primary) h2 {
		margin-top: 0.8rem;
		font-size: 1.45rem;
	}

	.analysis-route-grid p {
		max-width: 34rem;
		margin: 1rem 0 0;
		font-size: 0.8125rem;
		line-height: 1.65;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.analysis-route-grid strong {
		margin-top: auto;
		padding-top: 1.5rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.route-ledger-lines {
		display: grid;
		width: min(100%, 28rem);
		gap: 0.65rem;
		margin-top: 2.25rem;
	}

	.route-ledger-lines span {
		display: block;
		height: 1px;
		background: rgb(0 56 168 / 0.22);
	}

	.route-ledger-lines span:nth-child(2) {
		width: 82%;
	}

	.route-ledger-lines span:nth-child(3) {
		width: 91%;
	}

	.route-ledger-lines span:nth-child(4) {
		width: 68%;
	}

	@media (max-width: 54rem) {
		.scope-ledger {
			grid-template-columns: repeat(2, minmax(0, 1fr));
		}

		.scope-ledger > div {
			border-bottom: 1px solid var(--color-border);
		}

		.scope-ledger > div:nth-child(2n) {
			border-right: 0;
		}

		.scope-ledger > div:last-child {
			grid-column: 1 / -1;
			border-bottom: 0;
		}

		.analysis-route-grid {
			grid-template-columns: 1fr;
			grid-template-rows: none;
		}

		.analysis-route-grid a.primary {
			grid-row: auto;
		}
	}

	@media (max-width: 40rem) {
		.analysis-overview {
			padding-top: 2.25rem;
		}

		.overview-intro {
			align-items: start;
			flex-direction: column;
			gap: 1.25rem;
		}

		.overview-note {
			max-width: 20rem;
		}

		.scope-ledger {
			box-shadow: none;
		}

		.analysis-route-grid {
			margin-top: 1.5rem;
			box-shadow: none;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.analysis-route-grid a,
		.route-heading svg {
			transition: none;
		}
	}
</style>
