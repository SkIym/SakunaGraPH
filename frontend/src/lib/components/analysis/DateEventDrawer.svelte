<script>
	import { manageDrawerFocus } from '../../actions/focus.js';

	let {
		datePrefix = '',
		items = [],
		loading = false,
		error = '',
		onclose = () => {},
		onselect = () => {},
	} = $props();
	const formatter = new Intl.DateTimeFormat('en-PH', {
		year: 'numeric',
		month: 'long',
		day: 'numeric',
	});
	function formatDate(value) {
		if (!value) return 'No date recorded';
		const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
		return Number.isNaN(parsed.valueOf()) ? value : formatter.format(parsed);
	}
</script>

<aside use:manageDrawerFocus class="date-event-drawer" aria-label="Events for selected date">
	<header>
		<div>
			<p class="workspace-kicker">Date events</p>
			<h2>{datePrefix}</h2>
		</div>
		<button
			type="button"
			data-focus-first
			onclick={onclose}
			class="drawer-close"
			aria-label="Close date events">&times;</button
		>
	</header>
	<div class="drawer-body">
		{#if loading}
			<div class="drawer-loading">
				{#each [1, 2, 3]}<div></div>{/each}
			</div>
		{:else if error}
			<p class="drawer-error">{error}</p>
		{:else if items.length}
			<div class="date-event-list">
				{#each items as item (item.event)}
					<button type="button" onclick={() => onselect(item.event)} class="date-event-item">
						<div class="date-event-heading">
							<p>
								{item.eventName}
							</p>
							<span
								class:event-major={item.eventType === 'MajorEvent'}
								class:event-incident={item.eventType !== 'MajorEvent'}
								class="event-kind">{item.eventType === 'MajorEvent' ? 'Major' : 'Incident'}</span
							>
						</div>
						<p class="event-date">{formatDate(item.startDate)}</p>
						{#if item.disasterTypes.length}
							<p class="event-types">
								{item.disasterTypes.map((type) => type.label).join(', ')}
							</p>
						{/if}
					</button>
				{/each}
			</div>
		{:else}
			<p class="drawer-empty">No events start in this selected period.</p>
		{/if}
	</div>
</aside>

<style>
	.date-event-drawer {
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-block-start: 3px solid var(--color-brand);
		background: var(--color-surface);
		box-shadow: 0 12px 24px rgba(23, 32, 51, 0.08);
	}

	header {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 0.75rem;
		border-block-end: 1px solid var(--color-border);
		padding: 1rem;
	}

	h2 {
		margin-block-start: 0.3rem;
		color: var(--color-text);
		font-family: var(--font-mono);
		font-size: 0.8rem;
		font-weight: 700;
	}

	.drawer-close {
		display: grid;
		width: 2.75rem;
		height: 2.75rem;
		flex: 0 0 auto;
		place-items: center;
		border: 1px solid transparent;
		color: var(--color-text-muted);
		font-size: 1.2rem;
		line-height: 1;
		transition:
			background-color 160ms ease,
			border-color 160ms ease,
			color 160ms ease;
	}

	.drawer-close:hover {
		border-color: var(--color-border);
		background: var(--color-surface-subtle);
		color: var(--color-text);
	}

	.drawer-close:focus-visible,
	.date-event-item:focus-visible {
		outline: 2px solid var(--color-focus);
		outline-offset: 2px;
	}

	.drawer-body {
		max-height: 29.375rem;
		overflow-y: auto;
	}

	.drawer-loading {
		display: grid;
		gap: 1px;
		background: var(--color-border);
	}

	.drawer-loading div {
		height: 5rem;
		animation: pulse 1.4s ease-in-out infinite;
		background: var(--color-surface-subtle);
	}

	.drawer-error {
		margin: 1rem;
		border-inline-start: 3px solid var(--color-danger);
		background: var(--color-danger-surface);
		padding: 0.75rem;
		color: var(--color-danger);
		font-size: 0.75rem;
	}

	.date-event-list {
		display: grid;
	}

	.date-event-item {
		width: 100%;
		border-block-end: 1px solid var(--color-border);
		padding: 1rem;
		background: transparent;
		text-align: left;
		transition: background-color 160ms ease;
	}

	.date-event-item:hover {
		background: var(--color-brand-soft);
	}

	.date-event-heading {
		display: flex;
		align-items: flex-start;
		justify-content: space-between;
		gap: 0.75rem;
	}

	.date-event-heading p {
		display: -webkit-box;
		overflow: hidden;
		color: var(--color-text);
		font-size: 0.75rem;
		font-weight: 700;
		line-height: 1.5;
		-webkit-box-orient: vertical;
		-webkit-line-clamp: 2;
		line-clamp: 2;
	}

	.event-kind {
		flex: 0 0 auto;
		border-inline-start: 2px solid;
		padding: 0.2rem 0.4rem;
		font-family: var(--font-mono);
		font-size: 0.56rem;
		font-weight: 700;
	}

	.event-major {
		border-color: var(--color-brand);
		background: var(--color-brand-soft);
		color: var(--color-brand);
	}

	.event-incident {
		border-color: #d6a900;
		background: var(--color-accent-soft);
		color: #705900;
	}

	.event-date {
		margin-block-start: 0.35rem;
		color: var(--color-text-muted);
		font-family: var(--font-mono);
		font-size: 0.61rem;
	}

	.event-types {
		overflow: hidden;
		margin-block-start: 0.55rem;
		color: var(--color-text-secondary);
		font-size: 0.625rem;
		text-overflow: ellipsis;
		white-space: nowrap;
	}

	.drawer-empty {
		padding: 2.5rem 1rem;
		color: var(--color-text-muted);
		font-size: 0.75rem;
		text-align: center;
	}

	@keyframes pulse {
		50% {
			opacity: 0.5;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.drawer-loading div {
			animation: none;
		}
	}
</style>
