<script>
	import { getDisasterDetails } from '$lib/api/disasters.js';
	import { focusTrap } from '../actions/focus.js';

	let { event = '', onclose = () => {} } = $props();

	let details = $state(null);
	let loading = $state(true);
	let error = $state('');
	let retryToken = $state(0);
	let locationsExpanded = $state(false);
	let incidentsExpanded = $state(false);
	let expandedForEvent = $state('');
	const requestVersion = $derived({ event, retry: retryToken });
	const dateFormat = new Intl.DateTimeFormat('en-PH', {
		year: 'numeric',
		month: 'long',
		day: 'numeric',
	});

	function detailsErrorMessage(requestError) {
		if (requestError?.kind === 'network') {
			return 'Could not reach the data service. Check your connection and try again.';
		}
		if (requestError?.kind === 'timeout') {
			return 'Event details took too long to load. Try again.';
		}
		if (requestError?.status === 404)
			return 'This event is no longer available in the current graph.';
		if (requestError?.status === 429)
			return 'Too many requests reached the data service. Wait a moment and try again.';
		if (requestError?.status >= 500)
			return 'Event details are temporarily unavailable. Try again shortly.';
		return 'Could not load event details. Try again.';
	}

	function normalizeDetails(data) {
		return {
			...data,
			remarks: data?.remarks ?? [],
			locations: data?.locations ?? [],
			disasterTypes: data?.disasterTypes ?? [],
			majorEvents: data?.majorEvents ?? [],
			incidents: data?.incidents ?? [],
			alternates: data?.alternates ?? [],
			sources: (data?.sources ?? []).map((source) => ({
				...source,
				attributedTo: source.attributedTo ?? [],
			})),
		};
	}

	$effect(() => {
		if (event && event !== expandedForEvent) {
			expandedForEvent = event;
			locationsExpanded = false;
			incidentsExpanded = false;
		}
	});

	$effect(() => {
		const request = requestVersion;
		if (!request.event) return;
		const controller = new AbortController();
		loading = true;
		error = '';
		details = null;

		void getDisasterDetails(request.event, {
			signal: controller.signal,
		})
			.then((data) => (details = normalizeDetails(data)))
			.catch((requestError) => {
				if (requestError?.name !== 'AbortError') {
					error = detailsErrorMessage(requestError);
				}
			})
			.finally(() => {
				if (!controller.signal.aborted) loading = false;
			});

		return () => controller.abort();
	});

	function formatDate(value) {
		if (!value) return 'Not recorded';
		const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
		return Number.isNaN(parsed.valueOf()) ? value : dateFormat.format(parsed);
	}

	function safeReportLink(value) {
		if (!value) return null;
		try {
			const url = new URL(value);
			return ['http:', 'https:'].includes(url.protocol) ? url.href : null;
		} catch {
			return null;
		}
	}

	function handleKeydown(keyboardEvent) {
		if (keyboardEvent.key === 'Escape') onclose();
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<button
	type="button"
	onclick={onclose}
	class="event-backdrop"
	tabindex="-1"
	aria-label="Close event details"
></button>

<div
	use:focusTrap
	role="dialog"
	aria-modal="true"
	aria-labelledby="event-details-title"
	aria-describedby="event-details-context"
	class="event-sheet"
>
	<header class="sheet-header">
		<div class="sheet-toolbar">
			<p class="dossier-label"><span aria-hidden="true"></span> Event dossier</p>
			<button
				type="button"
				onclick={onclose}
				data-focus-first
				class="close-button touch-target"
				aria-label="Close event details"
			>
				<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
					<path d="M6 6l12 12M18 6 6 18" />
				</svg>
			</button>
		</div>

		<div class="title-block">
			<h2 id="event-details-title" dir="auto">
				{details?.name ?? (loading ? 'Loading event…' : 'Event details')}
			</h2>
			<p id="event-details-context">Knowledge graph event record</p>
			{#if details}
				<div class="record-meta">
					<span class="record-type">
						{details.eventType === 'MajorEvent' ? 'Major event' : 'Incident'}
					</span>
					<span class="record-state"><span aria-hidden="true"></span> Selected record</span>
				</div>
			{/if}
		</div>
	</header>

	<div class="sheet-scroll">
		{#if loading}
			<div class="loading-state" role="status" aria-label="Loading event details">
				<p class="sr-only">Loading event details</p>
				<div class="skeleton-section skeleton-section--lead animate-pulse">
					<span></span><span></span><span></span>
				</div>
				{#each [1, 2, 3] as section}
					<div class="skeleton-section animate-pulse" aria-hidden="true">
						<span></span><span></span>
						<div style="width:{88 - section * 8}%"></div>
					</div>
				{/each}
			</div>
		{:else if error}
			<div class="error-state" role="alert">
				<div class="error-mark" aria-hidden="true">!</div>
				<div>
					<p class="error-title">Event details are unavailable</p>
					<p class="error-copy">{error}</p>
				</div>
				<button type="button" onclick={() => (retryToken += 1)} class="retry-button touch-target"
					>Try again</button
				>
			</div>
		{:else if details}
			<div class="dossier-body">
				<section class="dossier-section temporal-section" aria-labelledby="temporal-heading">
					<div class="section-heading">
						<span class="section-number">01</span>
						<div>
							<p class="section-kicker">Recorded timeframe</p>
							<h3 id="temporal-heading">Temporal extent</h3>
						</div>
					</div>

					<dl class="date-rail">
						<div>
							<dt>Start date</dt>
							<dd>{formatDate(details.startDate)}</dd>
						</div>
						<div>
							<dt>End date</dt>
							<dd>{formatDate(details.endDate)}</dd>
						</div>
					</dl>

					<div class="identifier-block">
						<span>Graph identifier</span>
						<code>{details.event}</code>
					</div>
				</section>

				<section class="dossier-section" aria-labelledby="notes-heading">
					<div class="section-heading">
						<span class="section-number">02</span>
						<div>
							<p class="section-kicker">Narrative evidence</p>
							<h3 id="notes-heading">Record notes</h3>
						</div>
					</div>
					{#if details.remarks.length}
						<div class="remarks-list">
							{#each details.remarks as remark, index (`${remark}-${index}`)}
								<p>{remark}</p>
							{/each}
						</div>
					{:else}
						<p class="empty-copy">No remarks were recorded for this event.</p>
					{/if}
				</section>

				<section class="dossier-section" aria-labelledby="classification-heading">
					<div class="section-heading">
						<span class="section-number">03</span>
						<div>
							<p class="section-kicker">Graph classification</p>
							<h3 id="classification-heading">Classification &amp; geography</h3>
						</div>
					</div>

					<div class="classification-ledger">
						<div class="classification-row">
							<div class="classification-label">
								<h4>Disaster type</h4>
								<p>Ontology classification</p>
							</div>
							<div class="classification-value">
								{#if details.disasterTypes.length}
									<div class="tag-list">
										{#each details.disasterTypes as disasterType (disasterType.uri)}
											<span class="data-tag data-tag--blue" dir="auto" title={disasterType.id}
												>{disasterType.label}</span
											>
										{/each}
									</div>
								{:else}
									<p class="empty-copy">No disaster type was recorded.</p>
								{/if}
							</div>
						</div>

						<div class="disclosure-block geography-disclosure">
							<button
								type="button"
								onclick={() => (locationsExpanded = !locationsExpanded)}
								aria-expanded={locationsExpanded}
								aria-controls="event-locations"
								class="disclosure-control touch-target"
							>
								<span class="classification-label">
									<span>Locations affected</span>
									<small>
										{details.locations.length}
										{details.locations.length === 1 ? 'place recorded' : 'places recorded'}
									</small>
								</span>
								<span
									aria-hidden="true"
									class="disclosure-chevron {locationsExpanded ? 'rotate-180' : ''}"
								>
									<svg viewBox="0 0 24 24" fill="none">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</span>
							</button>
							{#if locationsExpanded}
								<div id="event-locations" class="disclosure-content">
									{#if details.locations.length}
										<div class="tag-list">
											{#each details.locations as location (location.uri)}
												<span class="data-tag" dir="auto" title={location.id}>{location.label}</span
												>
											{/each}
										</div>
									{:else}
										<p class="empty-copy">No locations were recorded.</p>
									{/if}
								</div>
							{/if}
						</div>
					</div>
				</section>

				<section class="dossier-section evidence-section" aria-labelledby="evidence-heading">
					<div class="section-heading">
						<span class="section-number">04</span>
						<div>
							<p class="section-kicker">Evidence &amp; provenance</p>
							<h3 id="evidence-heading">Source records</h3>
						</div>
					</div>
					{#if details.sources.length}
						<div class="source-list">
							{#each details.sources as source, index (source.uri)}
								{@const reportLink = safeReportLink(source.reportLink)}
								<article class="source-record">
									<div class="source-record-head">
										<span>Evidence {String(index + 1).padStart(2, '0')}</span>
										{#if source.format}<b>{source.format}</b>{/if}
									</div>
									<h4>{source.reportName}</h4>
									{#if source.attributedTo.length}
										<p class="source-attribution">
											Published by {source.attributedTo.map((item) => item.label).join(', ')}
										</p>
									{/if}
									<dl class="source-dates">
										<div>
											<dt>Record obtained</dt>
											<dd>{formatDate(source.obtainedDate)}</dd>
										</div>
										<div>
											<dt>Last updated</dt>
											<dd>{formatDate(source.lastUpdateDate)}</dd>
										</div>
									</dl>
									{#if reportLink}
										<a
											href={reportLink}
											target="_blank"
											rel="noreferrer"
											class="source-link touch-target"
											>Open source report
											<svg aria-hidden="true" viewBox="0 0 24 24" fill="none">
												<path d="M7 17 17 7M8 7h9v9" />
											</svg></a
										>
									{/if}
								</article>
							{/each}
						</div>
					{:else}
						<div class="verification-note">
							<span aria-hidden="true">i</span>
							<div>
								<p>No linked source record</p>
								<small>
									Treat this event’s details as unverified in this interface until provenance is
									linked.
								</small>
							</div>
						</div>
					{/if}
				</section>

				<section class="dossier-section relationships-section" aria-labelledby="relations-heading">
					<div class="section-heading">
						<span class="section-number">05</span>
						<div>
							<p class="section-kicker">Knowledge graph</p>
							<h3 id="relations-heading">Related records</h3>
						</div>
					</div>

					{#if details.eventType === 'Incident'}
						<div class="relation-group">
							<h4>Linked major event <span>{details.majorEvents.length}</span></h4>
							{#if details.majorEvents.length}
								<div class="related-list">
									{#each details.majorEvents as related (related.uri)}
										<div class="related-record">
											<span aria-hidden="true"></span>
											<p dir="auto">{related.name}</p>
											<time>{formatDate(related.startDate)}</time>
										</div>
									{/each}
								</div>
							{:else}
								<p class="empty-copy">No linked major event is recorded in the current graph.</p>
							{/if}
						</div>
					{:else}
						<div class="disclosure-block">
							<button
								type="button"
								onclick={() => (incidentsExpanded = !incidentsExpanded)}
								aria-expanded={incidentsExpanded}
								aria-controls="event-incidents"
								class="disclosure-control touch-target"
							>
								<span>Linked incidents <b>{details.incidents.length}</b></span>
								<span
									aria-hidden="true"
									class="disclosure-chevron {incidentsExpanded ? 'rotate-180' : ''}"
								>
									<svg viewBox="0 0 24 24" fill="none">
										<path d="m6 9 6 6 6-6" />
									</svg>
								</span>
							</button>
							{#if incidentsExpanded}
								<div id="event-incidents" class="disclosure-content">
									{#if details.incidents.length}
										<div class="related-list">
											{#each details.incidents as related (related.uri)}
												<div class="related-record">
													<span aria-hidden="true"></span>
													<p dir="auto">{related.name}</p>
													<time>{formatDate(related.startDate)}</time>
												</div>
											{/each}
										</div>
									{:else}
										<p class="empty-copy">No linked incidents are recorded in the current graph.</p>
									{/if}
								</div>
							{/if}
						</div>
					{/if}

					<div class="relation-group alternate-group">
						<h4>Alternate event records <span>{details.alternates.length}</span></h4>
						{#if details.alternates.length}
							<div class="related-list">
								{#each details.alternates as alternate (alternate.uri)}
									<div class="related-record">
										<span aria-hidden="true"></span>
										<p>{alternate.name}</p>
										<div class="related-meta">
											<time>{formatDate(alternate.startDate)}</time>
											{#if alternate.eventType}<b
													>{alternate.eventType === 'MajorEvent' ? 'Major event' : 'Incident'}</b
												>{/if}
										</div>
									</div>
								{/each}
							</div>
						{:else}
							<p class="empty-copy">No alternate event records were recorded.</p>
						{/if}
					</div>
				</section>
			</div>
		{/if}
	</div>
</div>

<style>
	.event-backdrop {
		position: fixed;
		inset: 0;
		z-index: 50;
		cursor: default;
		border: 0;
		background: rgba(15, 23, 42, 0.42);
		animation: backdrop-in 160ms ease-out both;
	}

	.event-sheet {
		position: fixed;
		inset-block: 0;
		right: 0;
		z-index: 60;
		display: flex;
		width: min(100vw, 42rem);
		flex-direction: column;
		border-left: 1px solid var(--color-border);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
		animation: sheet-in 220ms cubic-bezier(0.2, 0.8, 0.2, 1) both;
	}

	.sheet-header {
		position: relative;
		z-index: 2;
		flex: none;
		border-bottom: 1px solid var(--color-border);
		background:
			linear-gradient(90deg, var(--color-accent) 0 0) top left / 5rem 3px no-repeat,
			var(--color-canvas);
		padding: 1rem 1.5rem 1.5rem;
	}

	.sheet-toolbar {
		display: flex;
		min-height: 2.75rem;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
	}

	.dossier-label,
	.section-kicker,
	.identifier-block > span,
	.relation-group h4,
	.source-record-head {
		margin: 0;
		font-size: 0.6875rem;
		font-weight: 700;
		line-height: 1rem;
		letter-spacing: 0.12em;
		text-transform: uppercase;
	}

	.dossier-label {
		display: flex;
		align-items: center;
		gap: 0.5rem;
		color: var(--color-brand);
	}

	.dossier-label > span {
		width: 0.5rem;
		height: 0.5rem;
		border: 2px solid var(--color-brand);
		transform: rotate(45deg);
	}

	.close-button {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		border: 1px solid transparent;
		border-radius: var(--radius-control);
		background: transparent;
		color: var(--color-text-secondary);
		transition:
			border-color 150ms ease,
			background 150ms ease,
			color 150ms ease;
	}

	.close-button svg {
		width: 1.25rem;
		height: 1.25rem;
	}

	.close-button path,
	.source-link path,
	.disclosure-chevron path {
		stroke: currentColor;
		stroke-width: 1.8;
		stroke-linecap: round;
		stroke-linejoin: round;
	}

	.title-block {
		max-width: 36rem;
		padding: 0.75rem 2.75rem 0 0;
	}

	.title-block h2 {
		margin: 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2rem, 4vw, 2.75rem);
		font-weight: 900;
		line-height: 1.05;
		letter-spacing: -0.025em;
		color: var(--color-text);
		overflow-wrap: anywhere;
		text-wrap: balance;
	}

	#event-details-context {
		margin: 0.625rem 0 0;
		font-size: 0.8125rem;
		line-height: 1.4;
		color: var(--color-text-muted);
	}

	.record-meta {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
		margin-top: 1rem;
	}

	.record-type,
	.record-state {
		display: inline-flex;
		min-height: 1.75rem;
		align-items: center;
		gap: 0.4rem;
		border-radius: 9999px;
		padding: 0.25rem 0.75rem;
		font-size: 0.6875rem;
		font-weight: 700;
		line-height: 1.25;
	}

	.record-type {
		border: 1px solid var(--color-brand-medium);
		background: var(--color-brand-soft);
		color: var(--color-brand);
	}

	.record-state {
		border: 1px solid #ead486;
		background: var(--color-accent-soft);
		color: var(--color-accent-ink);
	}

	.record-state > span {
		width: 0.375rem;
		height: 0.375rem;
		border-radius: 50%;
		background: #b99200;
	}

	.sheet-scroll {
		min-height: 0;
		flex: 1;
		overflow-y: auto;
		overscroll-behavior: contain;
		scrollbar-color: var(--color-brand) var(--color-surface-subtle);
	}

	.dossier-body {
		padding-bottom: max(1.5rem, env(safe-area-inset-bottom));
	}

	.dossier-section {
		border-bottom: 1px solid var(--color-border);
		padding: 2rem 1.5rem;
	}

	.dossier-section:last-child {
		border-bottom: 0;
	}

	.section-heading {
		display: grid;
		grid-template-columns: 2.5rem 1fr;
		align-items: start;
		gap: 0.75rem;
		margin-bottom: 1.5rem;
	}

	.section-number {
		padding-top: 0.125rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 600;
		line-height: 1.25rem;
		color: var(--color-brand);
	}

	.section-kicker {
		color: var(--color-text-muted);
	}

	.section-heading h3 {
		margin: 0.2rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.5rem;
		font-weight: 900;
		line-height: 1.2;
		letter-spacing: -0.015em;
		color: var(--color-text);
	}

	.date-rail {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		column-gap: 3rem;
		align-items: start;
		margin: 0;
		padding-left: 3.25rem;
	}

	.date-rail dt,
	.source-dates dt {
		font-size: 0.6875rem;
		font-weight: 700;
		line-height: 1rem;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text-muted);
	}

	.date-rail dd {
		margin: 0.35rem 0 0;
		font-size: 0.9375rem;
		font-weight: 650;
		line-height: 1.4;
		color: var(--color-text);
		font-variant-numeric: tabular-nums;
	}

	.identifier-block {
		margin: 1.5rem 0 0 3.25rem;
		border-left: 2px solid var(--color-brand-medium);
		padding: 0.125rem 0 0.125rem 0.875rem;
	}

	.identifier-block > span {
		display: block;
		color: var(--color-text-muted);
	}

	.identifier-block code {
		display: block;
		margin-top: 0.375rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		line-height: 1.6;
		color: var(--color-text-secondary);
		overflow-wrap: anywhere;
	}

	.remarks-list {
		display: grid;
		gap: 0.75rem;
		margin-left: 3.25rem;
	}

	.remarks-list p {
		margin: 0;
		border-left: 3px solid var(--color-brand);
		background: var(--color-surface-subtle);
		padding: 1rem 1.125rem;
		font-size: 0.875rem;
		line-height: 1.7;
		white-space: pre-wrap;
		color: var(--color-text-secondary);
		overflow-wrap: anywhere;
	}

	.empty-copy {
		margin: 0.5rem 0 0 3.25rem;
		font-size: 0.8125rem;
		line-height: 1.6;
		color: var(--color-text-muted);
	}

	.classification-ledger {
		margin-left: 3.25rem;
		border-top: 1px solid var(--color-border);
		border-bottom: 1px solid var(--color-border);
	}

	.classification-row {
		display: grid;
		grid-template-columns: minmax(8rem, 0.85fr) minmax(0, 1.15fr);
		align-items: center;
		gap: 1rem;
		padding: 1rem 0;
	}

	.classification-label h4,
	.classification-label > span,
	.relation-group h4 {
		margin: 0;
		font-size: 0.75rem;
		font-weight: 700;
		line-height: 1.4;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-text-secondary);
	}

	.classification-label p,
	.classification-label small {
		display: block;
		margin: 0.2rem 0 0;
		font-size: 0.6875rem;
		font-weight: 500;
		line-height: 1.45;
		letter-spacing: 0;
		text-transform: none;
		color: var(--color-text-muted);
	}

	.classification-value .tag-list {
		justify-content: flex-end;
		margin-top: 0;
	}

	.classification-value .empty-copy {
		margin: 0;
		text-align: right;
	}

	.tag-list {
		display: flex;
		flex-wrap: wrap;
		gap: 0.5rem;
		margin-top: 0.75rem;
	}

	.data-tag {
		display: inline-flex;
		max-width: 100%;
		min-height: 2rem;
		align-items: center;
		border: 1px solid var(--color-border);
		border-radius: 9999px;
		background: var(--color-canvas);
		padding: 0.35rem 0.75rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		line-height: 1.35;
		color: var(--color-text-secondary);
		overflow-wrap: anywhere;
	}

	.data-tag--blue {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
		font-family: 'Inter', system-ui, sans-serif;
		font-weight: 700;
		color: var(--color-brand);
	}

	.disclosure-block {
		margin-top: 1.5rem;
		border-top: 1px solid var(--color-border);
	}

	.geography-disclosure {
		margin-top: 0;
	}

	.geography-disclosure .disclosure-control {
		padding-block: 1rem;
	}

	.geography-disclosure .disclosure-control > .classification-label {
		font-size: inherit;
		letter-spacing: 0;
		text-transform: none;
	}

	.geography-disclosure .disclosure-content {
		border-top: 1px solid var(--color-border);
		padding: 0.875rem 0 1rem;
	}

	.geography-disclosure .disclosure-content .tag-list {
		margin-top: 0;
	}

	.disclosure-control {
		display: flex;
		width: 100%;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		border: 0;
		border-radius: 0;
		background: transparent;
		padding: 0.875rem 0;
		font: inherit;
		text-align: left;
		color: var(--color-text-secondary);
		transition: color 150ms ease;
	}

	.disclosure-control > span:first-child {
		font-size: 0.75rem;
		font-weight: 700;
		line-height: 1.4;
		letter-spacing: 0.08em;
		text-transform: uppercase;
	}

	.disclosure-control b,
	.relation-group h4 span {
		display: inline-flex;
		min-width: 1.5rem;
		height: 1.5rem;
		align-items: center;
		justify-content: center;
		margin-left: 0.35rem;
		border-radius: 9999px;
		background: var(--color-brand-soft);
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		letter-spacing: 0;
		color: var(--color-brand);
	}

	.disclosure-chevron {
		display: inline-flex;
		width: 2rem;
		height: 2rem;
		flex: none;
		align-items: center;
		justify-content: center;
		border-radius: 50%;
		background: var(--color-surface-subtle);
		color: var(--color-brand);
		transition:
			transform 180ms ease,
			background 180ms ease;
	}

	.disclosure-chevron svg {
		width: 1rem;
		height: 1rem;
	}

	.disclosure-chevron.rotate-180 {
		transform: rotate(180deg);
		background: var(--color-brand-soft);
	}

	.disclosure-content {
		padding: 0.25rem 0 0.75rem;
	}

	.disclosure-content .empty-copy,
	.relation-group .empty-copy {
		margin-left: 0;
	}

	.evidence-section {
		background: var(--color-surface-subtle);
	}

	.source-list {
		display: grid;
		gap: 1rem;
		margin-left: 3.25rem;
	}

	.source-record {
		border: 1px solid var(--color-border);
		border-left: 3px solid var(--color-brand);
		border-radius: 0 var(--radius-control) var(--radius-control) 0;
		background: var(--color-canvas);
		padding: 1rem 1.125rem;
		box-shadow: var(--shadow-control);
	}

	.source-record-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		color: var(--color-brand);
	}

	.source-record-head b {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.625rem;
		color: var(--color-text-muted);
	}

	.source-record h4 {
		margin: 0.75rem 0 0;
		font-size: 0.9375rem;
		font-weight: 700;
		line-height: 1.45;
		color: var(--color-text);
		overflow-wrap: anywhere;
	}

	.source-attribution {
		margin: 0.35rem 0 0;
		font-size: 0.75rem;
		line-height: 1.5;
		color: var(--color-text-secondary);
	}

	.source-dates {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1rem;
		margin: 1rem 0 0;
		border-top: 1px solid var(--color-border);
		padding-top: 0.875rem;
	}

	.source-dates dd {
		margin: 0.25rem 0 0;
		font-size: 0.75rem;
		line-height: 1.4;
		color: var(--color-text-secondary);
	}

	.source-link {
		display: inline-flex;
		align-items: center;
		gap: 0.4rem;
		margin-top: 0.625rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-brand);
		text-decoration: underline;
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.25rem;
	}

	.source-link svg {
		width: 1rem;
		height: 1rem;
		flex: none;
	}

	.verification-note {
		display: grid;
		grid-template-columns: 2rem minmax(0, 1fr);
		gap: 0.875rem;
		margin-left: 3.25rem;
		border: 1px solid #ead486;
		border-radius: var(--radius-control);
		background: var(--color-accent-soft);
		padding: 1rem;
		color: var(--color-accent-ink);
	}

	.verification-note > span {
		display: flex;
		width: 2rem;
		height: 2rem;
		align-items: center;
		justify-content: center;
		border: 1px solid #caa600;
		border-radius: 50%;
		font-family: 'Playfair Display', Georgia, serif;
		font-weight: 900;
	}

	.verification-note p {
		margin: 0;
		font-size: 0.8125rem;
		font-weight: 700;
		line-height: 1.4;
	}

	.verification-note small {
		display: block;
		margin-top: 0.25rem;
		font-size: 0.75rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	.relationships-section .disclosure-block {
		margin-left: 3.25rem;
		margin-top: 0;
	}

	.relation-group {
		margin-left: 3.25rem;
	}

	.relation-group h4 {
		display: flex;
		align-items: center;
	}

	.alternate-group {
		margin-top: 1.5rem;
		border-top: 1px solid var(--color-border);
		padding-top: 1.5rem;
	}

	.related-list {
		display: grid;
		gap: 0;
		margin-top: 0.75rem;
		border-top: 1px solid var(--color-border);
	}

	.related-record {
		display: grid;
		grid-template-columns: 0.5rem minmax(0, 1fr) auto;
		align-items: center;
		gap: 0.75rem;
		border-bottom: 1px solid var(--color-border);
		padding: 0.875rem 0;
	}

	.related-record > span {
		width: 0.375rem;
		height: 0.375rem;
		border-radius: 50%;
		background: var(--color-brand);
	}

	.related-record > p {
		margin: 0;
		font-size: 0.8125rem;
		font-weight: 650;
		line-height: 1.5;
		color: var(--color-text);
		overflow-wrap: anywhere;
	}

	.related-record time,
	.related-meta b {
		font-size: 0.6875rem;
		line-height: 1.4;
		color: var(--color-text-muted);
	}

	.related-meta {
		display: flex;
		flex-direction: column;
		align-items: flex-end;
		gap: 0.2rem;
	}

	.related-meta b {
		color: var(--color-brand);
	}

	.loading-state {
		padding: 2rem 1.5rem;
	}

	.skeleton-section {
		display: grid;
		gap: 0.75rem;
		border-bottom: 1px solid var(--color-border);
		padding: 1.75rem 0;
	}

	.skeleton-section span,
	.skeleton-section div {
		display: block;
		height: 0.75rem;
		border-radius: 9999px;
		background: var(--color-research-surface);
	}

	.skeleton-section span:first-child {
		width: 4rem;
		background: var(--color-brand-soft);
	}

	.skeleton-section span:nth-child(2) {
		width: 11rem;
		height: 1.5rem;
	}

	.skeleton-section--lead span:nth-child(3) {
		width: 75%;
	}

	.error-state {
		display: grid;
		grid-template-columns: 2.5rem minmax(0, 1fr);
		gap: 0.875rem;
		margin: 1.5rem;
		border: 1px solid var(--color-danger-border);
		border-radius: var(--radius-surface);
		background: var(--color-danger-surface);
		padding: 1rem;
	}

	.error-mark {
		display: flex;
		width: 2.5rem;
		height: 2.5rem;
		align-items: center;
		justify-content: center;
		border-radius: 50%;
		background: var(--color-danger);
		font-weight: 800;
		color: white;
	}

	.error-title,
	.error-copy {
		margin: 0;
	}

	.error-title {
		font-size: 0.875rem;
		font-weight: 700;
		color: #881326;
	}

	.error-copy {
		margin-top: 0.25rem;
		font-size: 0.8125rem;
		line-height: 1.55;
		color: #9f1239;
	}

	.retry-button {
		grid-column: 2;
		justify-self: start;
		border: 1px solid var(--color-danger);
		border-radius: var(--radius-control);
		background: var(--color-danger);
		padding: 0.5rem 1rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: white;
	}

	@media (hover: hover) {
		.close-button:hover {
			border-color: var(--color-border);
			background: var(--color-surface-subtle);
			color: var(--color-text);
		}

		.disclosure-control:hover,
		.source-link:hover {
			color: var(--color-brand-hover);
		}

		.retry-button:hover {
			background: var(--color-action-hover);
		}
	}

	@media (max-width: 639px) {
		.event-sheet {
			border-left: 0;
		}

		.sheet-header {
			padding: 0.75rem 1.25rem 1.25rem;
		}

		.title-block {
			padding-top: 0.5rem;
		}

		.title-block h2 {
			font-size: 2rem;
		}

		.dossier-section {
			padding: 1.75rem 1.25rem;
		}

		.section-heading {
			grid-template-columns: 2rem 1fr;
			gap: 0.5rem;
			margin-bottom: 1.25rem;
		}

		.section-heading h3 {
			font-size: 1.35rem;
		}

		.date-rail,
		.identifier-block,
		.remarks-list,
		.classification-ledger,
		.source-list,
		.verification-note,
		.relationships-section .disclosure-block,
		.relation-group {
			margin-left: 0;
		}

		.date-rail {
			grid-template-columns: repeat(2, minmax(0, 1fr));
			column-gap: 2rem;
			padding-left: 0;
		}

		.date-rail dd {
			font-size: 0.8125rem;
		}

		.identifier-block {
			margin-top: 1.25rem;
		}

		.related-record {
			grid-template-columns: 0.5rem minmax(0, 1fr);
		}

		.related-record > time,
		.related-meta {
			grid-column: 2;
			align-items: flex-start;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.event-backdrop,
		.event-sheet {
			animation: none;
		}

		.disclosure-chevron {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.event-backdrop {
			background: CanvasText;
			opacity: 0.45;
		}

		.event-sheet,
		.source-record,
		.verification-note,
		.data-tag,
		.record-type,
		.record-state {
			border-color: CanvasText;
		}

		.dossier-label > span,
		.record-state > span,
		.related-record > span {
			background: Highlight;
		}
	}

	@keyframes backdrop-in {
		from {
			opacity: 0;
		}
		to {
			opacity: 1;
		}
	}

	@keyframes sheet-in {
		from {
			transform: translateX(1.5rem);
		}
		to {
			transform: translateX(0);
		}
	}
</style>
