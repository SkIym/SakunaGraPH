<script>
	let { citations = [], retrieval = null, method = null } = $props();

	const modeLabels = {
		graphrag: 'Matched records from the knowledge graph',
		legacy: 'Direct graph lookup',
		fallback: 'Backup graph lookup',
	};

	const planningLabels = { llm: 'Question interpreted' };
	const queryLabels = {
		service: 'Used a prepared lookup',
		compiler: 'Built a read-only graph lookup',
		model_fallback: 'Built a read-only lookup with AI',
		not_run: 'No graph lookup needed',
	};
	const answerLabels = {
		deterministic: 'Answer calculated from the results',
		llm: 'Answer summarized from the results',
	};

	let methodStages = $derived(
		method
			? [
					planningLabels[method.planning],
					queryLabels[method.query],
					answerLabels[method.answer],
				].filter(Boolean)
			: [],
	);

	function citationHref(uri) {
		try {
			const parsed = new URL(uri);
			return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null;
		} catch {
			return null;
		}
	}
</script>

{#if methodStages.length || (retrieval?.mode && modeLabels[retrieval.mode])}
	<details class="method-disclosure">
		<summary>How this answer was made</summary>
		<div class="method-details">
			{#if retrieval?.mode && modeLabels[retrieval.mode]}
				<div class="retrieval-ledger">
					<span aria-hidden="true"></span>
					<p>
						<strong>{modeLabels[retrieval.mode]}</strong>
						{#if retrieval.sourceCount !== undefined}
							<small>
								{retrieval.sourceCount} source{retrieval.sourceCount === 1 ? '' : 's'} checked
							</small>
						{/if}
					</p>
					{#if retrieval.indexVersion}
						<code>Data index {retrieval.indexVersion}</code>
					{/if}
				</div>
			{/if}
			{#if methodStages.length}
				<section class="method-ledger" aria-label="Answer method">
					<strong>Steps</strong>
					<ol>
						{#each methodStages as stage}
							<li>{stage}</li>
						{/each}
					</ol>
				</section>
			{/if}
		</div>
	</details>
{/if}

{#if citations?.length}
	<section class="answer-sources" aria-label="Answer sources">
		<header>
			<h2>Sources used</h2>
			<span>{String(citations.length).padStart(2, '0')}</span>
		</header>
		<ol>
			{#each citations as citation, index (citation.id)}
				<li>
					<span class="source-number">{String(index + 1).padStart(2, '0')}</span>
					<div>
						{#if citationHref(citation.uri)}
							<a href={citationHref(citation.uri)} target="_blank" rel="noreferrer">
								{citation.label}
								<i aria-hidden="true">↗</i>
								<span class="sr-only"> (opens in a new tab)</span>
							</a>
						{:else}
							<strong>{citation.label}</strong>
						{/if}
						{#if citation.excerpt}
							<p>{citation.excerpt}</p>
						{/if}
						{#if citation.sourceRecord}
							<code>{citation.sourceRecord}</code>
						{/if}
					</div>
				</li>
			{/each}
		</ol>
	</section>
{/if}

<style>
	.method-disclosure {
		border-block-start: 1px solid var(--color-border);
	}

	.method-disclosure > summary {
		min-height: 2.75rem;
		cursor: pointer;
		align-content: center;
		padding: 0.65rem 1.2rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-brand-hover);
	}

	.method-disclosure > summary:hover {
		background: var(--color-brand-soft);
	}

	.method-details {
		border-block-start: 1px solid var(--color-border);
	}

	.method-ledger {
		display: grid;
		grid-template-columns: auto minmax(0, 1fr);
		align-items: center;
		gap: 0.85rem;
		border-block-start: 1px solid var(--color-border);
		padding: 0.75rem 1.2rem;
	}

	.method-ledger > strong {
		font-size: 0.68rem;
		color: var(--color-text);
	}

	.method-ledger ol {
		display: flex;
		min-width: 0;
		align-items: center;
		flex-wrap: wrap;
		gap: 0.35rem 0;
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.method-ledger li {
		font-family: var(--font-mono);
		font-size: 0.75rem;
		line-height: 1.5;
		color: var(--color-text-secondary);
	}

	.method-ledger li + li::before {
		content: '→';
		margin-inline: 0.5rem;
		color: var(--color-brand);
	}

	.retrieval-ledger {
		display: grid;
		grid-template-columns: auto minmax(0, 1fr) auto;
		align-items: center;
		gap: 0.6rem;
		border-block-start: 1px solid var(--color-border);
		background: var(--color-brand-soft);
		padding: 0.7rem 1.2rem;
	}

	.retrieval-ledger > span {
		width: 0.55rem;
		height: 0.55rem;
		border: 2px solid var(--color-brand);
		background: var(--color-canvas);
	}

	.retrieval-ledger p {
		display: flex;
		min-width: 0;
		align-items: baseline;
		flex-wrap: wrap;
		gap: 0.25rem 0.6rem;
		margin: 0;
	}

	.retrieval-ledger strong,
	.retrieval-ledger small {
		font-size: 0.75rem;
		line-height: 1.4;
	}

	.retrieval-ledger strong {
		font-weight: 700;
		color: var(--color-brand-hover);
	}

	.retrieval-ledger small {
		color: var(--color-text-secondary);
	}

	.retrieval-ledger code {
		font-family: var(--font-mono);
		font-size: 0.75rem;
		color: var(--color-text-secondary);
	}

	.answer-sources {
		border-block-start: 1px solid var(--color-border);
	}

	.answer-sources header {
		display: flex;
		min-height: 2.75rem;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		padding: 0.7rem 1.2rem;
	}

	.answer-sources h2 {
		margin: 0;
		font-size: 0.72rem;
		font-weight: 700;
		color: var(--color-text);
	}

	.answer-sources header span,
	.source-number {
		font-family: var(--font-mono);
		font-size: 0.58rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.answer-sources ol {
		margin: 0;
		padding: 0;
		border-block-start: 1px solid var(--color-border);
		list-style: none;
	}

	.answer-sources li {
		display: grid;
		grid-template-columns: auto minmax(0, 1fr);
		gap: 0.7rem;
		padding: 0.85rem 1.2rem 0.95rem;
	}

	.answer-sources li + li {
		border-block-start: 1px solid var(--color-border);
	}

	.answer-sources a,
	.answer-sources li > div > strong {
		font-size: 0.75rem;
		font-weight: 700;
		line-height: 1.4;
	}

	.answer-sources a {
		color: var(--color-brand);
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.2rem;
	}

	.answer-sources a:hover {
		color: var(--color-brand-hover);
		text-decoration-color: currentColor;
	}

	.answer-sources i {
		margin-inline-start: 0.2rem;
		font-style: normal;
	}

	.answer-sources p {
		margin: 0.3rem 0 0;
		font-size: 0.7rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.answer-sources li code {
		display: block;
		margin-top: 0.35rem;
		font-family: var(--font-mono);
		font-size: 0.58rem;
		line-height: 1.5;
		overflow-wrap: anywhere;
		color: var(--color-text-muted);
	}

	@media (max-width: 30rem) {
		.method-ledger {
			grid-template-columns: 1fr;
			gap: 0.35rem;
		}

		.retrieval-ledger {
			grid-template-columns: auto minmax(0, 1fr);
		}

		.retrieval-ledger code {
			grid-column: 2;
		}
	}
</style>
