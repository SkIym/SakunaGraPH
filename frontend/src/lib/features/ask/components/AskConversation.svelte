<script>
	import AskAnswerMeta from './AskAnswerMeta.svelte';

	let {
		messages = [],
		suggestions = [],
		announcement = '',
		onSend = () => {},
		bottomElement = $bindable(null),
	} = $props();
</script>

<div class="ask-scroll" id="messages-scroll">
	<p class="sr-only" role="status" aria-live="polite" aria-atomic="true">{announcement}</p>
	<div class:empty={messages.length === 0} class="conversation-column">
		{#if messages.length === 0}
			<section class="ask-intro" aria-labelledby="ask-title">
				<div class="intro-copy">
					<p class="workspace-kicker">Graph research assistant</p>
					<h1 id="ask-title">
						Ask SakunaGraPH.
						<em>Trace the evidence.</em>
					</h1>
					<p class="intro-description">
						Question Philippine disaster records in plain language. Every answer keeps its matched
						rows, query, and sources within reach.
					</p>
					<dl class="answer-contract">
						<div>
							<dt>Scope</dt>
							<dd>Events, places, dates, impacts, and source records</dd>
						</div>
						<div>
							<dt>Returns</dt>
							<dd>Answer, graph query, result rows, and provenance</dd>
						</div>
					</dl>
				</div>

				<aside class="prompt-ledger" aria-labelledby="prompt-ledger-title">
					<header>
						<div>
							<p class="workspace-kicker">Starting points</p>
							<h2 id="prompt-ledger-title">Questions for the graph</h2>
						</div>
						<span>{String(suggestions.length).padStart(2, '0')} prompts</span>
					</header>
					<ol>
						{#each suggestions as suggestion, index}
							<li>
								<button type="button" onclick={() => onSend(suggestion)}>
									<span>{String(index + 1).padStart(2, '0')}</span>
									<strong>{suggestion}</strong>
									<i aria-hidden="true">→</i>
								</button>
							</li>
						{/each}
					</ol>
				</aside>
			</section>
		{:else}
			<header class="session-header">
				<div>
					<p class="workspace-kicker">Evidence session</p>
					<h1>Ask SakunaGraPH</h1>
				</div>
				<span>
					{Math.ceil(messages.length / 2).toLocaleString()} question{messages.length > 2 ? 's' : ''}
				</span>
			</header>

			<div class="message-list">
				{#each messages as message, index}
					{#if message.role === 'user'}
						<article class="message-row user-message" aria-label="Your question">
							<div class="user-bubble" dir="auto">
								<span>You</span>
								<p>{message.text}</p>
							</div>
						</article>
					{:else}
						<article class="message-row answer-message" aria-label="Graph answer">
							<div class="answer-record">
								<header class="answer-record-header">
									<strong>Graph answer</strong>
								</header>

								{#if message.loading}
									<div class="answer-status">
										<span class="status-trace" aria-hidden="true"><i></i></span>
										<p>Checking the knowledge graph…</p>
									</div>
								{:else if message.error}
									<div class="answer-error" role="alert">
										<strong>Answer unavailable</strong>
										<p>{message.error}</p>
										{#if messages[index - 1]?.role === 'user'}
											<button type="button" onclick={() => onSend(messages[index - 1].text)}>
												Try again
											</button>
										{/if}
									</div>
								{:else}
									{#if message.text}
										<p class="answer-copy" dir="auto">{message.text}</p>
									{/if}

									{#if message.streaming}
										<div class="stream-status">
											<span aria-hidden="true"></span>
											Writing an answer from the matched records…
										</div>
									{:else if message.cancelled}
										<div class="cancelled-status">Answer stopped.</div>
									{/if}

									<AskAnswerMeta citations={message.citations} retrieval={message.retrieval} />

									{#if message.sparql}
										<details class="evidence-disclosure">
											<summary>
												<svg
													viewBox="0 0 24 24"
													width="11"
													height="11"
													fill="none"
													stroke="currentColor"
													stroke-width="2.5"
													class="chevron"
													aria-hidden="true"><path d="m9 18 6-6-6-6" /></svg
												>
												Query used
											</summary>
											<pre>{message.sparql}</pre>
										</details>
									{/if}

									{#if message.rows?.length > 0}
										{@const columns = Object.keys(message.rows[0])}
										<details class="evidence-disclosure">
											<summary>
												<svg
													viewBox="0 0 24 24"
													width="11"
													height="11"
													fill="none"
													stroke="currentColor"
													stroke-width="2.5"
													class="chevron"
													aria-hidden="true"><path d="m9 18 6-6-6-6" /></svg
												>
												Results · {message.rows.length} row{message.rows.length === 1 ? '' : 's'}
											</summary>
											<div class="results-table-wrap">
												<table>
													<thead>
														<tr>
															{#each columns as column}
																<th>{column}</th>
															{/each}
														</tr>
													</thead>
													<tbody>
														{#each message.rows as row, rowIndex}
															<tr class:alternate={rowIndex % 2 === 1}>
																{#each columns as column}
																	<td title={row[column] ?? ''}>{row[column] ?? '—'}</td>
																{/each}
															</tr>
														{/each}
													</tbody>
												</table>
											</div>
										</details>
									{:else if message.rows}
										<div class="no-records">
											No records matched this question. Try a broader place, date range, or disaster
											type.
										</div>
									{/if}
								{/if}
							</div>
						</article>
					{/if}
				{/each}
			</div>
		{/if}

		<div bind:this={bottomElement}></div>
	</div>
</div>

<style>
	.ask-scroll {
		min-height: 0;
		flex: 1;
		overflow-y: auto;
		overscroll-behavior: contain;
		padding: clamp(1.5rem, 4vw, 3.5rem) clamp(1rem, 4vw, 2.5rem) 2.5rem;
		scrollbar-gutter: stable;
	}

	.conversation-column {
		width: min(100%, 52rem);
		margin-inline: auto;
	}

	.conversation-column.empty {
		display: grid;
		width: min(100%, 66rem);
		min-height: 100%;
		align-items: center;
	}

	.ask-intro {
		display: grid;
		grid-template-columns: minmax(0, 0.9fr) minmax(20rem, 1.1fr);
		align-items: center;
		gap: clamp(2.5rem, 7vw, 5.5rem);
		padding-block: 1rem 3rem;
	}

	.intro-copy h1 {
		max-width: 11ch;
		margin: 0.75rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(3rem, 6vw, 5rem);
		font-weight: 900;
		line-height: 0.98;
		letter-spacing: -0.055em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.intro-copy h1 em {
		display: block;
		margin-top: 0.2em;
		font-weight: 700;
		color: var(--color-brand);
	}

	.intro-description {
		max-width: 36rem;
		margin: 1.5rem 0 0;
		font-size: 0.875rem;
		line-height: 1.75;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.answer-contract {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 1px;
		margin: 2rem 0 0;
		border-block: 1px solid var(--color-border);
		background: var(--color-border);
	}

	.answer-contract div {
		background: rgb(255 255 255 / 0.76);
		padding: 0.85rem 0.75rem 0.95rem 0;
	}

	.answer-contract div + div {
		padding-inline-start: 0.9rem;
	}

	.answer-contract dt {
		font-family: var(--font-mono);
		font-size: 0.58rem;
		font-weight: 700;
		letter-spacing: 0.1em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.answer-contract dd {
		margin: 0.35rem 0 0;
		font-size: 0.7rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	.prompt-ledger {
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-block-start: 3px solid var(--color-brand);
		border-radius: var(--radius-surface);
		background: var(--color-canvas);
		box-shadow: var(--shadow-surface);
	}

	.prompt-ledger header {
		display: flex;
		align-items: end;
		justify-content: space-between;
		gap: 1rem;
		padding: 1.25rem;
		border-block-end: 1px solid var(--color-border);
	}

	.prompt-ledger h2 {
		margin: 0.25rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.25rem;
		font-weight: 900;
		line-height: 1.15;
		color: var(--color-text);
	}

	.prompt-ledger header > span,
	.session-header > span {
		flex: none;
		font-family: var(--font-mono);
		font-size: 0.6rem;
		color: var(--color-text-muted);
	}

	.prompt-ledger ol {
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.prompt-ledger li + li {
		border-block-start: 1px solid var(--color-border);
	}

	.prompt-ledger button {
		display: grid;
		width: 100%;
		min-height: 4.4rem;
		grid-template-columns: auto minmax(0, 1fr) auto;
		align-items: center;
		gap: 0.9rem;
		border: 0;
		background: transparent;
		padding: 0.9rem 1.15rem;
		text-align: left;
		transition:
			background-color 180ms ease,
			transform 140ms ease;
	}

	.prompt-ledger button:hover {
		background: var(--color-brand-soft);
	}

	.prompt-ledger button:active {
		transform: translateX(2px);
	}

	.prompt-ledger button > span {
		font-family: var(--font-mono);
		font-size: 0.625rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.prompt-ledger strong {
		font-size: 0.78rem;
		font-weight: 600;
		line-height: 1.45;
		color: var(--color-text-secondary);
	}

	.prompt-ledger i {
		font-size: 1rem;
		font-style: normal;
		color: var(--color-brand);
		transition: transform 180ms ease;
	}

	.prompt-ledger button:hover i {
		transform: translateX(3px);
	}

	.session-header {
		display: flex;
		align-items: end;
		justify-content: space-between;
		gap: 1rem;
		margin-bottom: 2rem;
		padding-bottom: 1.1rem;
		border-block-end: 1px solid var(--color-border);
	}

	.session-header h1 {
		margin: 0.2rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.75rem, 4vw, 2.5rem);
		font-weight: 900;
		line-height: 1;
		letter-spacing: -0.035em;
		color: var(--color-text);
	}

	.message-list {
		display: grid;
		gap: 1.75rem;
	}

	.message-row {
		display: flex;
		min-width: 0;
	}

	.user-message {
		justify-content: flex-end;
	}

	.user-bubble {
		max-width: min(84%, 38rem);
		overflow-wrap: anywhere;
		border-radius: var(--radius-surface) var(--radius-surface) 0.25rem var(--radius-surface);
		background: var(--color-text);
		padding: 0.8rem 1rem 0.9rem;
		color: var(--color-canvas);
	}

	.user-bubble span {
		display: block;
		margin-bottom: 0.3rem;
		font-family: var(--font-mono);
		font-size: 0.56rem;
		font-weight: 700;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: #d7e3fb;
	}

	.user-bubble p {
		margin: 0;
		font-size: 0.875rem;
		line-height: 1.65;
	}

	.answer-message {
		justify-content: flex-start;
	}

	.answer-record {
		width: min(96%, 46rem);
		overflow: hidden;
		border: 1px solid var(--color-border);
		border-inline-start: 3px solid var(--color-brand);
		border-radius: 0.35rem var(--radius-surface) var(--radius-surface) var(--radius-surface);
		background: var(--color-surface);
	}

	.answer-record-header {
		display: flex;
		align-items: center;
		gap: 0.6rem;
		min-height: 2.75rem;
		border-block-end: 1px solid var(--color-border);
		background: var(--color-surface-subtle);
		padding: 0.65rem 1.15rem;
	}

	.answer-record-header strong {
		font-family: var(--font-mono);
		font-size: 0.6rem;
	}

	.answer-record-header strong {
		font-weight: 600;
		letter-spacing: 0.07em;
		text-transform: uppercase;
		color: var(--color-text-secondary);
	}

	.answer-status {
		display: flex;
		align-items: center;
		gap: 0.9rem;
		padding: 1.2rem;
	}

	.answer-status p {
		margin: 0;
		font-size: 0.78rem;
		color: var(--color-text-secondary);
	}

	.status-trace {
		position: relative;
		display: block;
		width: 2.5rem;
		height: 2px;
		overflow: hidden;
		background: var(--color-brand-medium);
	}

	.status-trace i {
		position: absolute;
		inset: 0;
		background: var(--color-brand);
		animation: evidence-trace 1.1s ease-in-out infinite;
		transform: translateX(-100%);
	}

	.answer-error {
		border-inline-start: 3px solid var(--color-danger);
		background: var(--color-danger-surface);
		padding: 1rem 1.15rem 1.15rem;
		color: #881323;
	}

	.answer-error > strong {
		font-size: 0.75rem;
	}

	.answer-error p {
		margin: 0.35rem 0 0;
		font-size: 0.8rem;
		line-height: 1.6;
		overflow-wrap: anywhere;
	}

	.answer-error button {
		min-height: 2.75rem;
		margin-top: 0.8rem;
		border: 1px solid var(--color-danger-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 0.8rem;
		font-size: 0.72rem;
		font-weight: 700;
		color: #881323;
		transition:
			background-color 160ms ease,
			transform 120ms ease;
	}

	.answer-error button:hover {
		background: var(--color-danger-surface);
	}

	.answer-error button:active {
		transform: translateY(1px);
	}

	.answer-copy {
		margin: 0;
		padding: 1.2rem 1.2rem 1.05rem;
		font-size: 0.875rem;
		line-height: 1.75;
		white-space: pre-wrap;
		overflow-wrap: anywhere;
		color: var(--color-text);
	}

	.stream-status,
	.cancelled-status {
		border-block-start: 1px solid var(--color-border);
		padding: 0.7rem 1.2rem;
		font-size: 0.7rem;
		color: var(--color-text-secondary);
	}

	.stream-status {
		display: flex;
		align-items: center;
		gap: 0.55rem;
	}

	.stream-status span {
		width: 0.5rem;
		height: 0.5rem;
		background: var(--color-brand);
		animation: stream-pulse 1.2s ease-in-out infinite;
	}

	.cancelled-status {
		font-style: italic;
	}

	.evidence-disclosure {
		border-block-start: 1px solid var(--color-border);
	}

	.evidence-disclosure summary {
		display: flex;
		min-height: 2.75rem;
		cursor: pointer;
		align-items: center;
		gap: 0.5rem;
		padding: 0.65rem 1.2rem;
		font-family: var(--font-mono);
		font-size: 0.64rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		list-style: none;
		transition:
			background-color 160ms ease,
			color 160ms ease;
	}

	.evidence-disclosure summary::-webkit-details-marker {
		display: none;
	}

	.evidence-disclosure summary:hover {
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.evidence-disclosure pre {
		overflow-x: auto;
		margin: 0;
		border-block-start: 1px solid var(--color-border);
		background: var(--color-surface-subtle);
		padding: 1rem 1.2rem 1.2rem;
		font-family: var(--font-mono);
		font-size: 0.68rem;
		line-height: 1.7;
		white-space: pre;
		color: var(--color-text-secondary);
	}

	.chevron {
		flex-shrink: 0;
		transition: transform 150ms ease;
	}

	details[open] .chevron {
		transform: rotate(90deg);
	}

	.results-table-wrap {
		overflow-x: auto;
		border-block-start: 1px solid var(--color-border);
		padding: 0.75rem 1.2rem 1.15rem;
	}

	table {
		width: 100%;
		border-collapse: collapse;
		font-size: 0.72rem;
	}

	th {
		border-block-end: 1px solid var(--color-border);
		padding: 0.45rem 1rem 0.45rem 0;
		font-family: var(--font-mono);
		font-size: 0.58rem;
		font-weight: 700;
		letter-spacing: 0.06em;
		text-align: left;
		text-transform: uppercase;
		white-space: nowrap;
		color: var(--color-text-secondary);
	}

	td {
		max-width: 14rem;
		overflow: hidden;
		padding: 0.55rem 1rem 0.55rem 0;
		text-overflow: ellipsis;
		white-space: nowrap;
		color: var(--color-text-secondary);
	}

	tr.alternate {
		background: var(--color-surface-subtle);
	}

	.no-records {
		border-block-start: 1px solid var(--color-border);
		background: var(--color-accent-soft);
		padding: 0.85rem 1.2rem;
		font-size: 0.72rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	@keyframes evidence-trace {
		50% {
			transform: translateX(20%);
		}
		100% {
			transform: translateX(100%);
		}
	}

	@keyframes stream-pulse {
		50% {
			opacity: 0.35;
		}
	}

	@media (max-width: 52rem) {
		.ask-intro {
			grid-template-columns: 1fr;
			gap: 2.5rem;
			align-content: center;
		}

		.intro-copy h1 {
			max-width: 13ch;
		}
	}

	@media (max-width: 36rem) {
		.ask-scroll {
			padding: 1.5rem 1rem 2rem;
		}

		.ask-intro {
			align-content: start;
			padding-block: 1rem 2rem;
		}

		.intro-copy h1 {
			font-size: clamp(2.35rem, 12vw, 3.2rem);
		}

		.answer-contract {
			grid-template-columns: 1fr;
		}

		.answer-contract div + div {
			padding-inline-start: 0;
		}

		.prompt-ledger header {
			align-items: start;
			flex-direction: column;
			gap: 0.5rem;
		}

		.prompt-ledger button {
			padding-inline: 1rem;
		}

		.session-header {
			align-items: start;
		}

		.user-bubble {
			max-width: 90%;
		}

		.answer-record {
			width: 100%;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.status-trace i,
		.stream-status span {
			animation: none;
		}

		.status-trace i {
			transform: none;
		}

		.prompt-ledger button,
		.prompt-ledger i,
		.chevron {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.prompt-ledger,
		.answer-record,
		.user-bubble {
			border: 1px solid CanvasText;
		}
	}
</style>
