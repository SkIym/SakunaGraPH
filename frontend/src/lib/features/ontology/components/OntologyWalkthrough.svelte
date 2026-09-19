<script>
	import { ONTOLOGY_WALKTHROUGHS } from '../walkthroughs.js';

	let activeId = $state(ONTOLOGY_WALKTHROUGHS[0].id);
	const active = $derived(
		ONTOLOGY_WALKTHROUGHS.find((walkthrough) => walkthrough.id === activeId) ??
			ONTOLOGY_WALKTHROUGHS[0],
	);
</script>

<section id="question-to-query" class="walkthrough" aria-labelledby="walkthrough-title">
	<header class="walkthrough-heading">
		<div>
			<p class="walkthrough-kicker">From question to evidence</p>
			<h2 id="walkthrough-title">See a question become a graph query.</h2>
			<p class="walkthrough-copy">
				Every answer begins as a use case, becomes a set of ontology relationships, and ends as a
				precise request to the graph.
			</p>
		</div>
		<p class="question-count">
			<strong>03</strong><span>of 20 executable<br />competency questions</span>
		</p>
	</header>

	<div class="walkthrough-body">
		<div class="question-selector" aria-label="Choose a competency question">
			{#each ONTOLOGY_WALKTHROUGHS as walkthrough, index}
				<button
					type="button"
					class:active={walkthrough.id === active.id}
					aria-pressed={walkthrough.id === active.id}
					aria-controls="walkthrough-flow"
					onclick={() => (activeId = walkthrough.id)}
				>
					<small aria-hidden="true">0{index + 1}</small>
					<span>{walkthrough.id}</span>
					<strong>{walkthrough.shortLabel}</strong>
				</button>
			{/each}
		</div>

		<div id="walkthrough-flow" class="flow-surface" aria-live="polite">
			{#key active.id}
				<div class="flow-trace" aria-hidden="true"><span></span></div>
			{/key}

			<ol class="flow-grid">
				<li class="flow-step question-step">
					<div class="step-heading">
						<span aria-hidden="true">1</span>
						<h3>Ask</h3>
					</div>
					<p class="category">{active.category}</p>
					<blockquote>{active.question}</blockquote>
				</li>

				<li class="flow-step concepts-step">
					<div class="step-heading">
						<span aria-hidden="true">2</span>
						<h3>Find the concepts</h3>
					</div>
					<ul class="concept-field" aria-label={`Ontology concepts required for ${active.id}`}>
						{#each active.concepts as concept}
							<li data-kind={concept.kind}>{concept.label}</li>
						{/each}
					</ul>
				</li>

				<li class="flow-step model-step">
					<div class="step-heading">
						<span aria-hidden="true">3</span>
						<h3>Connect the model</h3>
					</div>
					<ul class="relation-list" aria-label={`Ontology relationships used by ${active.id}`}>
						{#each active.relations as relation}
							<li>
								<strong>{relation.subject}</strong>
								<span class="predicate">
									<i aria-hidden="true"></i>
									<code>{relation.predicate}</code>
									<svg aria-hidden="true" viewBox="0 0 12 8">
										<path d="M1 4h9M7 1l3 3-3 3" />
									</svg>
								</span>
								<strong>{relation.object}</strong>
							</li>
						{/each}
					</ul>
					<p class="model-note">{active.insight}</p>
				</li>

				<li class="flow-step query-step">
					<div class="step-heading">
						<span aria-hidden="true">4</span>
						<h3>Ask the graph</h3>
					</div>
					<div class="query-preview">
						<div class="query-bar">
							<span>{active.id}.rq</span>
							<span>SELECT · read only</span>
						</div>
						<pre><code>{active.queryPreview}</code></pre>
					</div>
					<details>
						<summary>View the complete SPARQL query</summary>
						<pre><code>{active.query}</code></pre>
					</details>
					<a class="workspace-link" href={`/query?cq=${active.id}`}>
						Open {active.id} in the query workspace
						<svg aria-hidden="true" viewBox="0 0 20 20">
							<path d="M4 10h12M11 5l5 5-5 5" />
						</svg>
					</a>
				</li>
			</ol>
		</div>
	</div>
</section>

<style>
	.walkthrough {
		scroll-margin-top: calc(var(--app-nav-height) + 1.25rem);
		margin: clamp(5rem, 10vw, 9rem) 0 0;
		border-top: 1px solid var(--color-border);
		padding-top: clamp(3rem, 7vw, 6rem);
	}

	.walkthrough-heading {
		display: flex;
		align-items: end;
		justify-content: space-between;
		gap: 2rem;
		margin-bottom: 1.75rem;
	}

	.walkthrough-heading h2 {
		max-width: 17ch;
		margin: 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2.1rem, 4.5vw, 4rem);
		font-weight: 900;
		line-height: 1.02;
		letter-spacing: -0.035em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.walkthrough-kicker {
		margin: 0 0 0.7rem;
		font-size: 0.6875rem;
		font-weight: 750;
		line-height: 1.4;
		letter-spacing: 0.11em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.walkthrough-heading .walkthrough-copy {
		max-width: 60ch;
		margin: 1rem 0 0;
		font-size: 0.875rem;
		line-height: 1.7;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.question-count {
		display: flex;
		align-items: center;
		gap: 0.8rem;
		flex: none;
		margin: 0 0 0.2rem;
		color: var(--color-text-muted);
	}

	.question-count strong {
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 2.75rem;
		font-weight: 900;
		line-height: 1;
		color: var(--color-brand);
	}

	.question-count span {
		font-family: var(--font-mono);
		font-size: 0.625rem;
		line-height: 1.5;
	}

	.walkthrough-body {
		display: grid;
		grid-template-columns: minmax(14rem, 0.24fr) minmax(0, 1fr);
		border: 1px solid var(--color-border);
		border-radius: 0.35rem 1.5rem 0.35rem 1.5rem;
		overflow: hidden;
		box-shadow: 0 2rem 5rem -3.4rem rgb(18 42 78 / 0.45);
	}

	.question-selector {
		display: grid;
		align-content: start;
		background: #f5f7fb;
	}

	.question-selector button {
		display: grid;
		grid-template-columns: auto 1fr;
		align-items: center;
		gap: 0.2rem 0.8rem;
		min-height: 6.25rem;
		border: 0;
		border-bottom: 1px solid var(--color-border);
		background: transparent;
		padding: 1rem 1.1rem;
		text-align: left;
		color: var(--color-text-secondary);
		cursor: pointer;
		transition:
			background-color 180ms ease,
			color 180ms ease;
	}

	.question-selector button:last-child {
		border-bottom: 0;
	}

	.question-selector button:hover {
		background: var(--color-brand-soft);
		color: var(--color-text);
	}

	.question-selector button.active {
		background: #fff8cf;
		color: var(--color-text);
		box-shadow: inset 3px 0 0 var(--color-accent);
	}

	.question-selector button > small {
		grid-row: 1 / 3;
		align-self: start;
		font-family: var(--font-mono);
		font-size: 0.5625rem;
		line-height: 1.4;
		color: var(--color-text-secondary);
	}

	.question-selector button > span {
		font-family: var(--font-mono);
		font-size: 0.6875rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.question-selector strong {
		grid-column: 2;
		font-size: 0.8125rem;
		line-height: 1.35;
	}

	.flow-surface {
		position: relative;
		overflow: hidden;
		background: rgba(255, 255, 255, 0.94);
	}

	.flow-trace {
		display: none;
	}

	.flow-trace span {
		position: absolute;
		left: 0;
		display: block;
		width: 0.6rem;
		height: 0.6rem;
		margin-top: -0.28rem;
		border-radius: 9999px;
		background: var(--color-brand);
		box-shadow: 0 0 0 0.35rem rgba(215, 227, 251, 0.9);
		animation: follow-query 850ms cubic-bezier(0.16, 1, 0.3, 1) both;
	}

	.flow-grid {
		position: relative;
		z-index: 3;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		margin: 0;
		padding: 0;
		list-style: none;
	}

	.flow-step {
		min-width: 0;
		min-height: 19rem;
		padding: 1.5rem clamp(1.25rem, 2.4vw, 2rem) 1.8rem;
	}

	.flow-step + .flow-step {
		border-left: 1px solid var(--color-border);
	}

	.flow-step:nth-child(3) {
		border-top: 1px solid var(--color-border);
		border-left: 0;
	}

	.flow-step:nth-child(4) {
		border-top: 1px solid var(--color-border);
	}

	.step-heading {
		display: flex;
		align-items: center;
		gap: 0.65rem;
		min-height: 2.25rem;
	}

	.step-heading > span {
		display: grid;
		width: 2.15rem;
		height: 2.15rem;
		place-items: center;
		border: 1px solid var(--color-brand);
		border-radius: 50%;
		background: var(--color-canvas);
		font-family: var(--font-mono);
		font-size: 0.6875rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.step-heading h3 {
		margin: 0;
		font-size: 0.75rem;
		font-weight: 800;
		line-height: 1.25;
		letter-spacing: 0.04em;
		text-transform: uppercase;
		color: var(--color-text);
	}

	.category {
		margin: 1.35rem 0 0;
		font-family: var(--font-mono);
		font-size: 0.625rem;
		font-weight: 700;
		line-height: 1.5;
		letter-spacing: 0.04em;
		color: var(--color-brand);
	}

	blockquote {
		margin: 0.75rem 0 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.2rem, 1.7vw, 1.55rem);
		font-weight: 700;
		line-height: 1.3;
		letter-spacing: -0.015em;
		color: var(--color-text);
		text-wrap: pretty;
	}

	.concept-field {
		display: flex;
		flex-wrap: wrap;
		align-content: flex-start;
		gap: 0.6rem;
		margin: 1.35rem 0 0;
		padding: 0;
		list-style: none;
	}

	.concept-field li {
		min-height: 2.2rem;
		display: inline-flex;
		align-items: center;
		border: 1px solid var(--color-border);
		border-radius: 9999px;
		background: var(--color-canvas);
		padding: 0.45rem 0.7rem;
		font-family: var(--font-mono);
		font-size: 0.625rem;
		line-height: 1.25;
		color: var(--color-text-secondary);
	}

	.concept-field li[data-kind='class'] {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
		color: var(--color-brand-hover);
	}

	.concept-field li[data-kind='value'] {
		border-color: #ead06c;
		background: var(--color-accent-soft);
		color: var(--color-accent-ink);
	}

	.concept-field li[data-kind='operator'] {
		border-color: #9ecfb8;
		background: #edf8f2;
		color: #216447;
	}

	.relation-list {
		display: grid;
		gap: 0.8rem;
		margin: 1.35rem 0 0;
		padding: 0;
		list-style: none;
	}

	.relation-list li {
		display: grid;
		grid-template-columns: minmax(4.2rem, 0.8fr) minmax(5.7rem, 1fr) minmax(4.2rem, 0.8fr);
		align-items: center;
		gap: 0.35rem;
	}

	.relation-list strong {
		font-size: 0.625rem;
		font-weight: 700;
		line-height: 1.25;
		color: var(--color-text-secondary);
		overflow-wrap: anywhere;
	}

	.predicate {
		position: relative;
		display: grid;
		justify-items: center;
		min-width: 0;
		color: var(--color-brand);
	}

	.predicate i {
		position: absolute;
		top: 50%;
		left: 0;
		width: 100%;
		height: 1px;
		background: var(--color-brand-medium);
	}

	.predicate code {
		position: relative;
		z-index: 1;
		max-width: 100%;
		background: var(--color-canvas);
		padding: 0 0.25rem;
		font-family: var(--font-mono);
		font-size: 0.5625rem;
		line-height: 1.3;
		color: var(--color-brand);
		overflow-wrap: anywhere;
	}

	.predicate svg {
		position: absolute;
		right: -0.1rem;
		top: calc(50% - 0.23rem);
		width: 0.75rem;
		height: 0.5rem;
		fill: none;
		stroke: currentColor;
		stroke-width: 1.2;
	}

	.model-note {
		margin: 1.2rem 0 0;
		border-top: 1px solid var(--color-border);
		padding-top: 0.9rem;
		font-size: 0.6875rem;
		line-height: 1.55;
		color: var(--color-text-muted);
	}

	.query-preview {
		overflow: hidden;
		margin-top: 1.35rem;
		border-radius: var(--radius-control);
		background: #182236;
		box-shadow: var(--shadow-control);
		color: #e2e8f0;
	}

	.query-bar {
		display: flex;
		justify-content: space-between;
		gap: 1rem;
		border-bottom: 1px solid #334155;
		padding: 0.55rem 0.7rem;
		font-family: var(--font-mono);
		font-size: 0.5625rem;
		font-variant-numeric: tabular-nums;
		color: #b8c6dc;
	}

	.query-preview pre,
	details pre {
		margin: 0;
		font-family: var(--font-mono);
		font-size: 0.625rem;
		line-height: 1.65;
		tab-size: 2;
	}

	.query-preview pre {
		overflow: auto;
		padding: 0.8rem;
	}

	.query-preview code,
	details code {
		font: inherit;
	}

	details {
		margin-top: 0.85rem;
		font-size: 0.6875rem;
		color: var(--color-text-secondary);
	}

	details summary {
		min-height: 2.75rem;
		display: flex;
		align-items: center;
		width: fit-content;
		cursor: pointer;
		font-weight: 700;
		color: var(--color-brand);
	}

	details pre {
		max-height: 22rem;
		overflow: auto;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-surface-subtle);
		padding: 0.85rem;
		color: var(--color-text-secondary);
	}

	.workspace-link {
		display: inline-flex;
		align-items: center;
		gap: 0.55rem;
		min-height: 2.75rem;
		margin-top: 0.8rem;
		font-size: 0.75rem;
		font-weight: 800;
		line-height: 1.35;
		color: var(--color-brand);
		text-decoration: underline;
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.25rem;
	}

	.workspace-link:hover {
		color: var(--color-brand-hover);
		text-decoration-color: var(--color-brand);
	}

	.workspace-link svg {
		width: 1rem;
		height: 1rem;
		fill: none;
		stroke: currentColor;
		stroke-width: 1.7;
		stroke-linecap: round;
		stroke-linejoin: round;
		transition: transform 180ms ease;
	}

	.workspace-link:hover svg {
		transform: translateX(0.2rem);
	}

	@keyframes follow-query {
		from {
			left: 0;
		}
		to {
			left: 100%;
		}
	}

	@media (max-width: 1050px) {
		.walkthrough-body {
			grid-template-columns: 1fr;
		}

		.question-selector {
			grid-template-columns: repeat(3, minmax(0, 1fr));
		}

		.question-selector button {
			min-height: 5.25rem;
			border-right: 1px solid var(--color-border);
			border-bottom: 1px solid var(--color-border);
		}

		.question-selector button:last-child {
			border-right: 0;
			border-bottom: 1px solid var(--color-border);
		}

		.question-selector button.active {
			box-shadow: inset 0 3px 0 var(--color-accent);
		}
	}

	@media (max-width: 720px) {
		.walkthrough-heading {
			align-items: start;
			flex-direction: column;
			gap: 0.75rem;
		}

		.question-selector {
			grid-template-columns: 1fr;
		}

		.question-selector button {
			min-height: 4.6rem;
			border-right: 0;
			border-bottom: 1px solid var(--color-border);
		}

		.question-selector button:last-child {
			border-bottom: 0;
		}

		.question-selector button.active {
			box-shadow: inset 3px 0 0 var(--color-accent);
		}

		.flow-grid {
			grid-template-columns: 1fr;
		}

		.flow-step + .flow-step,
		.flow-step:nth-child(3) {
			border-top: 1px solid var(--color-border);
			border-left: 0;
		}

		.flow-step {
			min-height: 0;
			padding: 1.25rem 1rem 1.5rem;
		}

		blockquote {
			font-size: 1.35rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.flow-trace span {
			animation: none;
		}

		.workspace-link svg,
		.question-selector button {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.question-selector button.active,
		.concept-field li,
		.query-preview {
			border: 1px solid CanvasText;
			background: Canvas;
			color: CanvasText;
		}

		.flow-trace,
		.predicate i {
			background: CanvasText;
		}
	}
</style>
