<section class="process-teaser" aria-labelledby="process-teaser-title">
	<div class="teaser-copy">
		<h2 id="process-teaser-title">See a question become a graph query.</h2>
		<p>
			Follow one disaster-data question through the concepts, relationships, and SPARQL that make
			its answer possible.
		</p>
		<a href="/ontology#question-to-query">
			Follow the ontology process
			<svg aria-hidden="true" viewBox="0 0 20 20">
				<path d="M4 10h12M11 5l5 5-5 5" />
			</svg>
		</a>
	</div>

	<div class="mini-flow" aria-label="Question, concepts, ontology relationships, then SPARQL">
		<div class="mini-path" aria-hidden="true"></div>
		<div class="mini-node question-node">
			<span>1</span>
			<strong>Which floods happened, when, and where?</strong>
		</div>
		<div class="mini-node">
			<span>2</span>
			<strong>Event · Type · Date · Place</strong>
		</div>
		<div class="mini-node">
			<span>3</span>
			<strong>hasDisasterType · startDate · isPartOf</strong>
		</div>
		<div class="mini-node query-node">
			<span>4</span>
			<strong>SELECT ?event ?date ?region</strong>
		</div>
	</div>
</section>

<style>
	.process-teaser {
		display: grid;
		grid-template-columns: minmax(15rem, 0.68fr) minmax(0, 1.32fr);
		gap: clamp(3rem, 7vw, 6rem);
		max-width: 75rem;
		margin: 0 auto;
		padding: clamp(5rem, 9vw, 8rem) 1.25rem;
	}

	.teaser-copy h2 {
		max-width: 13ch;
		margin: 0;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(2.4rem, 4.5vw, 4rem);
		font-weight: 900;
		line-height: 1.02;
		letter-spacing: -0.035em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.teaser-copy p {
		max-width: 39ch;
		margin: 1.2rem 0 0;
		font-size: 0.9375rem;
		line-height: 1.75;
		color: var(--color-text-secondary);
		text-wrap: pretty;
	}

	.teaser-copy a {
		display: inline-flex;
		align-items: center;
		gap: 0.65rem;
		min-height: 2.75rem;
		margin-top: 1.4rem;
		font-size: 0.8125rem;
		font-weight: 800;
		color: var(--color-brand);
		text-decoration: underline;
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.3rem;
	}

	.teaser-copy a:hover {
		color: var(--color-brand-hover);
		text-decoration-color: var(--color-brand);
	}

	.teaser-copy svg {
		width: 1rem;
		height: 1rem;
		fill: none;
		stroke: currentColor;
		stroke-width: 1.7;
		stroke-linecap: round;
		stroke-linejoin: round;
		transition: transform 180ms ease;
	}

	.teaser-copy a:hover svg {
		transform: translateX(0.2rem);
	}

	.mini-flow {
		position: relative;
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		align-content: center;
		gap: 1px;
		min-height: 24rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-surface);
		background: var(--color-border);
		box-shadow: var(--shadow-surface);
		overflow: hidden;
	}

	.mini-path {
		position: absolute;
		z-index: 3;
		top: 50%;
		left: 50%;
		width: 0.75rem;
		height: 0.75rem;
		border: 2px solid var(--color-canvas);
		border-radius: 50%;
		background: var(--color-brand);
		box-shadow: 0 0 0 0.35rem var(--color-brand-medium);
		transform: translate(-50%, -50%);
	}

	.mini-node {
		position: relative;
		z-index: 1;
		display: flex;
		min-height: 11.75rem;
		flex-direction: column;
		justify-content: space-between;
		background: rgba(255, 255, 255, 0.96);
		padding: 1.25rem;
	}

	.mini-node span {
		display: grid;
		width: 2rem;
		height: 2rem;
		place-items: center;
		border: 1px solid var(--color-brand-medium);
		border-radius: 50%;
		font-family: var(--font-mono);
		font-size: 0.6875rem;
		font-weight: 700;
		color: var(--color-brand);
	}

	.mini-node strong {
		max-width: 22ch;
		font-size: 0.8125rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	.question-node {
		background: var(--color-accent-soft);
	}

	.question-node strong {
		font-family: 'Playfair Display', Georgia, serif;
		font-size: 1.25rem;
		line-height: 1.35;
		color: var(--color-text);
	}

	.query-node {
		background: #182236;
	}

	.query-node span {
		border-color: #648bd6;
		color: #d7e3fb;
	}

	.query-node strong {
		font-family: var(--font-mono);
		font-size: 0.75rem;
		color: #e2e8f0;
	}

	@media (max-width: 800px) {
		.process-teaser {
			grid-template-columns: 1fr;
			gap: 2.5rem;
		}
	}

	@media (max-width: 359px) {
		.mini-flow {
			grid-template-columns: 1fr;
			min-height: 0;
		}

		.mini-path {
			display: none;
		}

		.mini-node {
			min-height: 8.5rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.teaser-copy svg {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.mini-node,
		.question-node,
		.query-node {
			background: Canvas;
			color: CanvasText;
		}
	}
</style>
