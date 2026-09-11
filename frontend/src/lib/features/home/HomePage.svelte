<script>
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import HomeMapFigure from './HomeMapFigure.svelte';

	const researchTools = [
		{
			href: '/query',
			name: 'Run a SPARQL query',
			description: 'Inspect the loaded graph with read-only SPARQL SELECT queries.',
		},
		{
			href: '/ontology',
			name: 'Browse the ontology',
			description: 'Examine disaster classes, the taxonomy, and PSGC location structure.',
		},
		{
			href: '/analysis',
			name: 'Analyze event records',
			description: 'Filter records, compare metrics, and inspect dates and sources.',
		},
	];

	const recordSources = ['NDRRMC', 'GDA', 'EM-DAT', 'DROMIC'];
</script>

<svelte:head>
	<title>SakunaGraPH · Philippine disaster data</title>
	<meta
		name="description"
		content="Explore Philippine disaster records by place, ask questions in plain language, or inspect the underlying knowledge graph."
	/>
</svelte:head>

<NodeCanvas />

<main class="home-page relative z-1 overflow-hidden">
	<section
		class="hero-section page-viewport relative px-5 pt-8 pb-12 sm:pt-12 lg:px-8 lg:pt-14 lg:pb-10"
	>
		<div
			class="hero-layout mx-auto grid w-full max-w-[84rem] items-center gap-12 lg:grid-cols-[minmax(0,1.03fr)_minmax(27rem,0.97fr)] lg:gap-10 xl:gap-16"
		>
			<div class="hero-copy min-w-0 max-w-[45rem]">
				<div class="section-marker" aria-label="Philippine disaster knowledge graph">
					<span class="marker-number" aria-hidden="true">01</span>
					<span class="marker-rule" aria-hidden="true"></span>
					<span>Philippine disaster knowledge graph</span>
				</div>

				<a href="/" class="home-brand mt-8 inline-block" aria-label="SakunaGraPH home">
					Sakuna<span>GraPH</span>
				</a>

				<h1 class="mt-7 max-w-[13ch] text-balance font-black leading-[0.98] text-slate-800">
					Explore Philippine disaster records by place.
				</h1>

				<p
					class="hero-intro mt-7 max-w-[61ch] text-base leading-7 text-slate-600 sm:text-lg sm:leading-8"
				>
					SakunaGraPH connects records from several disaster-data sources in one knowledge graph.
					Begin with a place, then follow each event to its dates, evidence, and linked sources.
				</p>

				<div
					class="hero-actions mt-9 flex flex-col items-stretch gap-3 sm:flex-row sm:items-center"
				>
					<a href="/map" class="primary-action touch-target">
						<span>Explore the map</span>
						<svg
							aria-hidden="true"
							width="18"
							height="18"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
						>
							<path d="M5 12h14" /><path d="m13 6 6 6-6 6" />
						</svg>
					</a>
					<a href="/ask" class="secondary-action touch-target">
						<span>Ask about the data</span>
						<svg
							aria-hidden="true"
							width="16"
							height="16"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
						>
							<path d="M5 12h14" /><path d="m13 6 6 6-6 6" />
						</svg>
					</a>
				</div>

				<div class="research-note mt-7 flex max-w-[38rem] items-start gap-3">
					<svg
						aria-hidden="true"
						class="mt-0.5 shrink-0 text-[var(--color-brand)]"
						width="18"
						height="18"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="1.8"
						stroke-linecap="round"
						stroke-linejoin="round"
					>
						<circle cx="12" cy="12" r="9" /><path d="M12 11v5" /><path d="M12 8h.01" />
					</svg>
					<p class="text-sm leading-6 text-slate-600">
						A research and exploration tool—not a live emergency alert service.
					</p>
				</div>
			</div>

			<aside class="map-workbench min-w-0" aria-label="Interactive geographic entry point">
				<div class="map-workbench-header flex items-start justify-between gap-5">
					<div>
						<p class="workbench-label">Geographic entry point</p>
						<p class="mt-1 text-sm leading-5 text-slate-600">
							Select any province to open its records.
						</p>
					</div>
					<a href="/map?view=provinces" class="map-link shrink-0">Open map</a>
				</div>
				<div class="hero-map mt-2">
					<HomeMapFigure />
				</div>
			</aside>
		</div>

		<div
			class="source-ledger mx-auto mt-12 grid w-full max-w-[84rem] md:grid-cols-[0.72fr_1.3fr_0.9fr] lg:mt-6"
		>
			<div class="ledger-cell">
				<span class="ledger-label">Coverage</span>
				<strong>Philippine disaster records</strong>
			</div>
			<div class="ledger-cell">
				<span class="ledger-label">Event sources</span>
				<strong>{recordSources.join(' · ')}</strong>
			</div>
			<div class="ledger-cell">
				<span class="ledger-label">Geography</span>
				<strong>PSGC administrative areas</strong>
			</div>
		</div>
	</section>

	<section
		class="research-section relative px-5 py-20 lg:px-8 lg:py-28"
		aria-labelledby="research-tools-title"
	>
		<div
			class="mx-auto grid max-w-[75rem] gap-12 lg:grid-cols-[minmax(15rem,0.68fr)_minmax(0,1.32fr)] lg:gap-20"
		>
			<header class="max-w-sm">
				<div class="section-marker">
					<span class="marker-number" aria-hidden="true">02</span>
					<span class="marker-rule" aria-hidden="true"></span>
					<span>Research pathways</span>
				</div>
				<h2
					id="research-tools-title"
					class="editorial-title mt-7 text-4xl font-black leading-[1.04] text-slate-800 sm:text-5xl"
				>
					Choose your level of detail.
				</h2>
				<p class="mt-5 max-w-[34ch] text-base leading-7 text-slate-600">
					Move from guided exploration to the graph’s technical structure without losing the source
					trail.
				</p>
			</header>

			<nav class="research-index min-w-0" aria-label="Research tools">
				{#each researchTools as tool, index}
					<a href={tool.href} class="research-link group">
						<span class="research-number" aria-hidden="true">0{index + 1}</span>
						<span class="min-w-0">
							<span class="research-name">{tool.name}</span>
							<span class="research-description">{tool.description}</span>
						</span>
						<svg
							aria-hidden="true"
							class="research-arrow shrink-0"
							width="20"
							height="20"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="1.8"
							stroke-linecap="round"
							stroke-linejoin="round"
						>
							<path d="M5 12h14" /><path d="m13 6 6 6-6 6" />
						</svg>
					</a>
				{/each}
			</nav>
		</div>
	</section>

	<section
		class="evidence-section relative px-5 py-20 lg:px-8 lg:pt-28 lg:pb-32"
		aria-labelledby="understand-data-title"
	>
		<div class="mx-auto max-w-[75rem]">
			<div class="evidence-heading grid gap-7 lg:grid-cols-[0.75fr_1.25fr] lg:items-end lg:gap-16">
				<div class="section-marker">
					<span class="marker-number" aria-hidden="true">03</span>
					<span class="marker-rule" aria-hidden="true"></span>
					<span>Use notes</span>
				</div>
				<div>
					<h2
						id="understand-data-title"
						class="editorial-title max-w-[17ch] text-4xl font-black leading-[1.04] text-slate-800 sm:text-5xl"
					>
						Understand the data before using it.
					</h2>
					<p class="mt-5 max-w-[66ch] text-base leading-7 text-slate-600">
						SakunaGraPH combines source records without claiming that every event, place, period, or
						field is complete. Follow the linked evidence whenever timing or precision matters.
					</p>
				</div>
			</div>

			<div class="evidence-layout mt-14 grid gap-12 lg:grid-cols-[0.75fr_1.25fr] lg:gap-16">
				<aside class="evidence-principle">
					<p class="principle-kicker">Working principle</p>
					<p class="principle-copy mt-4">Evidence first. Interpretation second.</p>
					<div class="mt-7 flex flex-wrap gap-x-6 gap-y-2 text-sm font-semibold">
						<a href="/analysis/events" class="brand-link editorial-link">Inspect event records</a>
						<a href="/ontology" class="brand-link editorial-link">Browse the ontology</a>
					</div>
				</aside>

				<div class="evidence-list">
					<article class="evidence-item">
						<p class="evidence-number" aria-hidden="true">A</p>
						<div>
							<h3>Data freshness</h3>
							<p>
								A site-wide last-updated time is not currently published. Individual source records
								may include obtained and last-updated dates; open an event to inspect what is
								available.
							</p>
						</div>
					</article>

					<article class="evidence-item">
						<p class="evidence-number" aria-hidden="true">B</p>
						<div>
							<h3>Source coverage</h3>
							<p>
								The graph integrates records associated with {recordSources.join(', ')}. PSGC
								provides the Philippine administrative geography used to connect locations. Coverage
								varies by source, place, period, and field.
							</p>
						</div>
					</article>

					<article class="evidence-item">
						<p class="evidence-number" aria-hidden="true">C</p>
						<div>
							<h3>Methodology</h3>
							<p>
								Source files are parsed, normalized into RDF, linked to shared disaster and location
								concepts, and aligned across datasets. Alternate source records remain available for
								inspection.
							</p>
						</div>
					</article>
				</div>
			</div>
		</div>
	</section>

	<footer class="home-footer px-5 py-9 lg:px-8">
		<div
			class="mx-auto flex max-w-[75rem] flex-col gap-8 sm:flex-row sm:items-end sm:justify-between"
		>
			<div>
				<p class="footer-wordmark">Sakuna<span>GraPH</span></p>
				<p class="mt-2 max-w-[62ch] text-sm leading-6 text-slate-600">
					Built by Judelle Clareese E. Gaza and Abram Josh C. Marcelo at the UP Diliman Web Science
					Laboratory.
				</p>
			</div>
			<div class="flex flex-wrap gap-x-5 gap-y-2 text-sm font-medium">
				<a
					href="https://github.com/ElleDiablo"
					target="_blank"
					rel="noopener noreferrer"
					class="footer-link">Judelle on GitHub</a
				>
				<a
					href="https://github.com/SkIym"
					target="_blank"
					rel="noopener noreferrer"
					class="footer-link">Abram on GitHub</a
				>
			</div>
		</div>
	</footer>
</main>

<style>
	.home-page {
		background:
			radial-gradient(circle at 77% 8%, rgba(237, 242, 255, 0.82), transparent 25rem),
			radial-gradient(circle at 8% 34%, rgba(237, 242, 255, 0.46), transparent 21rem),
			var(--color-canvas);
	}

	.hero-section::after {
		position: absolute;
		right: max(1.25rem, calc((100vw - 84rem) / 2));
		bottom: 0;
		left: max(1.25rem, calc((100vw - 84rem) / 2));
		height: 1px;
		background: var(--color-border);
		content: '';
	}

	.home-brand,
	.editorial-title,
	h1,
	.principle-copy,
	.footer-wordmark {
		font-family: 'Playfair Display', Georgia, serif;
	}

	.home-brand {
		max-width: 100%;
		font-size: clamp(1.55rem, 4vw, 2.2rem);
		font-weight: 900;
		line-height: 1;
		letter-spacing: 0.01em;
		color: var(--color-text);
		text-decoration: none;
	}

	.home-brand span,
	.footer-wordmark span {
		color: var(--color-brand);
	}

	h1 {
		font-size: clamp(3rem, 6.1vw, 5.45rem);
		letter-spacing: -0.045em;
		text-wrap: balance;
		overflow-wrap: anywhere;
	}

	.hero-intro,
	.evidence-item p,
	.research-description {
		text-wrap: pretty;
	}

	.section-marker {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 500;
		line-height: 1.4;
		letter-spacing: 0.08em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.marker-number {
		font-variant-numeric: tabular-nums;
	}

	.marker-rule {
		width: 2.25rem;
		height: 1px;
		background: var(--color-brand);
	}

	.primary-action,
	.secondary-action {
		display: inline-flex;
		align-items: center;
		justify-content: center;
		gap: 0.75rem;
		border-radius: var(--radius-control);
		font-size: 0.875rem;
		font-weight: 600;
		text-decoration: none;
		transition:
			transform 220ms cubic-bezier(0.16, 1, 0.3, 1),
			background-color 220ms ease,
			color 220ms ease,
			box-shadow 220ms ease;
	}

	.primary-action {
		padding: 0.625rem 1.25rem;
		background: var(--color-text);
		box-shadow: 0 9px 22px -15px rgba(30, 41, 59, 0.78);
		color: var(--color-canvas);
	}

	.primary-action:hover {
		transform: translateY(-2px);
		background: #334155;
		box-shadow: 0 14px 26px -16px rgba(30, 41, 59, 0.84);
	}

	.secondary-action {
		padding: 0.625rem 0.25rem;
		color: var(--color-brand);
	}

	.secondary-action svg,
	.primary-action svg {
		transition: transform 220ms cubic-bezier(0.16, 1, 0.3, 1);
	}

	.secondary-action:hover {
		color: var(--color-brand-hover);
	}

	.secondary-action:hover svg,
	.primary-action:hover svg {
		transform: translateX(3px);
	}

	.primary-action:active,
	.secondary-action:active,
	.research-link:active {
		transform: scale(0.985);
	}

	.research-note {
		border-top: 1px solid var(--color-border);
		padding-top: 1rem;
	}

	.map-workbench {
		border: 1px solid rgba(215, 227, 251, 0.95);
		border-radius: var(--radius-surface);
		background: rgba(255, 255, 255, 0.86);
		box-shadow:
			var(--shadow-surface),
			inset 0 1px 0 rgba(255, 255, 255, 0.96);
		padding: 1rem 1rem 0.9rem;
		backdrop-filter: blur(12px);
	}

	.map-workbench-header {
		border-bottom: 1px solid var(--color-border);
		padding: 0.2rem 0.35rem 0.9rem;
	}

	.workbench-label,
	.ledger-label,
	.principle-kicker {
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 500;
		line-height: 1.4;
		letter-spacing: 0.07em;
		text-transform: uppercase;
		color: var(--color-brand);
	}

	.map-link {
		min-height: 2.75rem;
		display: inline-flex;
		align-items: center;
		border-radius: 0.5rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-brand);
		text-underline-offset: 0.25rem;
		transition:
			background-color 180ms ease,
			color 180ms ease;
	}

	.map-link:hover {
		color: var(--color-brand-hover);
		text-decoration: underline;
	}

	.hero-map :global(figure > div) {
		height: clamp(25rem, 38vw, 34rem);
	}

	.hero-map :global(figcaption) {
		justify-content: flex-start;
		padding-inline: 0.35rem;
	}

	.source-ledger {
		position: relative;
		z-index: 1;
		border-top: 1px solid var(--color-border);
		border-bottom: 1px solid var(--color-border);
		background: rgba(255, 255, 255, 0.7);
	}

	.ledger-cell {
		display: grid;
		gap: 0.35rem;
		padding: 0.85rem 1rem;
	}

	.ledger-cell strong {
		font-size: 0.75rem;
		font-weight: 600;
		line-height: 1.5;
		letter-spacing: 0.01em;
		color: var(--color-text-secondary);
	}

	.research-section {
		border-bottom: 1px solid var(--color-border);
		background: rgba(241, 244, 250, 0.82);
	}

	.research-index {
		border-top: 1px solid var(--color-border);
	}

	.research-link {
		display: grid;
		grid-template-columns: 2rem minmax(0, 1fr) auto;
		align-items: center;
		gap: 1.1rem;
		min-height: 8rem;
		border-bottom: 1px solid var(--color-border);
		padding: 1.45rem 1rem;
		color: var(--color-text);
		text-decoration: none;
		transition:
			background-color 220ms ease,
			transform 220ms cubic-bezier(0.16, 1, 0.3, 1);
	}

	.research-link:hover {
		background: rgba(237, 242, 255, 0.88);
	}

	.research-number {
		align-self: start;
		padding-top: 0.28rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 500;
		font-variant-numeric: tabular-nums;
		color: var(--color-brand);
	}

	.research-name {
		display: block;
		font-family: 'Playfair Display', Georgia, serif;
		font-size: clamp(1.35rem, 2.4vw, 1.75rem);
		font-weight: 700;
		line-height: 1.2;
		letter-spacing: -0.015em;
		color: var(--color-text);
	}

	.research-description {
		display: block;
		max-width: 48ch;
		margin-top: 0.45rem;
		font-size: 0.875rem;
		line-height: 1.55;
		color: var(--color-text-secondary);
	}

	.research-arrow {
		color: var(--color-brand);
		transition: transform 220ms cubic-bezier(0.16, 1, 0.3, 1);
	}

	.research-link:hover .research-arrow {
		transform: translateX(4px);
	}

	.evidence-section {
		background: rgba(255, 255, 255, 0.9);
	}

	.evidence-heading {
		border-bottom: 1px solid var(--color-border);
		padding-bottom: 2.5rem;
	}

	.evidence-principle {
		align-self: start;
		border-left: 2px solid var(--color-brand);
		padding-left: 1.25rem;
	}

	.principle-copy {
		max-width: 12ch;
		font-size: clamp(1.9rem, 3.3vw, 2.7rem);
		font-weight: 700;
		line-height: 1.12;
		letter-spacing: -0.025em;
		color: var(--color-text);
		text-wrap: balance;
	}

	.editorial-link {
		text-decoration-line: underline;
		text-decoration-color: var(--color-brand-medium);
		text-underline-offset: 0.3rem;
		transition:
			color 180ms ease,
			text-decoration-color 180ms ease;
	}

	.editorial-link:hover {
		text-decoration-color: var(--color-brand);
	}

	.evidence-list {
		border-top: 1px solid var(--color-border);
	}

	.evidence-item {
		display: grid;
		grid-template-columns: 2rem minmax(0, 1fr);
		gap: 1.25rem;
		border-bottom: 1px solid var(--color-border);
		padding: 1.75rem 0;
	}

	.evidence-number {
		padding-top: 0.2rem;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.75rem;
		font-weight: 500;
		color: var(--color-brand);
	}

	.evidence-item h3 {
		font-size: 1rem;
		font-weight: 700;
		line-height: 1.4;
		color: var(--color-text);
	}

	.evidence-item p:not(.evidence-number) {
		max-width: 64ch;
		margin-top: 0.55rem;
		font-size: 0.875rem;
		line-height: 1.7;
		color: var(--color-text-secondary);
	}

	.home-footer {
		border-top: 1px solid var(--color-border);
		background: var(--color-canvas);
	}

	.footer-wordmark {
		font-size: 1.15rem;
		font-weight: 900;
		line-height: 1;
		letter-spacing: 0.01em;
		color: var(--color-text);
	}

	.footer-link {
		color: var(--color-text-secondary);
		text-decoration-line: underline;
		text-decoration-color: var(--color-border);
		text-underline-offset: 0.3rem;
		transition:
			color 180ms ease,
			text-decoration-color 180ms ease;
	}

	.footer-link:hover {
		color: var(--color-brand-hover);
		text-decoration-color: var(--color-brand-medium);
	}

	@media (min-width: 768px) {
		.ledger-cell + .ledger-cell {
			border-left: 1px solid var(--color-border);
		}
	}

	@media (min-width: 1024px) {
		.map-workbench {
			transform: translateY(0.75rem);
		}

		.evidence-principle {
			position: sticky;
			top: calc(var(--app-nav-height) + 2rem);
		}
	}

	@media (max-width: 767px) {
		.hero-section {
			padding-bottom: 2rem;
		}

		.source-ledger {
			margin-top: 2rem;
		}

		.ledger-cell + .ledger-cell {
			border-top: 1px solid var(--color-border);
		}
	}

	@media (max-width: 639px) {
		.home-page {
			background:
				radial-gradient(circle at 92% 13%, rgba(237, 242, 255, 0.9), transparent 18rem),
				var(--color-canvas);
		}

		h1 {
			font-size: clamp(2.65rem, 14vw, 4rem);
		}

		.hero-actions > a {
			width: 100%;
		}

		.secondary-action {
			border: 1px solid var(--color-border);
			padding-inline: 1rem;
			background: rgba(255, 255, 255, 0.72);
		}

		.map-workbench {
			padding: 0.8rem 0.65rem 0.75rem;
		}

		.map-workbench-header {
			padding-inline: 0.25rem;
		}

		.hero-map :global(figure > div) {
			height: 26rem;
		}

		.hero-map :global(figcaption) {
			padding-inline: 0.25rem;
		}

		.research-link {
			grid-template-columns: 1.65rem minmax(0, 1fr) auto;
			gap: 0.75rem;
			min-height: 7.25rem;
			padding-inline: 0;
		}

		.evidence-heading {
			padding-bottom: 2rem;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.primary-action,
		.secondary-action,
		.primary-action svg,
		.secondary-action svg,
		.research-link,
		.research-arrow {
			transition: none;
		}
	}

	@media (forced-colors: active) {
		.map-workbench,
		.source-ledger,
		.secondary-action {
			background: Canvas;
		}

		.primary-action {
			border: 1px solid ButtonText;
		}
	}
</style>
