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

<main class="relative" style="z-index:1;">
	<section
		class="page-viewport mx-auto grid w-full max-w-6xl items-center gap-12 px-5 py-14 lg:grid-cols-[minmax(0,1fr)_minmax(20rem,25rem)] lg:px-8"
	>
		<div class="min-w-0 max-w-3xl">
			<a href="/" class="home-brand inline-block" aria-label="SakunaGraPH home">
				Sakuna<span>GraPH</span>
			</a>
			<h1
				class="mt-8 max-w-[14ch] break-words text-balance font-black leading-[1.03] text-slate-800 [overflow-wrap:anywhere]"
			>
				Explore Philippine disaster records by place.
			</h1>
			<p
				class="mt-6 max-w-[66ch] break-words text-base leading-7 text-slate-600 [overflow-wrap:anywhere] sm:text-lg"
			>
				SakunaGraPH brings records from several disaster-data sources into one knowledge graph.
				Start with a region or province, then open an event to see its dates, locations, and linked
				sources.
			</p>

			<div class="mt-9 flex flex-col gap-3 sm:flex-row">
				<a
					href="/map"
					class="touch-target inline-flex items-center justify-center rounded-lg bg-slate-800 px-5 py-2.5 text-sm font-semibold text-white transition-colors hover:bg-slate-700"
				>
					Explore the map
					<svg
						aria-hidden="true"
						class="ml-2"
						width="17"
						height="17"
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
				<a
					href="/ask"
					class="touch-target inline-flex items-center justify-center rounded-lg border border-slate-300 bg-white px-5 py-2.5 text-sm font-semibold text-slate-700 transition-colors hover:border-slate-400 hover:text-slate-900"
				>
					Ask about the data
				</a>
			</div>

			<p class="mt-5 max-w-[65ch] text-sm leading-6 text-slate-600">
				This is a research and exploration tool, not a live emergency alert service.
			</p>
		</div>

		<HomeMapFigure />
	</section>

	<section
		class="border-t border-slate-200 bg-white/65 px-5 py-10 lg:px-8"
		aria-labelledby="research-tools-title"
	>
		<div class="mx-auto grid max-w-6xl gap-7 lg:grid-cols-[15rem_minmax(0,1fr)] lg:gap-10">
			<div>
				<h2 id="research-tools-title" class="editorial-title text-2xl font-black text-slate-800">
					Research tools
				</h2>
				<p class="mt-2 text-sm leading-6 text-slate-600">
					Use the same graph at a more technical level.
				</p>
			</div>
			<nav
				class="grid min-w-0 divide-y divide-slate-200 md:grid-cols-3 md:divide-x md:divide-y-0"
				aria-label="Research tools"
			>
				{#each researchTools as tool}
					<a
						href={tool.href}
						class="group block min-w-0 py-4 md:px-6 md:py-0 md:first:pl-0 md:last:pr-0"
					>
						<span
							class="flex items-center justify-between gap-4 text-sm font-semibold text-slate-800 group-hover:text-blue-800"
						>
							{tool.name}
							<svg
								aria-hidden="true"
								class="shrink-0"
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
						</span>
						<span class="mt-1 block text-sm leading-5 text-slate-600">{tool.description}</span>
					</a>
				{/each}
			</nav>
		</div>
	</section>

	<section
		class="border-y border-slate-200 bg-white/90 px-5 py-16 lg:px-8"
		aria-labelledby="understand-data-title"
	>
		<div class="mx-auto max-w-6xl">
			<h2
				id="understand-data-title"
				class="editorial-title max-w-[18ch] text-3xl font-black leading-tight text-slate-800 sm:text-4xl"
			>
				Understand the data before using it.
			</h2>
			<p class="mt-4 max-w-[70ch] text-base leading-7 text-slate-600">
				SakunaGraPH combines source records without claiming that every event, place, period, or
				field is complete. Use the linked evidence when timing or precision matters.
			</p>

			<div class="mt-10 grid gap-8 md:grid-cols-3 md:gap-0">
				<section class="md:pr-8">
					<h3 class="text-lg font-bold text-slate-800">Data freshness</h3>
					<p class="mt-3 text-sm leading-6 text-slate-600">
						A site-wide last-updated time is not currently published. Individual source records may
						include obtained and last-updated dates; open an event to inspect what is available.
					</p>
				</section>
				<section class="border-slate-200 md:border-l md:px-8">
					<h3 class="text-lg font-bold text-slate-800">Source coverage</h3>
					<p class="mt-3 text-sm leading-6 text-slate-600">
						The graph integrates records associated with {recordSources.join(', ')}. PSGC provides
						the Philippine administrative geography used to connect locations.
					</p>
					<p class="mt-3 text-sm leading-6 text-slate-600">
						Coverage varies by source, place, period, and field.
					</p>
				</section>
				<section class="border-slate-200 md:border-l md:pl-8">
					<h3 class="text-lg font-bold text-slate-800">Methodology</h3>
					<p class="mt-3 text-sm leading-6 text-slate-600">
						Source files are parsed, normalized into RDF, linked to shared disaster and location
						concepts, and aligned across datasets. Alternate source records remain available for
						inspection.
					</p>
					<div class="mt-4 flex flex-wrap gap-x-5 gap-y-2 text-sm font-semibold">
						<a
							href="/analysis/events"
							class="text-blue-800 underline decoration-blue-200 underline-offset-4 hover:text-blue-950"
							>Inspect event records</a
						>
						<a
							href="/ontology"
							class="text-blue-800 underline decoration-blue-200 underline-offset-4 hover:text-blue-950"
							>Browse the ontology</a
						>
					</div>
				</section>
			</div>
		</div>
	</section>

	<footer class="px-5 py-10 lg:px-8">
		<div class="mx-auto flex max-w-6xl flex-col gap-8 sm:flex-row sm:items-end sm:justify-between">
			<div>
				<h2 class="text-sm font-bold text-slate-800">About SakunaGraPH</h2>
				<p class="mt-2 max-w-[62ch] text-sm leading-6 text-slate-600">
					Built by Judelle Clareese E. Gaza and Abram Josh C. Marcelo at the UP Diliman Web Science
					Laboratory.
				</p>
			</div>
			<div class="flex flex-wrap gap-x-5 gap-y-2 text-sm">
				<a
					href="https://github.com/ElleDiablo"
					target="_blank"
					rel="noopener noreferrer"
					class="text-slate-700 underline decoration-slate-300 underline-offset-4 hover:text-slate-950"
					>Judelle on GitHub</a
				>
				<a
					href="https://github.com/SkIym"
					target="_blank"
					rel="noopener noreferrer"
					class="text-slate-700 underline decoration-slate-300 underline-offset-4 hover:text-slate-950"
					>Abram on GitHub</a
				>
			</div>
		</div>
	</footer>
</main>

<style>
	.home-brand,
	.editorial-title,
	h1 {
		font-family: 'Playfair Display', Georgia, serif;
	}

	.home-brand {
		font-size: clamp(1.65rem, 4vw, 2.35rem);
		font-weight: 900;
		line-height: 1;
		letter-spacing: 0.01em;
		color: var(--color-text);
		text-decoration: none;
	}

	.home-brand span {
		color: var(--color-brand);
	}

	h1 {
		font-size: clamp(2.55rem, 6.6vw, 4.8rem);
	}
</style>
