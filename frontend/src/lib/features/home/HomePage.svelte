<script>
	import { onDestroy } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import SparqlEditor from '$lib/components/SparqlEditor.svelte';
	import { COMPETENCY_QUESTIONS } from '$lib/competency_queries.js';
	import { createQueryWorkbench, QUERY_PRESETS } from './queryWorkbench.svelte.js';

	const GraPH_color = '#305bb2';

	const workbench = createQueryWorkbench();
	let ResultsModalComponent = $state(null);
	let runButton = $state(null);

	$effect(() => {
		if (!workbench.results || ResultsModalComponent) return;
		void import('$lib/components/ResultsModal.svelte').then(
			({ default: component }) => (ResultsModalComponent = component),
		);
	});

	onDestroy(() => workbench.cancel());
</script>

<svelte:head>
	<title>SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<main class="relative" style="z-index:1;">
	<!-- ── Hero section: full viewport ── -->
	<section
		class="relative flex min-h-[calc(100vh-52px)] flex-col items-center justify-center px-4 py-12"
	>
		<!-- ── Brand ── -->
		<div class="mb-8 text-center">
			<h1
				style="font-family:'Playfair Display',Georgia,serif; font-weight:900; font-size:clamp(3.5rem,8vw,6.5rem); line-height:1.05; letter-spacing:0.055em; color:#1e293b;"
			>
				Sakuna<span style="color:{GraPH_color};">GraPH</span>
			</h1>
			<p class="mt-3 text-xs font-semibold uppercase text-slate-600" style="letter-spacing:0.18em;">
				An Ontology-Based Knowledge Graph for Disaster Data Integration
			</p>
		</div>

		<!-- ── Editor card ── -->
		<div class="w-full max-w-3xl">
			<div
				class="overflow-hidden rounded-2xl border border-slate-200/80 bg-white/85 shadow-xl shadow-slate-200/60"
				style="backdrop-filter:blur(12px);"
			>
				<!-- macOS-style title bar -->
				<div class="flex items-center border-b border-slate-200/80 bg-slate-50/80 px-4 py-3">
					<div class="flex items-center gap-1.5">
						<div class="h-3 w-3 rounded-full bg-red-400"></div>
						<div class="h-3 w-3 rounded-full bg-yellow-400"></div>
						<div class="h-3 w-3 rounded-full bg-green-400"></div>
					</div>
					<span class="ml-3 font-mono text-xs text-slate-600">SPARQL Query</span>
					<span class="ml-auto font-mono text-xs text-slate-600">Ctrl+Enter to run</span>
				</div>

				<!-- Competency question dropdown -->
				<div class="border-b border-slate-100 px-4 py-2.5">
					<div class="relative">
						<select
							bind:value={workbench.selectedCompetency}
							onchange={workbench.selectCompetency}
							aria-label="SPARQL query preset"
							class="w-full cursor-pointer appearance-none rounded-lg border border-slate-200 bg-white px-3 py-2 pr-8 text-xs text-slate-600 transition-all focus:border-indigo-300 focus:outline-none focus:ring-1 focus:ring-indigo-100"
						>
							<option value="">Custom Query</option>
							{#each COMPETENCY_QUESTIONS as cq, i}
								<option value={String(i)}>{cq.label}</option>
							{/each}
						</select>
						<div
							class="pointer-events-none absolute inset-y-0 right-2.5 flex items-center text-slate-400"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="13"
								height="13"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2.5"
								stroke-linecap="round"
								stroke-linejoin="round"
							>
								<path d="m6 9 6 6 6-6" />
							</svg>
						</div>
					</div>
				</div>

				<!-- CodeMirror editor -->
				<!-- svelte-ignore a11y_no_static_element_interactions -->
				<div onkeydown={workbench.handleEditorKeydown}>
					{#key workbench.editorKey}
						<SparqlEditor bind:value={workbench.query} />
					{/key}
				</div>

				<!-- Error banner -->
				{#if workbench.error}
					<div
						class="mx-4 mb-3 flex items-start gap-2 rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700"
					>
						<svg
							xmlns="http://www.w3.org/2000/svg"
							width="15"
							height="15"
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							stroke-width="2"
							stroke-linecap="round"
							stroke-linejoin="round"
							class="mt-px shrink-0"
						>
							<circle cx="12" cy="12" r="10" />
							<line x1="12" y1="8" x2="12" y2="12" />
							<line x1="12" y1="16" x2="12.01" y2="16" />
						</svg>
						{workbench.error}
					</div>
				{/if}

				<!-- Action bar -->
				<div class="flex items-center justify-between px-4 pb-4">
					<button
						onclick={workbench.reset}
						class="text-xs text-slate-400 transition-colors hover:text-slate-600"
					>
						Reset
					</button>

					<button
						bind:this={runButton}
						onclick={workbench.run}
						disabled={workbench.loading}
						class="flex items-center gap-2 rounded-xl bg-red-700 px-5 py-2.5 text-sm font-semibold text-white shadow-sm shadow-red-200 transition-all duration-150 hover:bg-red-800 active:scale-95 disabled:cursor-not-allowed disabled:opacity-60"
					>
						{#if workbench.loading}
							<svg
								class="animate-spin"
								xmlns="http://www.w3.org/2000/svg"
								width="15"
								height="15"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="2.5"
								stroke-linecap="round"
								stroke-linejoin="round"
							>
								<path d="M21 12a9 9 0 1 1-6.219-8.56" />
							</svg>
							Running…
						{:else}
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="14"
								height="14"
								viewBox="0 0 24 24"
								fill="currentColor"
								stroke="none"
							>
								<polygon points="6 3 20 12 6 21 6 3" />
							</svg>
							Run Query
						{/if}
					</button>
				</div>
			</div>

			<!-- Quick-access preset chips -->
			<div class="mt-4 flex flex-wrap justify-center gap-2">
				{#each QUERY_PRESETS as preset}
					<button
						onclick={() => workbench.loadPreset(preset)}
						class="rounded-full border border-slate-200 bg-white/70 px-3 py-1.5 font-mono text-xs text-slate-500 shadow-sm backdrop-blur-sm transition-all hover:border-indigo-300 hover:bg-white hover:text-indigo-600"
					>
						{preset.label}
					</button>
				{/each}
			</div>
		</div>

		<!-- Scroll hint -->
		<div class="absolute bottom-6 flex flex-col items-center gap-1 animate-bounce opacity-40">
			<span class="text-[10px] uppercase tracking-widest text-slate-600">scroll</span>
			<svg
				xmlns="http://www.w3.org/2000/svg"
				width="16"
				height="16"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
				stroke-linecap="round"
				stroke-linejoin="round"
				class="text-slate-400"
			>
				<path d="m6 9 6 6 6-6" />
			</svg>
		</div>
	</section>

	<!-- ── Team section ── -->
	<section class="flex min-h-[calc(100vh-52px)] flex-col items-center justify-center px-4 py-20">
		<p class="text-[10px] font-bold uppercase tracking-widest text-slate-600">Built by</p>
		<div class="mt-8 flex items-start gap-16">
			<div class="flex flex-col items-center gap-4">
				<img
					src="/elle-416.jpg"
					srcset="/elle-208.jpg 208w, /elle-416.jpg 416w"
					alt="Judelle Clareese E. Gaza"
					width="208"
					height="208"
					loading="lazy"
					decoding="async"
					sizes="208px"
					class="h-52 w-52 rounded-full object-cover ring-2 ring-slate-200 shadow-lg"
				/>
				<div class="text-center">
					<p class="text-base font-semibold text-slate-700">Judelle Clareese E. Gaza</p>
					<p class="mt-0.5 text-xs text-slate-600">UPD BS Computer Science</p>
					<p class="text-xs text-slate-600">Web Science Laboratory</p>
					<div class="mt-3 flex items-center justify-center gap-3">
						<a
							href="https://www.instagram.com/gaza_judelle/"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Judelle Gaza on Instagram"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-pink-100 hover:text-pink-600 hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><rect width="20" height="20" x="2" y="2" rx="5" ry="5" /><path
									d="M16 11.37A4 4 0 1 1 12.63 8 4 4 0 0 1 16 11.37z"
								/><line x1="17.5" x2="17.51" y1="6.5" y2="6.5" /></svg
							>
						</a>
						<a
							href="https://github.com/ElleDiablo"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Judelle Gaza on GitHub"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-slate-800 hover:text-white hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><path
									d="M15 22v-4a4.8 4.8 0 0 0-1-3.2c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.4 5.4 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"
								/><path d="M9 18c-4.51 2-5-2-7-2" /></svg
							>
						</a>
						<a
							href="https://www.linkedin.com/in/judelle-gaza/"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Judelle Gaza on LinkedIn"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-blue-600 hover:text-white hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><path
									d="M16 8a6 6 0 0 1 6 6v7h-4v-7a2 2 0 0 0-2-2 2 2 0 0 0-2 2v7h-4v-7a6 6 0 0 1 6-6z"
								/><rect width="4" height="12" x="2" y="9" /><circle cx="4" cy="4" r="2" /></svg
							>
						</a>
					</div>
				</div>
			</div>
			<div class="flex flex-col items-center gap-4">
				<img
					src="/abram-416.jpg"
					srcset="/abram-208.jpg 208w, /abram-416.jpg 416w"
					alt="Abram Josh C. Marcelo"
					width="208"
					height="208"
					loading="lazy"
					decoding="async"
					sizes="208px"
					class="h-52 w-52 rounded-full object-cover ring-2 ring-slate-200 shadow-lg"
				/>
				<div class="text-center">
					<p class="text-base font-semibold text-slate-700">Abram Josh C. Marcelo</p>
					<p class="mt-0.5 text-xs text-slate-600">UPD BS Computer Science</p>
					<p class="text-xs text-slate-600">Web Science Laboratory</p>
					<div class="mt-3 flex items-center justify-center gap-3">
						<a
							href="https://www.instagram.com/abrammsq/?hl=en"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Abram Marcelo on Instagram"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-pink-100 hover:text-pink-600 hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><rect width="20" height="20" x="2" y="2" rx="5" ry="5" /><path
									d="M16 11.37A4 4 0 1 1 12.63 8 4 4 0 0 1 16 11.37z"
								/><line x1="17.5" x2="17.51" y1="6.5" y2="6.5" /></svg
							>
						</a>
						<a
							href="https://github.com/SkIym"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Abram Marcelo on GitHub"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-slate-800 hover:text-white hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><path
									d="M15 22v-4a4.8 4.8 0 0 0-1-3.2c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.4 5.4 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"
								/><path d="M9 18c-4.51 2-5-2-7-2" /></svg
							>
						</a>
						<a
							href="https://www.linkedin.com/in/ajcmarcelo/"
							target="_blank"
							rel="noopener noreferrer"
							aria-label="Abram Marcelo on LinkedIn"
							class="flex h-9 w-9 items-center justify-center rounded-full bg-slate-100 text-slate-500 transition-all hover:bg-blue-600 hover:text-white hover:scale-110"
						>
							<svg
								xmlns="http://www.w3.org/2000/svg"
								width="18"
								height="18"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								stroke-width="1.8"
								stroke-linecap="round"
								stroke-linejoin="round"
								><path
									d="M16 8a6 6 0 0 1 6 6v7h-4v-7a2 2 0 0 0-2-2 2 2 0 0 0-2 2v7h-4v-7a6 6 0 0 1 6-6z"
								/><rect width="4" height="12" x="2" y="9" /><circle cx="4" cy="4" r="2" /></svg
							>
						</a>
					</div>
				</div>
			</div>
		</div>

		<!-- Footer -->
		<footer class="mt-20 flex items-center gap-3 text-xs text-slate-600">
			<span>Read-only access</span>
			<span class="h-1 w-1 rounded-full bg-slate-300"></span>
			<span>Powered by GraphDB</span>
			<span class="h-1 w-1 rounded-full bg-slate-300"></span>
			<span>SPARQL 1.1</span>
		</footer>
	</section>
</main>

{#if workbench.resultsOpen && workbench.results && ResultsModalComponent}
	<ResultsModalComponent
		results={workbench.results}
		onclose={workbench.closeResults}
		returnFocus={runButton}
	/>
{/if}
