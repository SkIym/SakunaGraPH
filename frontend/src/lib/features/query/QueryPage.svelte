<script>
	import { onDestroy } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import SparqlEditor from '$lib/components/SparqlEditor.svelte';
	import { COMPETENCY_QUESTIONS } from '$lib/competency_queries.js';
	import {
		createQueryWorkbench,
		MAX_QUERY_LENGTH,
		QUERY_PRESETS,
	} from '$lib/features/home/queryWorkbench.svelte.js';

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
	<title>SPARQL query workspace · SakunaGraPH</title>
	<meta
		name="description"
		content="Run read-only SPARQL SELECT queries against the currently loaded SakunaGraPH knowledge graph."
	/>
</svelte:head>

<NodeCanvas active={workbench.loading} />
<svelte:window onkeydown={workbench.handleEditorKeydown} />

<main class="relative mx-auto w-full max-w-5xl px-4 py-10 sm:px-6 sm:py-14" style="z-index:1;">
	<header class="max-w-3xl">
		<h1
			class="font-black leading-tight text-slate-800"
			style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(2.25rem,6vw,4.5rem);"
		>
			SPARQL query workspace
		</h1>
		<p class="mt-4 max-w-[68ch] text-base leading-7 text-slate-600">
			Run read-only SPARQL SELECT queries against the graph currently loaded into SakunaGraPH. This
			workspace is intended for researchers familiar with RDF and SPARQL.
		</p>
		<p class="mt-2 text-sm leading-6 text-slate-600">
			New to the data? <a
				href="/map"
				class="font-semibold text-blue-800 underline decoration-blue-200 underline-offset-4 hover:text-blue-950"
				>Explore the map instead</a
			>.
		</p>
	</header>

	<section class="mt-8" aria-labelledby="query-editor-title">
		<div
			class="overflow-hidden rounded-2xl border border-slate-200/80"
			style="background:var(--color-surface); box-shadow:var(--shadow-surface);"
		>
			<div
				class="flex flex-wrap items-center gap-x-4 gap-y-1 border-b border-slate-200/80 bg-slate-50/90 px-4 py-3"
			>
				<h2 id="query-editor-title" class="font-mono text-xs font-semibold text-slate-700">
					Read-only SPARQL editor
				</h2>
				<span class="ml-auto font-mono text-xs text-slate-600">Ctrl or ⌘ + Enter to run</span>
			</div>

			<div class="border-b border-slate-100 px-4 py-3">
				<label for="query-example" class="mb-1.5 block text-xs font-semibold text-slate-700">
					Start from an example
				</label>
				<div class="relative">
					<select
						id="query-example"
						bind:value={workbench.selectedCompetency}
						onchange={workbench.selectCompetency}
						class="min-h-11 w-full cursor-pointer appearance-none rounded-lg border border-slate-300 bg-white px-3 py-2 pr-9 text-sm text-slate-700 transition-colors focus:border-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-100"
					>
						<option value="">Write your own query</option>
						{#each COMPETENCY_QUESTIONS as question, index}
							<option value={String(index)}>{question.label}</option>
						{/each}
					</select>
					<svg
						aria-hidden="true"
						class="pointer-events-none absolute top-1/2 right-3 -translate-y-1/2 text-slate-500"
						width="16"
						height="16"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
						stroke-linecap="round"
						stroke-linejoin="round"
					>
						<path d="m6 9 6 6 6-6" />
					</svg>
				</div>
			</div>

			<div>
				{#key workbench.editorKey}
					<SparqlEditor bind:value={workbench.query} maxLength={MAX_QUERY_LENGTH} />
				{/key}
			</div>
			<p
				id="query-character-limit"
				class="px-4 pt-2 text-right font-mono text-[0.6875rem] text-slate-500"
			>
				{workbench.query.length.toLocaleString()} / {MAX_QUERY_LENGTH.toLocaleString()} characters
			</p>

			{#if workbench.error}
				<div
					role="alert"
					aria-live="assertive"
					class="mx-4 mb-3 break-words rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm leading-6 text-red-800 [overflow-wrap:anywhere]"
				>
					{workbench.error}
				</div>
			{/if}

			<div class="flex flex-wrap items-center justify-between gap-3 px-4 pb-4">
				<button
					type="button"
					onclick={workbench.reset}
					class="touch-target rounded-lg px-3 text-sm font-medium text-slate-700 transition-colors hover:bg-slate-100 hover:text-slate-900"
				>
					Reset query
				</button>

				<button
					type="button"
					bind:this={runButton}
					onclick={workbench.run}
					disabled={workbench.loading}
					class="touch-target flex items-center gap-2 rounded-xl bg-[var(--color-action)] px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-[var(--color-action-hover)] disabled:cursor-not-allowed disabled:opacity-60"
				>
					{#if workbench.loading}
						<svg
							aria-hidden="true"
							class="animate-spin"
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
						Running query…
					{:else}
						Run query
					{/if}
				</button>
			</div>
		</div>

		<div class="mt-4 flex flex-wrap gap-2" aria-label="Sample queries">
			{#each QUERY_PRESETS as preset}
				<button
					type="button"
					onclick={() => workbench.loadPreset(preset)}
					class="touch-target rounded-full border border-slate-300 bg-white px-3 py-1.5 font-mono text-xs text-slate-700 transition-colors hover:border-blue-700 hover:text-blue-800"
				>
					{preset.label}
				</button>
			{/each}
		</div>
	</section>

	<section class="mt-12 border-t border-slate-200 pt-8" aria-labelledby="query-scope-title">
		<h2 id="query-scope-title" class="text-lg font-bold text-slate-800">Query scope</h2>
		<p class="mt-2 max-w-[70ch] text-sm leading-6 text-slate-600">
			Only SELECT queries are supported. Results reflect the graph currently loaded by the
			SakunaGraPH data pipeline; they do not establish complete or real-time coverage.
		</p>
	</section>
</main>

{#if workbench.resultsOpen && workbench.results && ResultsModalComponent}
	<ResultsModalComponent
		results={workbench.results}
		onclose={workbench.closeResults}
		returnFocus={runButton}
	/>
{/if}
