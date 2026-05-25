<script>
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import SparqlEditor from '$lib/components/SparqlEditor.svelte';
	import ResultsModal from '$lib/components/ResultsModal.svelte';
	import { COMPETENCY_QUESTIONS } from '$lib/competency_queries.js';

	const GraPH_color = '#305bb2' 

	// --- Default query shown on load ---
	const DEFAULT_QUERY = `PREFIX :     <https://sakuna.ph/>

SELECT DISTINCT ?event ?disasterType
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType .
}
LIMIT 10`;

	// --- Quick-access presets shown as chips ---
	const PRESETS = [
		{
			label: 'Disaster events',
			query: DEFAULT_QUERY
		},
		{
			label: 'Events by type',
			query: `PREFIX :     <https://sakuna.ph/>

SELECT ?disasterType (COUNT(DISTINCT ?event) AS ?count)
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType .
}
GROUP BY ?disasterType
ORDER BY DESC(?count)
LIMIT 10`
		},
		{
			label: 'Named events',
			query: `PREFIX :    <https://sakuna.ph/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

SELECT DISTINCT ?event ?name ?disasterType
WHERE {
  ?event a :DisasterEvent ;
         :hasDisasterType ?disasterType ;
         owl:topDataProperty ?name .
  FILTER(datatype(?name) = xsd:string)
}
LIMIT 10`
		},
		{
			label: 'Ontology classes',
			query: `PREFIX : <https://sakuna.ph/>

SELECT DISTINCT ?class (COUNT(?inst) AS ?count)
WHERE {
  ?inst a ?class .
  FILTER(STRSTARTS(STR(?class), "https://sakuna.ph/"))
}
GROUP BY ?class
ORDER BY DESC(?count)
LIMIT 15`
		}
	];

	// --- State ---
	let query = $state(DEFAULT_QUERY);
	let editorKey = $state(0);
	let selectedCQ = $state('');
	let results = $state(null);
	let loading = $state(false);
	let error = $state('');
	let showModal = $state(false);

	// --- Security: client-side write-op guard (server enforces this too) ---
	const WRITE_PATTERNS = [
		/\bINSERT\b/i,
		/\bDELETE\b/i,
		/\bCLEAR\b/i,
		/\bDROP\b/i,
		/\bCREATE\s+GRAPH\b/i,
		/\bLOAD\b/i,
		/\bCOPY\s+GRAPH\b/i,
		/\bMOVE\s+GRAPH\b/i
	];

	function isWriteOp(q) {
		return WRITE_PATTERNS.some((p) => p.test(q));
	}

	function loadQuery(q) {
		query = q;
		editorKey++;
		error = '';
	}

	function onCQSelect() {
		if (!selectedCQ) return;
		loadQuery(COMPETENCY_QUESTIONS[parseInt(selectedCQ)].query);
	}

	function loadPreset(preset) {
		selectedCQ = '';
		loadQuery(preset.query);
	}

	async function runQuery() {
		error = '';
		const trimmed = query.trim();
		if (!trimmed) {
			error = 'Please enter a SPARQL query.';
			return;
		}
		if (isWriteOp(trimmed)) {
			error =
				'Write operations (INSERT, DELETE, CLEAR, DROP, etc.) are not permitted. This is a read-only interface.';
			return;
		}
		loading = true;
		try {
			const res = await fetch('/api/sparql', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ query: trimmed })
			});
			const data = await res.json();
			if (!res.ok) {
				error = data.error ?? 'An error occurred while processing the query.';
				return;
			}
			results = data;
			showModal = true;
		} catch {
			error = 'Could not reach the server. Please try again.';
		} finally {
			loading = false;
		}
	}

	function handleKeyDown(e) {
		if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
			e.preventDefault();
			runQuery();
		}
	}
</script>

<svelte:head>
	<title>SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<main
	class="relative flex min-h-[calc(100vh-52px)] flex-col items-center justify-center px-4 py-12"
	style="z-index:1;"
>
	<!-- ── Brand ── -->
	<div class="mb-8 text-center">
		<h1
			style="font-family:'Playfair Display',Georgia,serif; font-weight:900; font-size:clamp(3.5rem,8vw,6.5rem); line-height:1.05; letter-spacing:0.055em; color:#1e293b;"
		>
			Sakuna<span style="color:{GraPH_color};">GraPH</span>
		</h1>
		<p
			class="mt-3 text-xs font-semibold uppercase text-slate-400"
			style="letter-spacing:0.18em;"
		>
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
				<span class="ml-3 font-mono text-xs text-slate-400">SPARQL Query</span>
				<span class="ml-auto font-mono text-xs text-slate-300">Ctrl+Enter to run</span>
			</div>

			<!-- Competency question dropdown -->
			<div class="border-b border-slate-100 px-4 py-2.5">
				<div class="relative">
					<select
						bind:value={selectedCQ}
						onchange={onCQSelect}
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
			<div onkeydown={handleKeyDown}>
				{#key editorKey}
					<SparqlEditor bind:value={query} />
				{/key}
			</div>

			<!-- Error banner -->
			{#if error}
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
					{error}
				</div>
			{/if}

			<!-- Action bar -->
			<div class="flex items-center justify-between px-4 pb-4">
				<button
					onclick={() => {
						selectedCQ = '';
						loadQuery(DEFAULT_QUERY);
					}}
					class="text-xs text-slate-400 transition-colors hover:text-slate-600"
				>
					Reset
				</button>

				<button
					onclick={runQuery}
					disabled={loading}
					class="flex items-center gap-2 rounded-xl bg-red-700 px-5 py-2.5 text-sm font-semibold text-white shadow-sm shadow-red-200 transition-all duration-150 hover:bg-red-800 active:scale-95 disabled:cursor-not-allowed disabled:opacity-60"
				>
					{#if loading}
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
			{#each PRESETS as preset}
				<button
					onclick={() => loadPreset(preset)}
					class="rounded-full border border-slate-200 bg-white/70 px-3 py-1.5 font-mono text-xs text-slate-500 shadow-sm backdrop-blur-sm transition-all hover:border-indigo-300 hover:bg-white hover:text-indigo-600"
				>
					{preset.label}
				</button>
			{/each}
		</div>
	</div>

	<!-- Footer -->
	<footer class="mt-12 flex items-center gap-3 text-xs text-slate-300">
		<span>Read-only access</span>
		<span class="h-1 w-1 rounded-full bg-slate-300"></span>
		<span>Powered by GraphDB</span>
		<span class="h-1 w-1 rounded-full bg-slate-300"></span>
		<span>SPARQL 1.1</span>
	</footer>
</main>

{#if showModal && results}
	<ResultsModal {results} onclose={() => (showModal = false)} />
{/if}
