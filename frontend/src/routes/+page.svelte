<script>
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import SparqlEditor from '$lib/components/SparqlEditor.svelte';
	import ResultsModal from '$lib/components/ResultsModal.svelte';

	// --- Preset example queries ---
	const PRESETS = [
		{
			label: 'Sample triples',
			query: `SELECT ?subject ?predicate ?object
WHERE {
  ?subject ?predicate ?object .
}
LIMIT 10`
		},
		{
			label: 'Distinct classes',
			query: `PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?class
WHERE {
  ?instance a ?class .
}
ORDER BY ?class
LIMIT 20`
		},
		{
			label: 'Disaster events',
			query: `PREFIX sakuna: <https://sakuna.ph/ontology#>
PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?event ?type ?date
WHERE {
  ?event a sakuna:DisasterEvent ;
         sakuna:hasDisasterType ?type ;
         sakuna:startDate       ?date .
}
ORDER BY DESC(?date)
LIMIT 10`
		},
		{
			label: 'Named graphs',
			query: `SELECT DISTINCT ?g
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
LIMIT 20`
		}
	];

	const DEFAULT_QUERY = PRESETS[0].query;

	// --- State ---
	let query = $state(DEFAULT_QUERY);
	let editorKey = $state(0); // increment to remount editor (reset)
	let results = $state(null);
	let loading = $state(false);
	let error = $state('');
	let showModal = $state(false);

	// --- Security: client-side guard (server repeats this check) ---
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

	function loadPreset(preset) {
		query = preset.query;
		editorKey++;
		error = '';
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

<!-- Floating node animation (fixed background) -->
<NodeCanvas />

<!-- Main content -->
<main
	class="relative flex min-h-screen flex-col items-center justify-center px-4 py-16"
	style="z-index:1;"
>
	<!-- ── Brand ── -->
	<div class="mb-10 text-center">
		<!-- Graph icon -->
		<div class="mb-5 flex justify-center">
			<div
				class="flex h-14 w-14 items-center justify-center rounded-2xl bg-indigo-600 shadow-lg shadow-indigo-200"
			>
				<svg
					xmlns="http://www.w3.org/2000/svg"
					width="28"
					height="28"
					viewBox="0 0 24 24"
					fill="none"
					stroke="white"
					stroke-width="1.8"
					stroke-linecap="round"
					stroke-linejoin="round"
				>
					<circle cx="18" cy="5" r="3" />
					<circle cx="6" cy="12" r="3" />
					<circle cx="18" cy="19" r="3" />
					<line x1="8.59" y1="13.51" x2="15.42" y2="17.49" />
					<line x1="15.41" y1="6.51" x2="8.59" y2="10.49" />
				</svg>
			</div>
		</div>

		<h1
			class="text-5xl font-extrabold tracking-tight text-slate-900"
			style="letter-spacing:-0.02em;"
		>
			Sakuna<span class="text-indigo-600">GraPH</span>
		</h1>

		<p
			class="mt-3 text-xs font-semibold uppercase tracking-widest text-slate-400"
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
			<div
				class="flex items-center border-b border-slate-200/80 bg-slate-50/80 px-4 py-3"
			>
				<div class="flex items-center gap-1.5">
					<div class="h-3 w-3 rounded-full bg-red-400"></div>
					<div class="h-3 w-3 rounded-full bg-yellow-400"></div>
					<div class="h-3 w-3 rounded-full bg-green-400"></div>
				</div>
				<span class="ml-3 font-mono text-xs text-slate-400">SPARQL Query</span>
				<span class="ml-auto font-mono text-xs text-slate-300">Ctrl+Enter to run</span>
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
					onclick={() => loadPreset(PRESETS[0])}
					class="text-xs text-slate-400 transition-colors hover:text-slate-600"
				>
					Reset to default
				</button>

				<button
					onclick={runQuery}
					disabled={loading}
					class="flex items-center gap-2 rounded-xl bg-indigo-600 px-5 py-2.5 text-sm font-semibold text-white shadow-sm shadow-indigo-300 transition-all duration-150 hover:bg-indigo-700 active:scale-95 disabled:cursor-not-allowed disabled:opacity-60"
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

		<!-- ── Preset query chips ── -->
		<div class="mt-5 flex flex-wrap justify-center gap-2">
			{#each PRESETS as preset}
				<button
					onclick={() => loadPreset(preset)}
					class="rounded-full border border-slate-200 bg-white/70 px-3 py-1.5 font-mono text-xs text-slate-500 shadow-sm backdrop-blur-sm transition-all hover:border-indigo-300 hover:bg-white hover:text-indigo-600 hover:shadow-indigo-100"
				>
					{preset.label}
				</button>
			{/each}
		</div>
	</div>

	<!-- ── Footer ── -->
	<footer class="mt-14 flex items-center gap-3 text-xs text-slate-300">
		<span>Read-only access</span>
		<span class="h-1 w-1 rounded-full bg-slate-300"></span>
		<span>Powered by GraphDB</span>
		<span class="h-1 w-1 rounded-full bg-slate-300"></span>
		<span>SPARQL 1.1</span>
	</footer>
</main>

<!-- Results modal -->
{#if showModal && results}
	<ResultsModal {results} onclose={() => (showModal = false)} />
{/if}
