<script>
	// @ts-nocheck
	import { page } from '$app/state';

	import logo from '$lib/images/philippines_logo.svg';
	import TableComponent from '$lib/components/table-page/TableComponent.svelte';
	import { getFilteredCsvData } from "$lib/demo_data/filteredData.svelte";

	import { Button, Tooltip } from "flowbite-svelte";

	let user = $derived(page.data.user);

	// Toggle this while testing
	let useSparql = $state(false);

	let sparqlRows = $state([]);
	let sparqlLoading = $state(false);
	let sparqlError = $state(null);

	let rawDisasterData = $derived(
		useSparql ? sparqlRows : getFilteredCsvData()
	);

	const SPARQL_ENDPOINT = "/api/sparql";

	const PREFIXES = `
		PREFIX : <https://sakuna.ph/>
		PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
		PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
		PREFIX owl: <http://www.w3.org/2002/07/owl#>
		PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
		PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
		PREFIX prov: <http://www.w3.org/ns/prov#>
		PREFIX qudt: <http://qudt.org/schema/qudt/>
		`;

		function buildMajorEventsQuery() {
			return `
			${PREFIXES}

			SELECT ?event ?name ?start
			WHERE {
				?event a :MajorEvent .
				OPTIONAL { ?event :eventName ?name . }
				OPTIONAL { ?event :startDate ?start . }
			}
			ORDER BY DESC(?start)
			LIMIT 50
			`;
		}

	function flattenBinding(row) {
		return {
			event: row.event?.value ?? "",
			name: row.name?.value ?? "",
			start: row.start?.value ?? "",
		};
	}

	async function runSparqlTest() {
		console.log("Test print.");
		sparqlLoading = true;
		sparqlError = null;

		try {
			const query = buildMajorEventsQuery();

			const response = await fetch(SPARQL_ENDPOINT, {
				method: "POST",
				headers: {
					"Content-Type": "application/sparql-query",
					"Accept": "application/sparql-results+json"
				},
				body: query
			});

			if (!response.ok) {
				throw new Error(`SPARQL request failed: ${response.status} ${response.statusText}`);
			}

			const json = await response.json();

			sparqlRows = json.results.bindings.map(flattenBinding);
			useSparql = true;

			console.log("SPARQL rows:", sparqlRows);
		} catch (err) {
			console.error(err);
			sparqlError = err.message;
		} finally {
			sparqlLoading = false;
		}
	}

	let processedRows = $derived.by(() => {
		return rawDisasterData.map(row => ({
			...row,
			monetary_damage: row.monetary_damage
				? (Number(row.monetary_damage) / 1000000).toFixed(2)
				: row.monetary_damage ?? null
		}));
	});

	const PER_PAGE = 50;
	let currentPage = $state(0);
	let totalPages = $derived(Math.max(1, Math.ceil(processedRows.length / PER_PAGE)));
	let pageRows = $derived(processedRows.slice(currentPage * PER_PAGE, (currentPage + 1) * PER_PAGE));

	$effect(() => {
		// reset to first page when filtered data changes
		processedRows;
		currentPage = 0;
	});

	function jsonToCSV(json) {
		if (!json.length) return "";

		const headers = Object.keys(json[0]);

		const rows = json.map(obj =>
			headers.map(header => {
				const value = obj[header] ?? "";
				return `"${String(value).replace(/"/g, '""')}"`;
			}).join(",")
		);

		return [headers.join(","), ...rows].join("\n");
	}

	function downloadCSV() {
		const csv = jsonToCSV(processedRows);

		const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
		const url = URL.createObjectURL(blob);

		const link = document.createElement("a");
		link.href = url;
		link.setAttribute("download", "disaster_data.csv");
		document.body.appendChild(link);
		link.click();

		document.body.removeChild(link);
		URL.revokeObjectURL(url);
	}
</script>

<svelte:head>
	<title>Disaster</title>
	<meta name="description" content="table view" />
	<link rel="icon" href={logo} />
</svelte:head>

<div class="flex justify-center">
    <div class="flex flex-col my-6 w-full max-w-300">
        <h2 class="mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 font-bold text-xl md:text-2xl lg:text-3xl text-gray-900 dark:text-gray-100">
            TABULAR FORM
        </h2>
        <p class="text-justify dark:text-gray-300 text-gray-700">
            The tabular form provides a structured and organized way to view disaster data in the Philippines.
            Users can easily access and analyze information related to various disasters, including their
            occurrences, impacts, and responses. This format allows for efficient data comparison and retrieval.
        </p>
    </div>
</div>

<section class="mx-auto w-full max-w-180 md:max-w-200 lg:max-w-300 px-4">
    <div class="flex flex-wrap items-center justify-between gap-2 mb-4">
		<!--
        <div class="flex gap-2 flex-wrap">

            <Button onclick={runSparqlTest} size="sm" color="alternative">
                Test SPARQL Data
            </Button>
            <Button onclick={() => useSparql = false} size="sm" color="alternative">
                Use CSV Data
            </Button>
        </div>
	-->
        {#if !user}
            <div>
                <Button size="sm" color="blue" disabled>Download</Button>
                <Tooltip>Please sign in to download the data.</Tooltip>
            </div>
        {:else}
            <Button size="sm" color="blue" onclick={downloadCSV}>Download</Button>
        {/if}
    </div>

    {#if sparqlLoading}
        <p class="text-gray-600 dark:text-gray-400 text-sm mb-2">Loading SPARQL data...</p>
    {/if}
    {#if sparqlError}
        <p class="text-red-600 text-sm mb-2">{sparqlError}</p>
    {/if}

    <div class="max-w-full w-full mb-4">
        <TableComponent data={pageRows} />
    </div>

    <div class="flex items-center justify-between gap-2 mb-10 text-sm text-gray-600 dark:text-gray-400">
        <span>{processedRows.length.toLocaleString()} rows total &mdash; page {currentPage + 1} of {totalPages}</span>
        <div class="flex gap-2">
            <Button size="xs" color="alternative" disabled={currentPage === 0} onclick={() => currentPage = 0}>«</Button>
            <Button size="xs" color="alternative" disabled={currentPage === 0} onclick={() => currentPage -= 1}>‹ Prev</Button>
            <Button size="xs" color="alternative" disabled={currentPage >= totalPages - 1} onclick={() => currentPage += 1}>Next ›</Button>
            <Button size="xs" color="alternative" disabled={currentPage >= totalPages - 1} onclick={() => currentPage = totalPages - 1}>»</Button>
        </div>
    </div>
</section>

