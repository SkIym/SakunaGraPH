<script>
	import QueryPage from '$lib/features/query/QueryPage.svelte';

	let { data } = $props();
</script>

<QueryPage initialCompetencyId={data.competencyId} />
