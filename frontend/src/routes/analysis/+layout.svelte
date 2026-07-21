<script>
	import AnalysisShell from '$lib/features/analysis/AnalysisShell.svelte';

	let { children } = $props();
</script>

<AnalysisShell>{@render children()}</AnalysisShell>
