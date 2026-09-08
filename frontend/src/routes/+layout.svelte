<script>
	import { onMount } from 'svelte';
	import '../app.css';
	import NavBar from '$lib/components/NavBar.svelte';
	let { children } = $props();
	let hydrated = $state(false);

	onMount(() => {
		hydrated = true;
	});
</script>

<NavBar />

<div class="app-shell-offset" data-app-hydrated={hydrated ? 'true' : 'false'}>
	{@render children()}
</div>
