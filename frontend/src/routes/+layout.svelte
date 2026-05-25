<script>
	import '../app.css';
	import NavBar from '$lib/components/NavBar.svelte';
	let { children } = $props();
</script>

<NavBar />

<div style="padding-top:52px;">
	{@render children()}
</div>
