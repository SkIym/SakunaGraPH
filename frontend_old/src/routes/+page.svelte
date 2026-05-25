<script>
	import logo from '$lib/images/philippines_logo.svg';
	import Home from '$lib/components/Home.svelte';
	// import FormModal from '$lib/components/formModal.svelte';
</script>

<svelte:head>
	<link rel="icon" type="image/svg+xml" href={logo}>
	<title>Sakuna PH</title>
	<meta name="description" content="Home Page" />
</svelte:head>

<section>
	<Home />
	<!-- <FormModal name={displayName} source={"setup"} open={false} /> -->
</section>
