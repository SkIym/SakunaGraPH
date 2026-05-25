<svelte:head>
	<title>Sakuna PH</title>
	<meta name="description" content="Signup" />
</svelte:head>

<script lang="ts">
	import { page } from '$app/state';
	// import FormModal from '$lib/components/formModal.svelte';

	let user = $state(page.data.user);
	let displayName = user?.user_metadata?.full_name || user?.user_metadata?.name || 'User';

</script>

<section class="pt-10">
</section>

