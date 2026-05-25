<script lang="ts">
	import { invalidateAll } from '$app/navigation';
  	import { onMount, onDestroy } from 'svelte';

	import '../app.css';
	import { page } from '$app/state';
	import Header from '$lib/components/Header/Header.svelte';
	import Filter from '$lib/components/Filter.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import FormModal from '$lib/components/formModal.svelte';
	import { supabase } from '$lib/supabase';

	let { children, data } = $props();
	
	let showSetup = $state(false);
	let showFilter = $derived(
		page.url.pathname.startsWith('/disasters')
	);

	$effect(() => {
		if (page.url.searchParams.get('setup') === 'true') {
			showSetup = true;
			// console.log("Setup query param detected, showing setup modal.");
		}
	});

	let subscription;

	onMount(() => {
		const { data } = supabase.auth.onAuthStateChange((event) => {
		if (event === 'SIGNED_IN' || event === 'SIGNED_OUT') {
			invalidateAll(); // refresh server data
		}
		});

		subscription = data.subscription;
	});

	onDestroy(() => {
		subscription?.unsubscribe();
	});
</script>

<div class="flex min-h-screen flex-col dark:bg-gray-900">
	<Modal />
	<Header />
	<FormModal bind:open={showSetup} name={data.user?.user_metadata?.full_name} source={"setup"} session={data.session}/>

	<main class="shrink">
			<div class="flex justify-center mt-16 p-4 md:p-10">
				{#if showFilter}
					<Filter/>
				{/if}
				<div class="flex-col">
					{@render children()}
				</div>
			</div>
	</main>
</div>