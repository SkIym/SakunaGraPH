<script>
	import { onDestroy, tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import AskComposer from './components/AskComposer.svelte';
	import AskConversation from './components/AskConversation.svelte';
	import { ASK_SUGGESTIONS, createAskState } from './state.svelte.js';

	let bottomElement = $state(null);
	const ask = createAskState({
		onUpdated: async () => {
			await tick();
			const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
			bottomElement?.scrollIntoView({ behavior: reduceMotion ? 'auto' : 'smooth' });
		},
	});

	onDestroy(() => ask.cancel());
</script>

<svelte:head>
	<title>Ask · SakunaGraPH</title>
</svelte:head>

<NodeCanvas interactive={false} />

<div class="relative flex flex-col" style="height:calc(100vh - 52px); z-index:1;">
	<AskConversation
		messages={ask.messages}
		suggestions={ASK_SUGGESTIONS}
		announcement={ask.announcement}
		onSend={ask.send}
		bind:bottomElement
	/>
	<AskComposer
		bind:input={ask.input}
		sending={ask.sending}
		onSend={ask.send}
		onCancel={ask.cancel}
	/>
</div>
