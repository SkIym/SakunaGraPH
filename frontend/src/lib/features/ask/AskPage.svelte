<script>
	import { onDestroy, tick } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
	import AskComposer from './components/AskComposer.svelte';
	import AskConversation from './components/AskConversation.svelte';
	import { ASK_QUESTION_MAX_LENGTH, ASK_SUGGESTIONS, createAskState } from './state.svelte.js';

	let bottomElement = $state(null);
	let scrollFrame = null;
	const ask = createAskState({
		onUpdated: async () => {
			await tick();
			if (scrollFrame !== null) return;
			scrollFrame = window.requestAnimationFrame(() => {
				scrollFrame = null;
				const scroller = document.getElementById('messages-scroll');
				if (!scroller || !bottomElement) return;
				const distanceFromBottom =
					scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight;
				if (distanceFromBottom <= 240) {
					bottomElement.scrollIntoView({ behavior: 'auto', block: 'end' });
				}
			});
		},
	});

	onDestroy(() => {
		ask.cancel();
		if (scrollFrame !== null) window.cancelAnimationFrame(scrollFrame);
	});
</script>

<svelte:head>
	<title>Ask · SakunaGraPH</title>
</svelte:head>

<NodeCanvas active={ask.sending} />

<div class="ask-viewport relative flex min-h-0 flex-col" style="z-index:1;">
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
		error={ask.inputError}
		maxLength={ASK_QUESTION_MAX_LENGTH}
		onSend={ask.send}
		onCancel={ask.cancel}
	/>
</div>
