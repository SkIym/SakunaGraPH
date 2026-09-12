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

<NodeCanvas active={ask.sending} />

<main class="ask-workspace ask-viewport">
	<div class="ask-frame">
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
</main>

<style>
	.ask-workspace {
		position: relative;
		z-index: 1;
		display: grid;
		min-height: 0;
		place-items: stretch center;
		overflow: hidden;
		background: rgb(248 250 252 / 0.34);
	}

	.ask-frame {
		display: flex;
		width: min(100%, 72rem);
		height: 100%;
		min-height: 0;
		flex-direction: column;
		border-inline: 1px solid rgb(226 232 240 / 0.84);
		background: rgb(255 255 255 / 0.78);
		box-shadow: 0 18px 50px -40px rgb(0 56 168 / 0.45);
	}

	@media (max-width: 72rem) {
		.ask-frame {
			border-inline: 0;
			box-shadow: none;
		}
	}

	@media (forced-colors: active) {
		.ask-workspace,
		.ask-frame {
			background: Canvas;
		}
	}
</style>
