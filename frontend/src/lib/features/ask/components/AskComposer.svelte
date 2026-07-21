<script>
	let { input = $bindable(''), sending = false, onSend = () => {}, onCancel = () => {} } = $props();

	function handleKeydown(event) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			onSend();
		}
	}
</script>

<div
	class="flex-shrink-0 border-t border-slate-200/80 bg-white/88 px-4 py-3"
	style="backdrop-filter:blur(12px);"
>
	<div class="mx-auto flex w-full max-w-3xl items-end gap-3">
		<textarea
			bind:value={input}
			onkeydown={handleKeydown}
			aria-label="Question"
			rows="1"
			placeholder="Ask a question about Philippine disaster data…"
			class="flex-1 resize-none rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-sm leading-relaxed text-slate-800 shadow-sm outline-none transition-colors placeholder-slate-400 focus:border-slate-400 focus:ring-2 focus:ring-slate-200"
			style="max-height:140px; overflow-y:auto; field-sizing:content;"></textarea>
		<button
			type="button"
			onclick={() => (sending ? onCancel() : onSend())}
			disabled={!sending && !input.trim()}
			class="flex-shrink-0 rounded-xl bg-slate-800 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-all hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
		>
			{#if sending}
				Cancel
			{:else}
				Send
			{/if}
		</button>
	</div>
	<p class="mx-auto mt-1.5 max-w-3xl text-[10px] text-slate-400">
		{sending
			? 'Enter to replace the active request · Cancel stops it'
			: 'Enter to send · Shift+Enter for new line'}
	</p>
</div>
