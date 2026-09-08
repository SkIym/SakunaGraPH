<script>
	let {
		input = $bindable(''),
		sending = false,
		error = '',
		maxLength = 1_000,
		onSend = () => {},
		onCancel = () => {},
	} = $props();

	const showCount = $derived(input.length >= Math.floor(maxLength * 0.8));

	function handleKeydown(event) {
		if (event.key === 'Enter' && !event.shiftKey) {
			event.preventDefault();
			onSend();
		}
	}
</script>

<div
	class="flex-shrink-0 border-t border-slate-200/80 px-4 pt-3"
	style="background:var(--color-surface); padding-bottom:max(0.75rem, env(safe-area-inset-bottom));"
>
	<div class="mx-auto flex w-full max-w-3xl items-end gap-3">
		<textarea
			bind:value={input}
			onkeydown={handleKeydown}
			aria-label="Question"
			aria-describedby="ask-input-help ask-input-limit"
			aria-invalid={error ? 'true' : undefined}
			maxlength={maxLength}
			rows="1"
			placeholder="Ask a question about Philippine disaster data…"
			class="min-h-11 flex-1 resize-none rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-base leading-relaxed text-slate-800 shadow-sm outline-none transition-colors placeholder:text-slate-500 focus:border-blue-700 focus:ring-2 focus:ring-blue-100 sm:text-sm"
			style="max-height:140px; overflow-y:auto; field-sizing:content;"></textarea>
		<button
			type="button"
			onclick={() => (sending ? onCancel() : onSend())}
			disabled={!sending && !input.trim()}
			class="touch-target flex-shrink-0 rounded-xl bg-slate-800 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
		>
			{#if sending}
				Stop answer
			{:else}
				Send
			{/if}
		</button>
	</div>
	<div class="mx-auto mt-1.5 flex max-w-3xl items-start justify-between gap-3 text-xs">
		<div class="min-w-0">
			<p id="ask-input-help" class="text-slate-600">
				{sending
					? 'Enter to replace the current question · Stop answer ends the current response'
					: 'Enter to send · Shift+Enter for new line'}
			</p>
			{#if error}<p class="mt-1 break-words font-semibold text-red-700" role="alert">
					{error}
				</p>{/if}
		</div>
		<p id="ask-input-limit" class="shrink-0 text-slate-500" class:invisible={!showCount}>
			{input.length.toLocaleString()} / {maxLength.toLocaleString()}
		</p>
	</div>
</div>
