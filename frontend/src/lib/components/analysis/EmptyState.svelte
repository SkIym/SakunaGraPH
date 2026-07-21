<script>
	let {
		title = 'No records found',
		description = 'Try changing or clearing the active filters.',
		actionLabel = '',
		onaction = () => {},
		tone = 'neutral',
	} = $props();
</script>

<div
	class="flex min-h-72 flex-col items-center justify-center rounded-lg border border-dashed px-6 py-12 text-center
		{tone === 'error' ? 'border-red-200 bg-red-50/50' : 'border-slate-200 bg-slate-50/40'}"
	role={tone === 'error' ? 'alert' : 'status'}
>
	<div
		class="flex h-10 w-10 items-center justify-center rounded-full
			{tone === 'error' ? 'bg-red-100 text-red-600' : 'bg-slate-100 text-slate-400'}"
		aria-hidden="true"
	>
		{#if tone === 'error'}
			<svg viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="1.8">
				<circle cx="12" cy="12" r="9"></circle>
				<path d="M12 7v6"></path>
				<path d="M12 17h.01"></path>
			</svg>
		{:else}
			<svg viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="1.8">
				<ellipse cx="12" cy="5" rx="8" ry="3"></ellipse>
				<path d="M4 5v7c0 1.7 3.6 3 8 3s8-1.3 8-3V5"></path>
				<path d="M4 12v7c0 1.7 3.6 3 8 3s8-1.3 8-3v-7"></path>
			</svg>
		{/if}
	</div>
	<h2 class="mt-4 text-sm font-semibold {tone === 'error' ? 'text-red-800' : 'text-slate-700'}">
		{title}
	</h2>
	<p
		class="mt-1.5 max-w-md text-xs leading-5 {tone === 'error' ? 'text-red-600' : 'text-slate-500'}"
	>
		{description}
	</p>
	{#if actionLabel}
		<button
			type="button"
			onclick={onaction}
			class="mt-4 h-9 rounded-md border px-3 text-xs font-semibold transition
				{tone === 'error'
				? 'border-red-200 bg-white text-red-700 hover:bg-red-50'
				: 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50'}"
		>
			{actionLabel}
		</button>
	{/if}
</div>
