<script>
	import { onDestroy } from 'svelte';
	import { exportAnalysisEvents } from '$lib/api/analysis.js';

	let { params = '', disabled = false } = $props();

	let exporting = $state(false);
	let error = $state('');
	let activeRequest = null;

	onDestroy(() => activeRequest?.abort());

	async function exportCsv() {
		if (disabled || exporting) return;
		exporting = true;
		error = '';
		activeRequest = new AbortController();

		try {
			const { blob, filename } = await exportAnalysisEvents(params, {
				signal: activeRequest.signal,
			});
			const objectUrl = URL.createObjectURL(blob);
			const link = document.createElement('a');
			link.href = objectUrl;
			link.download = filename;
			document.body.appendChild(link);
			link.click();
			link.remove();
			window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
		} catch (requestError) {
			if (requestError.name === 'AbortError') return;
			error = requestError.message || 'Export failed.';
		} finally {
			exporting = false;
			activeRequest = null;
		}
	}
</script>

<div class="flex items-center gap-2">
	{#if error}
		<span
			class="hidden max-w-44 truncate text-[10px] text-red-600 sm:inline"
			role="alert"
			title={error}>{error}</span
		>
	{/if}
	<button
		type="button"
		onclick={exportCsv}
		disabled={disabled || exporting}
		aria-busy={exporting}
		title={error || 'Export all filtered records as CSV'}
		class="flex h-9 items-center gap-2 rounded-md bg-slate-800 px-3 text-xs font-semibold text-white transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-300"
	>
		{#if exporting}
			<span
				class="h-3.5 w-3.5 animate-spin rounded-full border-2 border-white/40 border-t-white"
				aria-hidden="true"
			></span>
			Exporting…
		{:else}
			<svg
				viewBox="0 0 24 24"
				class="h-3.5 w-3.5"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
				aria-hidden="true"
			>
				<path d="M12 3v12"></path>
				<path d="m7 10 5 5 5-5"></path>
				<path d="M5 21h14"></path>
			</svg>
			Export CSV
		{/if}
	</button>
</div>
