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
		class="export-button"
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

<style>
	.export-button {
		display: flex;
		height: 2.75rem;
		align-items: center;
		gap: 0.5rem;
		border: 0;
		border-radius: var(--radius-control);
		background: var(--color-text);
		padding: 0 1rem;
		font-size: 0.75rem;
		font-weight: 700;
		color: var(--color-canvas);
		box-shadow: var(--shadow-control);
		transition:
			background-color 180ms ease,
			transform 120ms ease;
	}

	.export-button:hover:not(:disabled) {
		background: #0f1a2b;
	}

	.export-button:active:not(:disabled) {
		transform: translateY(1px);
	}

	.export-button:disabled {
		cursor: not-allowed;
		background: var(--color-border);
		color: var(--color-text-secondary);
	}
</style>
