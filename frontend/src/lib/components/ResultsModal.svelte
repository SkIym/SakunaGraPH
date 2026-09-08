<script>
	import { focusTrap } from '../actions/focus.js';

	let { results, onclose, returnFocus = null } = $props();

	const PAGE_SIZE = 10;
	let page = $state(0);

	const vars = $derived(results?.head?.vars ?? []);
	const bindings = $derived(results?.results?.bindings ?? []);
	const totalPages = $derived(Math.ceil(bindings.length / PAGE_SIZE) || 1);
	const pageRows = $derived(bindings.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE));

	const paginationPages = $derived(() => {
		if (totalPages <= 7) return Array.from({ length: totalPages }, (_, i) => i);
		const visible = new Set([0, totalPages - 1, page]);
		for (let i = Math.max(1, page - 1); i <= Math.min(totalPages - 2, page + 1); i++)
			visible.add(i);
		const sorted = [...visible].sort((a, b) => a - b);
		const out = [];
		let prev = -1;
		for (const p of sorted) {
			if (p - prev > 1) out.push(null);
			out.push(p);
			prev = p;
		}
		return out;
	});

	function cellDisplay(binding, varName) {
		const cell = binding[varName];
		if (!cell) return { text: '—', isUri: false, full: '' };
		if (cell.type === 'uri') {
			const local = cell.value.split(/[#/]/).filter(Boolean).pop() ?? cell.value;
			return { text: local, isUri: true, full: cell.value };
		}
		return { text: cell.value ?? '—', isUri: false, full: '' };
	}

	function onBackdrop(e) {
		if (e.target === e.currentTarget) onclose?.();
	}
</script>

<!-- Backdrop -->
<div
	use:focusTrap={{ returnFocus }}
	role="dialog"
	aria-modal="true"
	aria-labelledby="query-results-title"
	class="fixed inset-0 z-50 flex items-center justify-center p-3 sm:p-6"
	style="background:rgba(15,23,42,0.4); backdrop-filter:blur(4px);"
	onclick={onBackdrop}
	onkeydown={(e) => e.key === 'Escape' && onclose?.()}
	tabindex="-1"
>
	<!-- Modal panel -->
	<div
		role="document"
		class="flex w-full max-w-3xl flex-col overflow-hidden rounded-2xl bg-white shadow-2xl"
		style="max-height:min(80vh, calc(100dvh - 1.5rem));"
	>
		<!-- Header -->
		<div class="flex shrink-0 items-center justify-between border-b border-slate-100 px-5 py-4">
			<div>
				<h2 id="query-results-title" class="text-sm font-semibold text-slate-800">Query Results</h2>
				<p class="mt-0.5 text-xs text-slate-400">
					{bindings.length}
					{bindings.length === 1 ? 'result' : 'results'}
					{#if totalPages > 1}· page {page + 1} of {totalPages}{/if}
				</p>
			</div>
			<button
				onclick={onclose}
				data-focus-first
				class="flex h-11 w-11 shrink-0 items-center justify-center rounded-full text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-700"
				aria-label="Close results"
			>
				<svg
					xmlns="http://www.w3.org/2000/svg"
					width="16"
					height="16"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					stroke-width="2"
					stroke-linecap="round"
					stroke-linejoin="round"
				>
					<path d="M18 6 6 18" /><path d="m6 6 12 12" />
				</svg>
			</button>
		</div>

		<!-- Table -->
		<div class="flex-1 overflow-auto">
			{#if bindings.length === 0}
				<div class="flex h-40 flex-col items-center justify-center gap-3 text-slate-400">
					<svg
						xmlns="http://www.w3.org/2000/svg"
						width="32"
						height="32"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="1.5"
						stroke-linecap="round"
						stroke-linejoin="round"
						class="opacity-40"
					>
						<circle cx="11" cy="11" r="8" /><path d="m21 21-4.3-4.3" />
					</svg>
					<div class="max-w-md px-5 text-center">
						<p class="text-sm font-semibold text-slate-700">The query returned no rows</p>
						<p class="mt-1 text-xs leading-5 text-slate-600">
							Broaden its filters or choose another sample query, then run it again.
						</p>
					</div>
				</div>
			{:else}
				<table class="w-full text-sm">
					<thead class="sticky top-0 z-10 border-b border-slate-200 bg-slate-50">
						<tr>
							<th
								class="w-9 px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wider text-slate-400"
								>#</th
							>
							{#each vars as v}
								<th
									class="max-w-[14rem] break-all px-3 py-2.5 text-left text-xs font-semibold uppercase tracking-wider text-slate-500"
									>?{v}</th
								>
							{/each}
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100">
						{#each pageRows as row, i}
							<tr class="transition-colors hover:bg-slate-50/80">
								<td class="px-3 py-2 font-mono text-xs text-slate-300"
									>{page * PAGE_SIZE + i + 1}</td
								>
								{#each vars as v}
									{@const cell = cellDisplay(row, v)}
									<td class="max-w-[240px] px-3 py-2">
										{#if cell.isUri}
											<span class="brand-link block truncate font-mono text-xs" title={cell.full}
												>{cell.text}</span
											>
										{:else}
											<span
												dir="auto"
												class="block break-words text-xs text-slate-700 [overflow-wrap:anywhere]"
												>{cell.text}</span
											>
										{/if}
									</td>
								{/each}
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>

		<!-- Pagination — centered -->
		{#if totalPages > 1}
			<div
				class="flex shrink-0 flex-col items-center gap-2 border-t border-slate-100 bg-slate-50 px-5 py-3"
			>
				<p class="text-xs text-slate-400">
					{page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, bindings.length)} of {bindings.length}
				</p>
				<div class="flex flex-wrap items-center justify-center gap-1">
					<button
						onclick={() => (page = Math.max(0, page - 1))}
						disabled={page === 0}
						class="min-h-11 rounded-lg border border-slate-200 px-3 py-2 text-xs font-medium text-slate-600 transition-colors hover:bg-white disabled:cursor-not-allowed disabled:opacity-40"
					>
						Prev
					</button>

					{#each paginationPages() as p}
						{#if p === null}
							<span class="flex h-11 w-5 items-center justify-center text-xs text-slate-500">…</span
							>
						{:else}
							<button
								onclick={() => (page = p)}
								class="h-11 w-11 rounded-lg text-xs font-medium transition-colors {page === p
									? 'bg-[#305bb2] text-white shadow-sm'
									: 'border border-slate-200 text-slate-600 hover:bg-white'}"
							>
								{p + 1}
							</button>
						{/if}
					{/each}

					<button
						onclick={() => (page = Math.min(totalPages - 1, page + 1))}
						disabled={page >= totalPages - 1}
						class="min-h-11 rounded-lg border border-slate-200 px-3 py-2 text-xs font-medium text-slate-600 transition-colors hover:bg-white disabled:cursor-not-allowed disabled:opacity-40"
					>
						Next
					</button>
				</div>
			</div>
		{/if}
	</div>
</div>
