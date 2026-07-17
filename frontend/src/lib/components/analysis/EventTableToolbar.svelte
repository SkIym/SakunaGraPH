<script>
	import ExportButton from '$lib/components/analysis/ExportButton.svelte';

	let {
		total = 0,
		page = 1,
		pageSize = 25,
		loading = false,
		columns = [],
		visibleColumns = new Set(),
		exportParams = '',
		onPageSizeChange = () => {},
		onToggleColumn = () => {}
	} = $props();

	const firstRecord = $derived(total === 0 ? 0 : (page - 1) * pageSize + 1);
	const lastRecord = $derived(Math.min(page * pageSize, total));
</script>

<div class="flex flex-col gap-3 border-b border-slate-200 px-4 py-3 sm:flex-row sm:items-center sm:justify-between sm:px-5">
	<div class="min-w-0">
		<p class="text-xs font-medium tabular-nums text-slate-700">
			{#if loading && total === 0}
				Loading records…
			{:else if total === 0}
				0 records
			{:else}
				{firstRecord.toLocaleString()}–{lastRecord.toLocaleString()} of {total.toLocaleString()} records
			{/if}
		</p>
		<p class="mt-0.5 text-[10px] text-slate-400">Exports include every record in the current filter scope.</p>
	</div>

	<div class="flex flex-wrap items-center gap-2">
		<label class="flex h-9 items-center gap-2 rounded-md border border-slate-200 bg-white px-2.5 text-[11px] text-slate-500">
			Rows
			<select
				value={pageSize}
				onchange={(event) => onPageSizeChange(Number(event.currentTarget.value))}
				disabled={loading}
				aria-label="Rows per page"
				class="bg-transparent text-xs font-semibold text-slate-700 outline-none disabled:text-slate-400"
			>
				{#each [10, 25, 50, 100] as size}
					<option value={size}>{size}</option>
				{/each}
			</select>
		</label>

		<details class="group relative">
			<summary class="flex h-9 cursor-pointer list-none items-center gap-2 rounded-md border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-600 transition hover:bg-slate-50 [&::-webkit-details-marker]:hidden">
				<svg viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true">
					<path d="M4 5h16M4 12h16M4 19h16"></path>
				</svg>
				Columns
			</summary>
			<div class="absolute right-0 z-20 mt-1.5 w-52 rounded-md border border-slate-200 bg-white p-2 shadow-lg">
				<p class="px-2 pb-1.5 text-[9px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Visible columns</p>
				{#each columns as column (column.id)}
					<label class="flex min-h-8 cursor-pointer items-center gap-2 rounded px-2 text-xs text-slate-600 hover:bg-slate-50">
						<input
							type="checkbox"
							checked={visibleColumns.has(column.id)}
							disabled={visibleColumns.size === 1 && visibleColumns.has(column.id)}
							onchange={() => onToggleColumn(column.id)}
							class="h-3.5 w-3.5 accent-indigo-600"
						/>
						{column.label}
					</label>
				{/each}
			</div>
		</details>

		<ExportButton params={exportParams} disabled={loading || total === 0} />
	</div>
</div>
