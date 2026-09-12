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
		onToggleColumn = () => {},
	} = $props();

	const firstRecord = $derived(total === 0 ? 0 : (page - 1) * pageSize + 1);
	const lastRecord = $derived(Math.min(page * pageSize, total));
</script>

<div class="event-table-toolbar">
	<div class="min-w-0">
		<p class="record-range">
			{#if loading && total === 0}
				Loading records…
			{:else if total === 0}
				0 records
			{:else}
				{firstRecord.toLocaleString()}–{lastRecord.toLocaleString()} of {total.toLocaleString()} records
			{/if}
		</p>
		<p class="mt-0.5 text-[10px] text-slate-400">
			Exports include every record in the current filter scope.
		</p>
	</div>

	<div class="flex flex-wrap items-center gap-2">
		<label class="toolbar-select">
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
			<summary class="toolbar-button [&::-webkit-details-marker]:hidden">
				<svg
					viewBox="0 0 24 24"
					class="h-3.5 w-3.5"
					fill="none"
					stroke="currentColor"
					stroke-width="1.8"
					aria-hidden="true"
				>
					<path d="M4 5h16M4 12h16M4 19h16"></path>
				</svg>
				Columns
			</summary>
			<div class="column-menu">
				<p
					class="px-2 pb-1.5 text-[9px] font-semibold uppercase text-slate-400"
					style="letter-spacing:0.1em;"
				>
					Visible columns
				</p>
				{#each columns as column (column.id)}
					<label
						class="flex min-h-11 cursor-pointer items-center gap-2 rounded-lg px-2 text-xs text-slate-600 hover:bg-slate-50"
					>
						<input
							type="checkbox"
							checked={visibleColumns.has(column.id)}
							disabled={visibleColumns.size === 1 && visibleColumns.has(column.id)}
							onchange={() => onToggleColumn(column.id)}
							class="h-4 w-4 accent-[var(--color-brand)]"
						/>
						{column.label}
					</label>
				{/each}
			</div>
		</details>

		<ExportButton params={exportParams} disabled={loading || total === 0} />
	</div>
</div>

<style>
	.event-table-toolbar {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		border-bottom: 1px solid var(--color-border);
		padding: 0.9rem 1.25rem;
		background: var(--color-surface-subtle);
	}

	.record-range {
		margin: 0;
		font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
		font-size: 0.6875rem;
		font-weight: 600;
		font-variant-numeric: tabular-nums;
		color: var(--color-text);
	}

	.toolbar-select,
	.toolbar-button {
		display: flex;
		height: 2.75rem;
		align-items: center;
		gap: 0.5rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 0.75rem;
		font-size: 0.75rem;
		font-weight: 600;
		color: var(--color-text-secondary);
		transition:
			border-color 160ms ease,
			background-color 160ms ease;
	}

	.toolbar-button {
		cursor: pointer;
		list-style: none;
	}

	.toolbar-button:hover {
		border-color: var(--color-brand-medium);
		background: var(--color-brand-soft);
	}

	.column-menu {
		position: absolute;
		right: 0;
		z-index: 20;
		width: 13rem;
		margin-top: 0.4rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0.5rem;
		box-shadow: var(--shadow-surface);
	}

	@media (max-width: 47.999rem) {
		.event-table-toolbar {
			align-items: flex-start;
			flex-direction: column;
		}
	}
</style>
