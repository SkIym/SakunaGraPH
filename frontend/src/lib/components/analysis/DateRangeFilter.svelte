<script>
	import { analysisFilters } from '$lib/analysis/filters.svelte.js';
</script>

<section aria-labelledby="analysis-date-heading" class="date-filter-section">
	<h2 id="analysis-date-heading" class="text-xs font-semibold text-slate-700">Date range</h2>
	<div class="date-filter-grid">
		<label class="min-w-0 text-[11px] font-medium text-slate-500" for="analysis-start-date">
			Start date
			<input
				id="analysis-start-date"
				type="date"
				value={analysisFilters.startDate}
				max={analysisFilters.endDate || undefined}
				oninput={(event) => analysisFilters.setStartDate(event.currentTarget.value)}
				class="analysis-date-input"
			/>
		</label>
		<label class="min-w-0 text-[11px] font-medium text-slate-500" for="analysis-end-date">
			End date
			<input
				id="analysis-end-date"
				type="date"
				value={analysisFilters.endDate}
				min={analysisFilters.startDate || undefined}
				oninput={(event) => analysisFilters.setEndDate(event.currentTarget.value)}
				class="analysis-date-input"
			/>
		</label>
	</div>
</section>

<style>
	.date-filter-section {
		border-bottom: 1px solid var(--color-border);
		padding: 1rem;
	}

	.date-filter-grid {
		display: grid;
		grid-template-columns: repeat(2, minmax(0, 1fr));
		gap: 0.5rem;
		margin-top: 0.75rem;
	}

	.analysis-date-input {
		display: block;
		width: 100%;
		min-width: 0;
		height: 2.75rem;
		margin-top: 0.4rem;
		border: 1px solid var(--color-border);
		border-radius: var(--radius-control);
		background: var(--color-canvas);
		padding: 0 0.35rem;
		font-size: 0.75rem;
		line-height: normal;
		text-align: center;
		color: var(--color-text);
		outline: none;
	}

	.analysis-date-input:focus {
		border-color: var(--color-brand);
		box-shadow: 0 0 0 2px var(--color-brand-medium);
	}

	.analysis-date-input::-webkit-datetime-edit {
		display: flex;
		height: 100%;
		align-items: center;
		justify-content: center;
		padding: 0;
	}

	.analysis-date-input::-webkit-datetime-edit-fields-wrapper {
		display: flex;
		height: 100%;
		align-items: center;
		padding: 0;
	}

	.analysis-date-input::-webkit-calendar-picker-indicator {
		margin: 0;
		padding: 0.125rem;
	}

	@media (max-width: 20rem) {
		.date-filter-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
