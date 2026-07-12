<script>
	import { analysisFilters } from '$lib/analysis/filters.svelte.js';
</script>

<section aria-labelledby="analysis-date-heading" class="border-b border-slate-200 px-4 py-4">
	<h2 id="analysis-date-heading" class="text-xs font-semibold text-slate-700">Date range</h2>
	<div class="mt-3 grid grid-cols-2 gap-2">
		<label class="min-w-0 text-[11px] font-medium text-slate-500" for="analysis-start-date">
			Start date
			<input
				id="analysis-start-date"
				type="date"
				value={analysisFilters.startDate}
				max={analysisFilters.endDate || undefined}
				oninput={(event) => analysisFilters.setStartDate(event.currentTarget.value)}
				class="mt-1.5 block h-9 w-full min-w-0 rounded-md border border-slate-200 bg-white px-2 text-xs text-slate-700 outline-none transition focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100"
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
				class="mt-1.5 block h-9 w-full min-w-0 rounded-md border border-slate-200 bg-white px-2 text-xs text-slate-700 outline-none transition focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100"
			/>
		</label>
	</div>
</section>
