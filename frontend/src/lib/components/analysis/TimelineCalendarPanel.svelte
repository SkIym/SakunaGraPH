<script>
	import YearHeatGrid from './YearHeatGrid.svelte';
	import MonthHeatGrid from './MonthHeatGrid.svelte';
	import DayHeatGrid from './DayHeatGrid.svelte';

	let {
		years = [],
		months = [],
		days = [],
		selectedYear = null,
		selectedMonth = null,
		selectedDay = null,
		onSelectYear = () => {},
		onSelectMonth = () => {},
		onSelectDay = () => {},
	} = $props();
	const monthLabel = $derived(
		selectedYear && selectedMonth
			? new Intl.DateTimeFormat('en-PH', { month: 'long', year: 'numeric' }).format(
					new Date(selectedYear, selectedMonth - 1, 1),
				)
			: 'Choose a month',
	);
</script>

<div class="space-y-6">
	<div>
		<div class="mb-3 flex items-baseline justify-between gap-3">
			<h3 class="text-xs font-semibold text-slate-700">Years</h3>
			<p class="text-[10px] text-slate-400">Select a year to drill down.</p>
		</div>
		<YearHeatGrid items={years} {selectedYear} onselect={onSelectYear} />
	</div>
	<div class="border-t border-slate-100 pt-5">
		<div class="mb-3 flex items-baseline justify-between gap-3">
			<h3 class="text-xs font-semibold text-slate-700">
				Months {selectedYear ? `in ${selectedYear}` : ''}
			</h3>
		</div>
		<MonthHeatGrid items={months} {selectedMonth} onselect={onSelectMonth} />
	</div>
	<div class="border-t border-slate-100 pt-5">
		<div class="mb-3 flex items-baseline justify-between gap-3">
			<h3 class="text-xs font-semibold text-slate-700">Days</h3>
			<p class="text-[10px] text-slate-400">{monthLabel}</p>
		</div>
		<DayHeatGrid
			items={days}
			year={selectedYear}
			month={selectedMonth}
			{selectedDay}
			onselect={onSelectDay}
		/>
	</div>
</div>
