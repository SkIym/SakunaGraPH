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

<div class="calendar-drill">
	<section class="calendar-step">
		<header>
			<h3><span>01</span> Years</h3>
			<p>Select a year to drill down.</p>
		</header>
		<YearHeatGrid items={years} {selectedYear} onselect={onSelectYear} />
	</section>
	<section class="calendar-step">
		<header>
			<h3>
				<span>02</span> Months {selectedYear ? `in ${selectedYear}` : ''}
			</h3>
		</header>
		<MonthHeatGrid items={months} {selectedMonth} onselect={onSelectMonth} />
	</section>
	<section class="calendar-step">
		<header>
			<h3><span>03</span> Days</h3>
			<p>{monthLabel}</p>
		</header>
		<DayHeatGrid
			items={days}
			year={selectedYear}
			month={selectedMonth}
			{selectedDay}
			onselect={onSelectDay}
		/>
	</section>
</div>

<style>
	.calendar-drill {
		display: grid;
	}

	.calendar-step {
		border-block-start: 1px solid var(--color-border);
		padding: 1.25rem 0;
	}

	.calendar-step:first-child {
		border-block-start: 0;
		padding-block-start: 0;
	}

	.calendar-step:last-child {
		padding-block-end: 0;
	}

	header {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: 1rem;
		margin-block-end: 0.75rem;
	}

	h3 {
		color: var(--color-text);
		font-size: 0.75rem;
		font-weight: 700;
	}

	h3 span {
		margin-inline-end: 0.5rem;
		color: var(--color-brand);
		font-family: var(--font-mono);
		font-size: 0.62rem;
	}

	p {
		color: var(--color-text-muted);
		font-size: 0.625rem;
	}
</style>
