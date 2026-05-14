<!-- 
TODO
[] - show x axis values even when y is 0 to show gaps in data
[] - hover on bars for details
[] - click on bars for more info (like in Map)
[/] - show months per year
-->

<script>
// @ts-nocheck

	import CSyears from '$lib/components/timeline-page/ColumnStack/CSyears.svelte';
	import CSmonths from '$lib/components/timeline-page/ColumnStack/CSmonths.svelte';
	import Legend from '$lib/components/legend.svelte'
	import logo from '$lib/images/philippines_logo.svg'

	import {seriesColors, seriesCount, seriesNames} from '$lib/components/chart-shapes/chart.svelte.js';
	import CalendarYear from '$lib/components/timeline-page/Calendar/CalendarYear.svelte';
	import CalendarMonth from '$lib/components/timeline-page/Calendar/CalendarMonth.svelte';
	import CalendarDay from '$lib/components/timeline-page/Calendar/CalendarDay.svelte';
	import DateInfo from '$lib/timeline/DateInfo.svelte';
</script>

<svelte:head>
	<title>Disaster</title>
	<meta name="description" content="Map view" />
	<link rel="icon" href={logo} />
</svelte:head>

<div class="flex justify-center">
	<div class="flex flex-col my-6 max-w-300">
		<h2 class = "mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 font-bold text-xl md:text-2xl lg:text-3xl text-gray-900 dark:text-gray-100">
			TIMELINE OF EVENTS
		</h2>
		<p class="text-justify dark:text-gray-300 text-gray-700">
			The timeline view presents a chronological sequence of disaster events in the Philippines.
			Users can explore past disasters, their impacts, and the responses undertaken over time.
			This feature allows for a better understanding of disaster trends and historical context.
		</p>
	</div>
</div>
	

<section class = "block mx-auto max-w-300 w-full">

	<div class="py-4 h-1/3 flex flex-col md:flex-row justify-between gap-5">
		<CalendarYear />
		<CalendarMonth />
		<CalendarDay />
	</div>

	<Legend />

	<div class="py-4 h-1/3">
		<CSyears />
	</div>
	<div class="py-4 h-1/3">
		<CSmonths />
	</div>
</section>

<DateInfo></DateInfo>

