<script>
	// @ts-nocheck
	import { Carousel } from 'flowbite-svelte';
	import logo from '$lib/images/philippines_logo.svg';

	// layercake
	import Histo from '$lib/components/metric-page/Histogram/Histo.svelte';
	import Scatter from '$lib/components/metric-page/Scatter/Scatter.svelte';

	// flowbite charts
	import Donut from '$lib/components/metric-page/Donut/Donut.svelte';
	import Line from '$lib/components/metric-page/Line/Line.svelte';
	import Vital from '$lib/components/metric-page/Vital/VitalSigns.svelte';
	import RankLocation from '$lib/components/metric-page/HorizontalBar/RankingLocation.svelte';
	import RankDisaster from '$lib/components/metric-page/HorizontalBar/RankingDisaster.svelte';

</script>

<svelte:head>
	<title>Disaster</title>
	<meta name="description" content="Map view" />
	<link rel="icon" href={logo} />
</svelte:head>

<div class="flex justify-center">
	<div class="flex flex-col my-6 max-w-300">
		<h2 class = "mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 font-bold text-xl md:text-2xl lg:text-3xl text-gray-900 dark:text-gray-100">
			METRIC DASHBOARD
		</h2>
		<p class="text-justify dark:text-gray-300 text-gray-700">
			The metric dashboard provides a comprehensive overview of disaster statistics in the Philippines.
			It includes various charts and graphs that illustrate trends and patterns in disaster occurrences,
			impacts, and responses over time. Users can analyze data to gain insights into disaster management
			and preparedness efforts.
		</p>
	</div>
</div>
	
<!-- metric charts and graphs -->
<section class = "grid grid-cols-1 gap-4 mx-auto max-w-300 w-full">
	<div class="grid grid-cols-1">
		<Vital />
	</div>
	<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
		<Donut />
		<Line />
	</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
		<RankLocation />
		<RankDisaster />
	</div>
	<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
		<Histo />
		<Scatter />
	</div>
</section>
