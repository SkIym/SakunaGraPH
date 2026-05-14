<script>
  import { LayerCake, Svg } from 'layercake';
  import { scaleSequential } from 'd3-scale';
  import { interpolateOranges } from 'd3-scale-chromatic';
  import CalendarMonthGrid from './CalendarMonthGrid.svelte';
  //import disasters from '$lib/demo_data/disaster_data.csv';
	import { getSelectedYear } from '$lib/timeline/calendarStates.svelte';
  import { getFilteredCsvData } from '$lib/demo_data/filteredData.svelte';

  let disasters = $derived(getFilteredCsvData());

  let year = $derived(getSelectedYear());

  let transformed = $derived(disasters.map(d => ({
    ...d,
    date: new Date(d.date),
    year: new Date(d.date).getFullYear(),
    month: new Date(d.date).getMonth()
  })));

  let dataByMonth = $derived(Array.from(
    transformed.filter(d => d.year === year).reduce((acc, cur) => {
      const month = cur.month;
      if (!acc.has(month)) {
        acc.set(month, { month, total_damage: 0, count: 0 });
      }
      const entry = acc.get(month);
      entry.total_damage += +cur.monetary_damage;
      entry.count += 1;
      return acc;
    }, new Map()).values()
  ).sort((a, b) => a.month - b.month));

  // Define a high-contrast color range
  const seriesColors = ['#fff5cc', '#ffeba9', '#ffe182', '#ffd754', '#ffcc00', '#ff9900'];
</script>

<div class="chart-container">
    <h2 class="mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 text-[1.4rem] text-gray-900 dark:text-gray-300">
        Disasters Per Month of {year}
    </h2>

    <LayerCake
        x="year"
        z="count"
        zScale={scaleSequential(interpolateOranges)}
        data={dataByMonth}
    >
        <Svg>
        <CalendarMonthGrid />
        </Svg>
    </LayerCake>
</div>

<style>
  .chart-container {
    aspect-ratio: 1 / 1;
    width: 100%;
    max-width: 33%;
    position: relative;
    font-family: sans-serif;
  }

  @media (max-width: 768px) {
    .chart-container {
      max-width: 100%;
      margin-bottom: 1rem;
    }
  }
</style>