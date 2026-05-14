<script>
  import { LayerCake, Svg } from 'layercake';
  import { scaleSequential } from 'd3-scale';
  import { interpolateOranges } from 'd3-scale-chromatic';
  import CalendarYearGrid from './CalendarYearGrid.svelte';
	import { getFilteredCsvData } from '$lib/demo_data/filteredData.svelte';
  //import disasters from '$lib/demo_data/disaster_data.csv';

  let disasters = $derived(getFilteredCsvData());
  
  let transformed = $derived(disasters.map(d => ({
    ...d,
    date: new Date(d.date),
    year: new Date(d.date).getFullYear()
  })));

  let dataByYear = $derived(Array.from(
    transformed.reduce((acc, cur) => {
      const year = cur.year;
      if (!acc.has(year)) {
        acc.set(year, { year, total_damage: 0, count: 0 });
      }
      const entry = acc.get(year);
      entry.total_damage += +cur.monetary_damage;
      entry.count += 1;
      return acc;
    }, new Map()).values()
  ).sort((a, b) => a.year - b.year));

  // Define a high-contrast color range
  const seriesColors = ['#fff5cc', '#ffeba9', '#ffe182', '#ffd754', '#ffcc00', '#ff9900'];
</script>

<div class="chart-container">
    <h2 class="mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 text-[1.4rem] text-gray-900 dark:text-gray-300">
        Disasters Per Year
    </h2>

    <LayerCake
        x="year"
        z="count"
        zScale={scaleSequential(interpolateOranges)}
        data={dataByYear}
    >
        <Svg>
        <CalendarYearGrid />
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