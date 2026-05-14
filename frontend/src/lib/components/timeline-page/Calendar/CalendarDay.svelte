<script>
  import { LayerCake, Svg } from 'layercake';
  import { scaleSequential } from 'd3-scale';
  import { interpolateOranges } from 'd3-scale-chromatic';
  import CalendarDayGrid from './CalendarDayGrid.svelte';
  //import disasters from '$lib/demo_data/disaster_data.csv';
  import { getSelectedYear, getSelectedMonth } from '$lib/timeline/calendarStates.svelte';
  import { getFilteredCsvData } from '$lib/demo_data/filteredData.svelte';

  const monthNames = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
  ];

  let disasters = $derived(getFilteredCsvData());

  let year = $derived(getSelectedYear());
  let month = $derived(getSelectedMonth());

  let transformed = $derived(disasters.map(d => ({
    ...d,
    date: new Date(d.date),
    year: new Date(d.date).getUTCFullYear(),
    month: new Date(d.date).getUTCMonth(),
    day: new Date(d.date).getDate()
  })));

  let dataByDay = $derived(Array.from(
    transformed.filter(d => d.year === year && d.month === month).reduce((acc, cur) => {
      const day = cur.day;
      if (!acc.has(day)) {
        acc.set(day, { day, total_damage: 0, count: 0 });
      }
      const entry = acc.get(day);
      entry.total_damage += +cur.monetary_damage;
      entry.count += 1;
      return acc;
    }, new Map()).values()
  ).sort((a, b) => a.day - b.day));

  // Define a high-contrast color range
  const seriesColors = ['#fff5cc', '#ffeba9', '#ffe182', '#ffd754', '#ffcc00', '#ff9900'];
</script>

<div class="chart-container">
    <h2 class="mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 text-[1.4rem] text-gray-900 dark:text-gray-300">
        Disasters Per Day of {monthNames[month]}, {year}
    </h2>

    <LayerCake
        x="year"
        z="count"
        zScale={scaleSequential(interpolateOranges)}
        data={dataByDay}
    >
        <Svg>
        <CalendarDayGrid />
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