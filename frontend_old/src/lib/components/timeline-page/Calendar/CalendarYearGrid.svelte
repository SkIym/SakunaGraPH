<script lang="ts">
	import { setSelectedYear, getSelectedYear } from '$lib/timeline/calendarStates.svelte';
  import { getContext } from 'svelte';
  import { setDateToShow, setShowInfo } from '$lib/timeline/dateInfoStates.svelte';
  import fullData from '$lib/demo_data/disaster_data.csv';
  const { width, height, data, zScale, z } = getContext('LayerCake');

  let { columns = 5, gap = 5 } = $props();

  // Create a continuous range of years from the data
  let yearRange = $derived.by(() => {
    const years = fullData.map(d => new Date(d.date).getFullYear());
    const min = Math.min(...years);
    const max = Math.max(...years);
    const list = [];
    for (let i = min; i <= max; i++) list.push(i);
    return list;
  });
  
  let cellSize = $derived(($width - (columns * gap)) / columns);

  // Helper to find data for a specific year
  let getYearData = $derived(year => $data.find(d => d.year === year));

  let fillColor = $derived(year => {
    const d = getYearData(year);
    // Use the z-accessor (total_damage) to get the color [cite: 52]
    return d ? $zScale($z(d)) : '#f0f0f0';
  });

  let rectX = $derived(index => (index % columns) * (cellSize + gap));
  let rectY = $derived(index => Math.floor(index / columns) * (cellSize + gap));

  // Prevents text selection on double-click
  document.addEventListener('mousedown', function(event) {
    if (event.detail > 1) {
      event.preventDefault();
    }
  }, false);
</script>

{#each yearRange as year, i}
  <g class="year-group">
    <rect
      class="year-square"
      role="button"
      tabindex="0"
      width={cellSize}
      height={cellSize}
      x={rectX(i)}
      y={rectY(i)}
      fill={fillColor(year)}
      stroke={year === getSelectedYear() ? '#fed217' : '#ccc'}
      stroke-width={year === getSelectedYear() ? 3 : 1}
      onclick={() => setSelectedYear(year)}
      ondblclick={() => {setDateToShow(year); setShowInfo(true);}}
      onkeydown={() => {return null}}
    >
      <title>
        Year: {year}
        {getYearData(year) 
          ? `\nDisasters: ${getYearData(year).count}`
          : '\nDisasters: 0'}
        {"\nDouble-click to show disasters"}
      </title>
    </rect>

    <text
      x={rectX(i) + 5}
      y={rectY(i) + 15}
      font-size="12px"
      font-weight="bold"
      fill="#000"
    >
      {year}
    </text>
  </g>
{/each}

<style>
  .year-square {
    transition: fill 0.2s;
    cursor: pointer;
  }
  .year-square:hover {
    opacity: 0.8;
    stroke: #000;
  }
  text {
    pointer-events: none;
  }
</style>