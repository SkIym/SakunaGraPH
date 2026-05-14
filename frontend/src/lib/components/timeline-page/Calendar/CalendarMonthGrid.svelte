<script lang="ts">
  import { getContext } from 'svelte';
  import { setSelectedMonth, getSelectedMonth, getSelectedYear } from '$lib/timeline/calendarStates.svelte';
	import { setDateToShow, setShowInfo } from '$lib/timeline/dateInfoStates.svelte';
  const { width, height, data, zScale, z } = getContext('LayerCake');

  // Default to 4 columns (3 rows) for a nice 12-month grid
  let { columns = 4, gap = 5 } = $props();

  let year = $derived(getSelectedYear());

  const monthNames = [
    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
  ];

  // We simply use 0-11 to represent Jan-Dec
  const monthRange = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

  let cellSize = $derived(($width - (columns * gap)) / columns);

  // Helper to find data for a specific month
  // Assumes your data objects have a 'month' property (0-indexed)
  let getMonthData = $derived(month => $data.find(d => d.month === month));

  let fillColor = $derived(month => {
    const d = getMonthData(month);
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

{#each monthRange as month, i}
  <g class="month-group">
    <rect
      class="month-square"
      role="button"
      tabindex="0"
      width={cellSize}
      height={cellSize}
      x={rectX(i)}
      y={rectY(i)}
      fill={fillColor(month)}
      stroke={month === getSelectedMonth() ? '#fed217' : '#ccc'}
      stroke-width={month === getSelectedMonth() ? 3 : 1}
      onclick={() => {setSelectedMonth(month);}}
      ondblclick={() => {setDateToShow(`${year}-${String(month + 1).padStart(2, '0')}`); setShowInfo(true);}}
      onkeydown={(e) => e.key === 'Enter' && console.log(monthNames[month])}
    >
      <title>
        Month: {monthNames[month]}
        {getMonthData(month) 
          ? `\nValue: ${$z(getMonthData(month))}`
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
      {monthNames[month]}
    </text>
  </g>
{/each}

<style>
  .month-square {
    transition: fill 0.2s;
    cursor: pointer;
  }
  .month-square:hover {
    opacity: 0.8;
    stroke: #000;
  }
  text {
    pointer-events: none;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }
</style>