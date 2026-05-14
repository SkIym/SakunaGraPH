<script lang="ts">
  import { getContext } from 'svelte';
  import { setSelectedDay, getSelectedDay, getSelectedYear, getSelectedMonth } from '$lib/timeline/calendarStates.svelte';
	import { setDateToShow, setShowInfo } from '$lib/timeline/dateInfoStates.svelte';
  const { width, height, data, zScale, z } = getContext('LayerCake');

  let {
    columns = 7, 
    gap = 5 
  } = $props();

  let year = $derived(getSelectedYear());
  let month = $derived(getSelectedMonth());

  let daysInMonth = $derived(new Date(year, month + 1, 0).getDate());
  
  let daysRange = $derived(Array.from({ length: daysInMonth }, (_, i) => i + 1));

  let cellSize = $derived(($width - (columns * gap)) / columns);

  let getDayData = $derived(day => $data.find(d => d.day === day));

  let fillColor = $derived(day => {
    const d = getDayData(day);
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

{#each daysRange as day, i}
  <g class="day-group">
    <rect
      class="day-square"
      role="button"
      tabindex="0"
      width={cellSize}
      height={cellSize}
      x={rectX(i)}
      y={rectY(i)}
      fill={fillColor(day)}
      stroke={day === getSelectedDay() ? '#fed217' : '#ccc'}
      stroke-width={day === getSelectedDay() ? 3 : 1}
      onclick={() => {setSelectedDay(day);}}
      ondblclick={() => {setDateToShow(`${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`); setShowInfo(true);}}
      onkeydown={(e) => e.key === 'Enter' && console.log(`Day ${day}`)}
    >
      <title>
        Day: {day}
        {getDayData(day) 
          ? `\nValue: ${$z(getDayData(day))}`
          : '\nDisasters: 0'}
        {"\nDouble-click to show disasters"}
      </title>
    </rect>
    
    <text
      x={rectX(i) + 4}
      y={rectY(i) + 12}
      font-size="10px"
      fill="#000"
    >
      {day}
    </text>
  </g>
{/each}

<style>
  .day-square {
    transition: fill 0.2s;
    cursor: pointer;
  }
  .day-square:hover {
    stroke: #333;
    stroke-width: 2px;
  }
  text {
    pointer-events: none;
    user-select: none;
  }
</style>