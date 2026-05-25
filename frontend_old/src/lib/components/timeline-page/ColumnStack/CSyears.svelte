<script>
  //@ts-nocheck
  import { LayerCake, Svg, Html, flatten, stack } from 'layercake';
  import { scaleBand, scaleOrdinal } from 'd3-scale';
  import { format } from 'd3-format';

  import ColumnStacked from '$lib/components/timeline-page/ColumnStack/ColumnStacked.svelte';
  import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
  import AxisY from '$lib/components/chart-shapes/AxisY.svelte';
  import Brush from '$lib/components/timeline-page/Brush/Brush.html.svelte';

  import { getFilteredCsvData } from '$lib/demo_data/filteredData.svelte';

  // Import and process your disaster data
  //import disaster from '$lib/demo_data/disaster_data.csv';
  //let disaster = $derived(getFilteredCsvData());
  import { disasterColors, subType, generalizeType, getProcessedYears, getFullData, allTypes, seriesColors, seriesNames } from '$lib/components/chart-shapes/chart.svelte.js';

  // State for brush extents
  let brushExtents = $state([0, 0.3]);

  let fullData = $derived(getFullData());
  const xKey = 'year';
  const yKey = [0, 1];
  const zKey = 'key';
  
  const formatLabelY = d => format(`~s`)(d);

  // Derive brushed data based on brush extents
  let brushedData = $derived.by(() => {
    if (!fullData.length) return [];
    
    if (brushExtents[0] === null && brushExtents[1] === null) {
      return fullData;
    }
    
    const minIndex = brushExtents[0] !== null ? 
      Math.floor(brushExtents[0] * fullData.length) : 0;
    const maxIndex = brushExtents[1] !== null ? 
      Math.ceil(brushExtents[1] * fullData.length) : fullData.length;
    
    return fullData.slice(minIndex, maxIndex);
  });

  // Create stacked data for the brushed view
  let stackedData = $derived.by(() => stack(brushedData, seriesNames));
  // console.log("StackedData", stackedData);
  let flatData = $derived.by(() => flatten(stackedData));
  
  // Create data for the brush overview (use total count per year)
  let brushData = $derived.by(() => {
    return fullData.map(d => {
      const total = seriesNames.reduce((sum, type) => sum + (d[type] || 0), 0);
      return {
        year: d.year,
        total: total
      };
    });
  });
</script>

<div id="columnStackBrush" class="">
  <h2 class="mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 text-[1.4rem] text-gray-900 dark:text-gray-300">
    Disasters Per Year, Stacked by Category
  </h2>
  
  <div class="outer-container h-87.5 flex flex-col gap-2">
    <!-- Main chart (brushed area) -->
    <div class="brushed-chart-container max-w-full h-[90%]">
      <LayerCake
        padding={{ top: 0, right: 0, bottom: 60, left: 20 }}
        x={d => d.data[xKey]}
        y={yKey}
        z={zKey}
        xScale={scaleBand().paddingInner(0.02).round(true)}
        xDomainSort={false}
        zScale={scaleOrdinal()}
        zDomain={seriesNames}
        zRange={seriesColors}
        {flatData}
        data={stackedData}
      >
        <Svg>
          <AxisX gridlines={false} rotate={true}  />
          <AxisY ticks={4} gridlines={false} format={formatLabelY} />
          <ColumnStacked stroke="#000" strokeWidth={0.5}/>
        </Svg>
      </LayerCake>
    </div>

    <!-- Brush overview chart -->
    <div class=" brush-container max-w-full h-[10%] border-2 rounded">
      <LayerCake
        padding={{ top: 5, bottom: 0, left: 20, right: 0 }}
        x="year"
        y="total"
        xScale={scaleBand().paddingInner(0.02).round(true)}
        xDomainSort={false}
        data={brushData}
      >
        <Svg>
          <!-- Simple bars for overview -->
          <rect
            x={({ x, d }) => x(d.year)}
            y={({ y, d }) => y(d.total)}
            width={({ x }) => x.bandwidth()}
            height={({ y, d }) => y.range()[0] - y(d.total)}
            fill="#3498DB"
            opacity="0.6"
          />
        </Svg>
        <Html>
          <Brush bind:min={brushExtents[0]} bind:max={brushExtents[1]} />
        </Html>
      </LayerCake>
    </div>
  </div>
</div>