<script>
    //@ts-nocheck
  import { LayerCake, Svg, Html, groupLonger, flatten } from 'layercake';

  import { scaleOrdinal } from 'd3-scale';
  import { timeParse, timeFormat } from 'd3-time-format';
  import { format } from 'd3-format';

  import MultiLine from '$lib/components/chart-shapes/Multiline.svelte';
  import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
  import AxisY from '$lib/components/chart-shapes/AxisY.svelte';
  import Labels from '$lib/components/charts/Multiline/GroupLabels.html.svelte';
  import SharedTooltip from '$lib/components/charts/Multiline/SharedTooltip.html.svelte';

  // This example loads csv data as json and converts numeric columns to numbers using @rollup/plugin-dsv. See vite.config.js for details
  import data from '$lib/demo_data/_data/fruit.csv';

  /* --------------------------------------------
   * Set what is our x key to separate it from the other series
   */
  const xKey = 'month';
  const yKey = 'value';
  const zKey = 'fruit';

  const xKeyCast = timeParse('%Y-%m-%d');

  const seriesNames = Object.keys(data[0]).filter(d => d !== xKey);
  const seriesColors = ['#ffe4b8', '#ffb3c0', '#ff7ac7', '#ff00cc'];

  /* --------------------------------------------
   * Cast values
   */
  data.forEach(d => {
    d[xKey] = typeof d[xKey] === 'string' ? xKeyCast(d[xKey]) : d[xKey];
  });

  const formatLabelX = timeFormat('%b. %e');
  const formatLabelY = d => format(`~s`)(d);

  const groupedData = groupLonger(data, seriesNames, {
    groupTo: zKey,
    valueTo: yKey
  });

  const maxValue = Math.max(...groupedData.flatMap(group => 
    group.values.map(d => d[yKey])
  ));

  // START BRUSH
  let brushExtents = $state([null, null]);

  let brushedData = $derived.by(() => {
    let selection = data.slice(
      (brushExtents[0] || 0) * data.length,
      (brushExtents[1] || 1) * data.length
    );
    if (selection.length < 2 && brushExtents[0] !== null) {
      selection = data.slice(brushExtents[0] * data.length, brushExtents[0] * data.length + 2);
    }
    
    return selection;
  });

  // END BRUSH
</script>

<div id="multiline" class="">
    <h2 class="mb-4 border-b-2 border-gray-300 pb-2 text-[1.4rem] text-gray-900">
        Multiline (Disaster Count per Category over Time)
    </h2>
    <div class="chart-container flex w-full max-w-full h-62.5">
      <LayerCake
          padding={{ top: 7, right: 10, bottom: 20, left: 25 }}
          x={xKey}
          y={yKey}
          z={zKey}
          yDomain={[0, maxValue * 1.05]}
          zScale={scaleOrdinal()}
          zRange={seriesColors}
          flatData={flatten(brushedData, 'values')}
          data={brushData}

      >
          <Svg>
          <AxisX
              gridlines={false}
              ticks={data.map(d => d[xKey]).sort((a, b) => a - b)}
              format={formatLabelX}
              snapLabels
              tickMarks
          />
          <AxisY ticks={4} format={formatLabelY} />
          <MultiLine />
          </Svg>

          <Html>
          <Labels />
          <SharedTooltip formatTitle={formatLabelX} dataset={data} />
          </Html>
      </LayerCake>

      <LayerCake padding={{ top: 5 }} x={xKey} y={yKey} yDomain={[0, null]} {data}>
        <Svg>
        <Line stroke="#00e047" />
        <Area fill="#00e04710" />
        </Svg>
        <Html>
        <Brush bind:min={brushExtents[0]} bind:max={brushExtents[1]} />
        </Html>
      </LayerCake>
    </div>
</div>

<style>
  /*
    The wrapper div needs to have an explicit width and height in CSS.
    It can also be a flexbox child or CSS grid element.
    The point being it needs dimensions since the <LayerCake> element will
    expand to fill it.
  */
  /* .chart-container {
    width: 100%;
    height: 250px;
  } */
</style>