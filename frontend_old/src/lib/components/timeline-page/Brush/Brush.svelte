<script>
//@ts-nocheck
  import { LayerCake, Svg, Html } from 'layercake';

  import Line from '$lib/components/chart-shapes/Line.svelte';
  import Area from '$lib/components/chart-shapes/Area.svelte';
  import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
  import AxisY from '$lib/components/chart-shapes/AxisY.svelte';
  import Brush from '$lib/components/timeline-page/Brush/Brush.html.svelte';

  // This example loads csv data as json and converts numeric columns to numbers using @rollup/plugin-dsv. See vite.config.js for details
  
  import data from '$lib/demo_data/_data/points.csv';

  let brushExtents = $state([null, null]);

  const xKey = 'myX';
  const yKey = 'myY';

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
  // console.log(brushedData);
  
</script>

<div id="Brush" class="">
    <h2 class = "mb-4 border-b-2 border-gray-300 pb-2 text-[1.4rem] text-gray-900">
        Brush
    </h2>
    <div class="outer-container h-87.5 flex flex-col gap-2">
      <div class="brushed-chart-container max-w-full h-[80%]">
        <LayerCake
            padding={{ bottom: 20, left: 25 }}
            x={xKey}
            y={yKey}
            yDomain={[0, null]}
            data={brushedData}
        >

            <Svg>
            <AxisX
                ticks={ticks => {
                const filtered = ticks.filter(t => t % 1 === 0);
                if (filtered.length > 7) {
                    return filtered.filter((t, i) => i % 2 === 0);
                }
                return filtered;
                }}
            />
            <AxisY ticks={4} />
            <Line stroke="#00e047" />
            <Area fill="#00e04710" />
            </Svg>
        </LayerCake>
        </div>

        <div class="brush-container max-w-full h-[20%]">
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
</div>

<style>
  /*
    The wrapper div needs to have an explicit width and height in CSS.
    It can also be a flexbox child or CSS grid element.
    The point being it needs dimensions since the <LayerCake> element will
    expand to fill it.
  */
  /* .brushed-chart-container {
    width: 100%;
    height: 80%;
  }
  .brush-container {
    width: 100%;
    height: 20%;
  } */
</style>