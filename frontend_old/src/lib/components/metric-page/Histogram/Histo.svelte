<script>
  //@ts-nocheck
  import { LayerCake, Svg, bin, takeEvery } from 'layercake';

  import { extent } from 'd3-array';
  import { scaleBand, scaleLog } from 'd3-scale';
  import { format } from 'd3-format';

  import { Card } from 'flowbite-svelte';

  import Column from '$lib/components/chart-shapes/Column.svelte';
  import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
  import AxisY from '$lib/components/chart-shapes/AxisY.svelte';

  import calcThresholds from '$lib/components/metric-page/Histogram/calcThreshold.js';

  // import data from '$lib/demo_data/_data/unemployment.js';
  // console.log(data);
  // data
  import disaster from '$lib/demo_data/disaster_data.csv'

  const f = format('.2s');

  const xKey = ['x0', 'x1'];
  const yKey = 'length';

  let binCount = $state(10);
  
  
  const disasterClean = $derived(disaster.filter(d => d.monetary_damage > 0));
  const logDomain = $derived(extent(disasterClean, d => Math.log10(d.monetary_damage)));
  let thresholds = $derived.by(() => {
      const [min, max] = logDomain;
      const step = (max - min) / binCount;
      return Array.from({ length: binCount + 1 }, (_, i) => Math.pow(10, min + i * step));
  });

  const domain = extent(disaster, d => d.monetary_damage);

  // $effect(() => {
  //   console.log(thresholds);
  // });

  // let thresholds = $derived(calcThresholds(domain, binCount));
  let slimThresholds = $derived(takeEvery(thresholds, 6));

  let binnedData = $derived(
    bin(disaster, d => d.monetary_damage, { 
    domain, 
    thresholds 
  })
  );
</script>

<div id="histogram" class="">
  <Card class="p-4 md:p-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
    <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">Economic Damage Distribution</h5>
    <div class="relative w-full max-w-full h-100">
      <div class="absolute right-2 top-0 z-10 flex items-center gap-2">
        <input
          type="range"
          min="5"
          max="30"
          step="5"
          bind:value={binCount}
          class="h-2 w-24 cursor-pointer appearance-none rounded-full bg-gray-200"
        />
        <span class="w-16 text-right dark:text-gray-300">{binCount} bins</span>
      </div>

      <div class="z-0 h-90 max-w-full my-4 ">
        <LayerCake
          padding={{ top: 20, right: 5, bottom: 20, left: 30 }}
          x={xKey}
          y={yKey}
          xDomain={thresholds}
          xScale={scaleBand().paddingInner(0)}
          yDomain={[0, null]}
          data={binnedData}
        >
          <Svg>
            <AxisX gridlines={false} baseline ticks={slimThresholds} format={d => f(d)} />
            <AxisY gridlines={false} ticks={3} />
            <Column fill="#2A6" stroke="#000" strokeWidth={1} />
          </Svg>
        </LayerCake>
      </div>
    </div>
  </Card>
  
</div>


<style>
  /* for slider */
  input[type='range']::-webkit-slider-thumb {
    appearance: none;
    width: 16px;
    height: 16px;
    background: "--color-theme-2";
    border-radius: 50%;
    border: 2px solid #ffffff;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
  }

  input[type='range']::-moz-range-thumb {
    width: 16px;
    height: 16px;
    background: #2563eb;
    border-radius: 50%;
    border: 2px solid #ffffff;
    box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
  }
</style>