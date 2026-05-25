<script>
  // @ts-nocheck
  import { LayerCake, Html } from 'layercake';
  import CirclePack from '$lib/components/charts/CirclePack/CirclePack.html.svelte';
  import { generalizeType } from '$lib/components/chart-shapes/chart.svelte.js';

  // This example loads csv data as json and converts numeric columns to numbers using @rollup/plugin-dsv. See vite.config.js for details
  // import data from '$lib/demo_data/fruitGroups.csv';
  // console.log(data);

  /** @type {string} */
  const idKey = 'type';
  /** @type {string} */
  const valueKey = 'value';

  // filter disaster count
  import disaster from '$lib/demo_data/disaster_data.csv';
  const disasterCount = disaster.reduce((acc, item) => {
    let type = item.type?.trim();  
    let generalType = generalizeType(type);
    acc[generalType] = (acc[generalType] || 0) + 1;
    return acc;
  }, {});
  // filtered to layercake json
  const data = Object.entries(disasterCount).map(([key, value]) => ({
    type: key,
    value: value.toString()
  }));
</script>

<div id="circlePack" class="">
    <h2 class = "mb-4 border-b-2 border-gray-300 pb-2 text-[1.4rem] text-gray-900">
      Number of Disasters Per Category
    </h2>
    <div class="chart-container max-w-full h-62.5">
      <LayerCake padding={{ top: 0, bottom: 20, left: 30 }} {data}>
          <Html>
          <CirclePack
            {idKey}
            {valueKey}
            fill="#8eb9e3"
            stroke="#023E8A"
            textColor="#61004e"
            textStroke="#ffdbf8"
            textStrokeWidth={1}
          />
          </Html>
      </LayerCake>
    </div>
</div>

<!-- <style>
  /*
    The wrapper div needs to have an explicit width and height in CSS.
    It can also be a flexbox child or CSS grid element.
    The point being it needs dimensions since the <LayerCake> element will
    expand to fill it.
  */
  .chart-container {
    width: 100%;
    height: 250px;
  }
</style> -->