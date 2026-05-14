<script>
    //@ts-nocheck
    import { LayerCake, Svg, WebGL, Html } from 'layercake';

    // LayerCake
    import ScatterWebGL from '$lib/components/metric-page/Scatter/Scatter.webgl.svelte';
    import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
    import AxisY from '$lib/components/chart-shapes/AxisY.svelte';
    import QuadTree from '$lib/components/chart-shapes/QuadTree.html.svelte';
    

    //flowbite
    import { Card } from 'flowbite-svelte';

    // data
    import disaster from '$lib/demo_data/disaster_data.csv'

    // chart.js
    import { disasterColors, generalizeType } from '$lib/components/chart-shapes/chart.svelte.js';

    const data = disaster
        .filter(d => d.affected_families && d.monetary_damage)
        .map(d => ({
        affected_families: +d.affected_families,
        monetary_damage: +d.monetary_damage / 1000000,
        type: generalizeType(d.type?.trim())
    }));
    const xKey = 'affected_families';
    const yKey = 'monetary_damage';
    const zKey = 'type';

    const r = 3;
    const xyPadding = 6;
</script>

<div id="multiline" class="">
  <Card class="p-4 md:p-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
    <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">Economic Damage (Millions) VS Affected Household </h5>
    <div class="chart-container flex w-full max-w-full h-100">
      <LayerCake
        padding={{ top: 20, right: 5, bottom: 20, left: 25 }}
        x={xKey}
        y={yKey}
        xPadding={[xyPadding, xyPadding]}
        yPadding={[xyPadding, xyPadding]}
        {data}
      >
        <Svg>
          <AxisX />
          <AxisY tickMarks={false} ticks={5} />
        </Svg>
          
        <WebGL>
          <ScatterWebGL {r} colors={disasterColors} zKey={zKey}/>
        </WebGL>

        <Html>
          <QuadTree>
            {#snippet children({ x, y, visible, found })}
              <div
                class="circle absolute rounded-full bg-(--color-theme-2) -translate-x-1/2 -translate-y-1/2 pointer-events-none w-2.5 h-2.5"
                style="top:{y}px;left:{x}px;display: {visible ? 'block' : 'none'};"
              ></div>

              {#if visible && found}
                <div
                  class="absolute z-50 pointer-events-none whitespace-nowrap rounded-lg border border-gray-200 bg-white p-3 shadow-sm"
                  style="top:{y}px; left:{x + 15}px; transform: translateY(-50%);"
                >
                  <div class="flex -flex-row mb-2 border-b border-gray-500 p-1 text-sm text-gray-500">
                    <div class="mt-1 mr-1 h-2.5 w-2.5 rounded-full bg-(--c)" style="--c: {disasterColors[found.type]}"></div>
                    {found.type}
                  </div>

                  <div class="space-y-1">
                    <div class="flex items-center justify-between gap-4 text-sm p-1">
                      <span class="font-normal text-gray-500">Economic Damage:</span>
                      <span class="font-bold text-gray-900">P{found.monetary_damage.toFixed()}M</span>
                    </div>
                    <div class="flex items-center justify-between gap-4 text-sm p-1">
                      <span class="font-normal text-l text-gray-500">Families:</span>
                      <span class="font-bold text-l text-gray-900">{found.affected_families.toLocaleString()}</span>
                    </div>
                  </div>
                </div>
              {/if}
            {/snippet}
          </QuadTree>
        </Html>
      </LayerCake>
    </div>
  </Card>
</div>