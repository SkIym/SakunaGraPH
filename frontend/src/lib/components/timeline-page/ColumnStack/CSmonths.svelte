<script>
    //@ts-nocheck 

    import { LayerCake, Svg, flatten, stack } from 'layercake';

    import { scaleBand, scaleOrdinal } from 'd3-scale';
    import { format } from 'd3-format';

    import ColumnStacked from '$lib/components/timeline-page/ColumnStack/ColumnStacked.svelte';
    import AxisX from '$lib/components/chart-shapes/AxisX.svelte';
    import AxisY from '$lib/components/chart-shapes/AxisY.svelte';

    // This example loads csv data as json and converts numeric columns to numbers using @rollup/plugin-dsv. See vite.config.js for details
    // import data from '$lib/demo_data/fruitOrdinal.csv';

    //import disaster from '$lib/demo_data/disaster_data.csv';
    import { disasterColors, subType, generalizeType, getProcessedMonths, allTypes, seriesColors, seriesNames } from '$lib/components/chart-shapes/chart.svelte.js';

    const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
   
    

    let data = $derived(Object.entries(getProcessedMonths())
    .sort(([a], [b]) => Number(a) - Number(b)) // "01" → "12"
    .map(([month, types]) => {
        const obj = { month };

        allTypes.forEach(type => {
            obj[type] = types[type] || 0;
        });

        obj.monthLabel = months[+obj.month - 1];

        return obj;
    })); 

    /* data.forEach(d => {
        d.monthLabel = months[+d.month - 1];
    }); */

    const xKey = 'monthLabel';
    const yKey = [0, 1];
    const zKey = 'key';


    const formatLabelY = d => format(`~s`)(d);
    let stackedData = $derived(stack(data, seriesNames));
</script>

<div id="columnStack" class="">
    <h2 class = "mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 text-[1.4rem] text-gray-900 dark:text-gray-300">
			Disasters Per Month, Stacked by Category
	</h2>
    <div class="max-w-full h-62.5 chart-container">
        <LayerCake
            padding={{ top: 0, right: 0, bottom: 20, left: 20 }}
            x={d => d.data[xKey]}
            y={yKey}
            z={zKey}
            xScale={scaleBand().paddingInner(0.02).round(true)}
            xDomainSort={false}
            zScale={scaleOrdinal()}
            zDomain={seriesNames}
            zRange={seriesColors}
            flatData={flatten(stackedData)}
            data={stackedData}
        >
            <Svg>
            <AxisX gridlines={false} />
            <AxisY ticks={4} gridlines={false} format={formatLabelY} />
            <ColumnStacked stroke="#000" strokeWidth={0.5} />
            </Svg>
        </LayerCake>
    </div>        
</div>