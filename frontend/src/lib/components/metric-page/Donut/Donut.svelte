<script lang="ts">
  //@ts-nocheck
  import type { ApexOptions } from "apexcharts";
  import { Chart } from "@flowbite-svelte-plugins/chart";
  import { Card, A, Button, Dropdown, DropdownItem, Popover, Tooltip } from "flowbite-svelte";
  import { InfoCircleSolid, ArrowDownToBracketOutline, ChevronDownOutline, ChevronRightOutline } from "flowbite-svelte-icons";
  
  // chart.js
  //import disaster from '$lib/demo_data/disaster_data.csv';
  import { getFilteredCsvData } from "$lib/demo_data/filteredData.svelte";
  let disaster = $derived(getFilteredCsvData());
  import { disasterColors, subType, generalizeType, allTypes, seriesColors, seriesNames } from '$lib/components/chart-shapes/chart.svelte.js';
  
  // data prepocessing
  /* let disasterCount = $derived(disaster.reduce((acc, item) => {
    let type = item.type?.trim();  
    let generalType = generalizeType(type);
    acc[generalType] = (acc[generalType] || 0) + 1;
    return acc;
  }, {})); */

  //let seriesCount = $derived(seriesNames.map(name => disasterCount[name]));
  let seriesCount = $derived(
    seriesNames.map(name => {
      return disaster.filter(item => generalizeType(item.type?.trim()) === name).length;
    })
  );

  let options: ApexOptions = $derived({
    // series: [35.1, 23.5, 2.4, 5.4],
    // colors: ["#1C64F2", "#16BDCA", "#FDBA8C", "#E74694"],
    series: seriesCount,
    colors: seriesColors,
    chart: {
      height: 320,
      width: "100%",
      type: "donut"
    },
    stroke: {
      colors: ["transparent"]
    },
    plotOptions: {
      pie: {
        expandOnClick: false,
        donut: {
          labels: {
            show: true,
            name: {
              show: true,
              fontFamily: "Inter, sans-serif",
              offsetY: 20
            },
            total: {
              showAlways: true,
              show: true,
              label: "Unique records",
              fontFamily: "Inter, sans-serif",
              formatter: function (w) {
                const sum = w.globals.seriesTotals.reduce((a: number, b: number) => {
                  return a + b;
                }, 0);
                return `${sum}`;
              }
            },
            value: {
              show: true,
              fontFamily: "Inter, sans-serif",
              offsetY: -20,
              formatter: function (value) {
                return value;
              }
            }
          },
          size: "80%"
        }
      }
    },
    grid: {
      padding: {
        top: -2
      }
    },
    labels: seriesNames,
    dataLabels: {
      enabled: false
    },
    legend: {
      position: "bottom",
      fontFamily: "Inter, sans-serif"
    },
    yaxis: {
      labels: {
        formatter: function (value) {
          return value;
        }
      }
    },
    xaxis: {
      labels: {
        formatter: function (value) {
          return value;
        }
      },
      axisTicks: {
        show: false
      },
      axisBorder: {
        show: false
      }
    },
    tooltip: {
      enabled: true,
      custom: function({ series, seriesIndex, w }) {
        const label = w.globals.labels[seriesIndex];
        const value = series[seriesIndex];
        const color = w.globals.colors[seriesIndex];

        // hardcoded tooltip styling HAHAHHHA 
        return `
          <div style="background: #fff; color: #111827; padding: 12px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); display: flex; align-items: center; gap: 8px; font-family: 'Inter', sans-serif;">
            <span style="background-color: ${color}; width: 12px; height: 12px; border-radius: 50%; display: inline-block;"></span>
            <span style="font-weight: 500;">${label}:</span>
            <span style="font-weight: 700;">${value}</span>
          </div>
        `;
      }
    }
  });
</script>

<Card class="p-4 md:p-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
  <div class="flex w-full items-start justify-between">
    <div class="flex-col items-center">
      <div class="mb-1 flex items-center">
        <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">Disaster Count</h5>
        <InfoCircleSolid id="donut1" class="ms-1 h-3.5 w-3.5 cursor-pointer text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white" />
        <Popover triggeredBy="#donut1" class="z-10 w-72 rounded-lg border border-gray-200 bg-white text-sm text-gray-500 shadow-xs dark:border-gray-600 dark:bg-gray-800 dark:text-gray-400">
          <div class="space-y-2 p-3">
            <h3 class="font-semibold text-gray-900 dark:text-white">Disaster Frequency</h3>
            <p>Report displays the most common disaster types occurring over the Philippines or a specific area. </p>
          </div>
        </Popover>
      </div>
    </div>
    <!-- <div class="flex items-center justify-end">
      <ArrowDownToBracketOutline class="h-3.5 w-3.5 dark:text-gray-300" />
      <Tooltip>Download CSV</Tooltip>
    </div> -->
  </div>

  <Chart {options} class="py-6 dark:text-gray-300!" />

</Card>