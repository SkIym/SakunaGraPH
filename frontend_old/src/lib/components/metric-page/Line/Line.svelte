<script lang="ts">
  //@ts-nocheck
  import type { ApexOptions } from "apexcharts";
  import { Chart } from "@flowbite-svelte-plugins/chart";
  import { Card, A, Button, Dropdown, DropdownItem, Popover } from "flowbite-svelte";
  import { InfoCircleSolid, ChevronRightOutline, ChevronDownOutline, FileLinesSolid } from "flowbite-svelte-icons";

  // Data imports

  // chart.js
  import { getVictimCount, seriesNames, allYears } from '$lib/components/chart-shapes/chart.svelte.js';
  let selectedType = $state("All Types");
  let seriesDropdown = $derived(
    [...seriesNames, "All Types"]
      .filter(name => name !== selectedType)
  );
  let result = $derived(getVictimCount(selectedType));
  let victimData = $derived(Object.entries(result.data).sort(([a], [b]) => a - b));
  let casualtyList = $derived(victimData.map(([,v]) => v.casualty));
  let missingList  = $derived(victimData.map(([,v]) => v.miss));
  let injuredList  = $derived(victimData.map(([,v]) => v.injury));

  let totalCasualty = $derived(result.totals.casualty);
  let totalMissing = $derived(result.totals.miss);
  let totalInjured = $derived(result.totals.injury);

  let options = $derived({
    chart: {
      height: "300px",
      type: "line",
      fontFamily: "Inter, sans-serif",
      dropShadow: {
        enabled: false
      },
      toolbar: {
        show: false
      }
    },
    tooltip: {
      enabled: true,
      // Custom HTML to force white background and dark text
      custom: function({ series, dataPointIndex, w }) {
        const year = w.globals.categoryLabels[dataPointIndex];
        
        // We grab the values and colors directly from the chart's internal state
        // series[0] = Casualties, [1] = Injured, [2] = Missing
        const items = w.config.series.map((s, i) => {
          return `
            <div style="display: flex; justify-content: space-between; align-items: center; gap: 16px;">
              <div style="display: flex; align-items: center; gap: 8px;">
                <span style="background: ${s.color}; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></span>
                <span style="font-size: 12px; color: #4b5563;">${s.name}:</span>
              </div>
              <span style="font-weight: 700; font-size: 12px; color: #111827;">${series[i][dataPointIndex]}</span>
            </div>
          `;
        }).join('');

        return `
          <div style="background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); font-family: 'Inter', sans-serif;">
            <div style="margin-bottom: 8px; font-weight: 700; color: #111827; border-bottom: 1px solid #f3f4f6; padding-bottom: 4px;">
              ${year}
            </div>
            <div style="display: flex; flex-direction: column; gap: 4px;">
              ${items}
            </div>
          </div>
        `;
      }
    },
    dataLabels: {
      enabled: false
    },
    stroke: {
      width: 6,
      curve: "smooth"
    },
    grid: {
      show: true,
      strokeDashArray: 4,
      borderColor: "#4b5563",
      row: { opacity: 1 },
      yaxis: {
        lines: {
          show: true // Ensures horizontal dashed lines show up
        }
      },
      padding: {
        left: 2,
        right: 2,
        top: -26
      }
    },
    series: [
      {
        name: "Casualties",
        data: casualtyList,
        color: "var(--color-theme-5)"
      },
      {
        name: "Injured",
        data: injuredList,
        color: "var(--color-theme-4)"
      },
      {
        name: "Missing",
        data: missingList,
        color: "var(--color-theme-3)"
      }
    ],
    legend: {
      show: false
    },
    xaxis: {
      categories: allYears,
      labels: {
        show: true,
        style: {
          fontFamily: "Inter, sans-serif",
          cssClass: "text-xs font-normal fill-gray-900 dark:fill-gray-300"
        }
      },
      axisBorder: {
        show: false
      },
      axisTicks: {
        show: false
      },
      tooltip: {
        enabled: false
      }
    },
    yaxis: {
      show: false
    }
  });

  function handleSelect(name: string) {
    selectedType = name;
  }
</script>

<Card class="p-4 md:p-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
  <div class="mb-5 flex justify-between">
    <div class="grid grid-cols-3 gap-4">
      <div>
        <h5 class="mb-2 inline-flex items-center leading-none font-normal text-gray-900 dark:text-gray-300">
          Casualty
          <InfoCircleSolid id="b1" class="ms-1 h-3 w-3 cursor-pointer text-gray-400 hover:text-gray-900 dark:hover:text-white" />
          <Popover triggeredBy="#b1" class="z-10 w-72 rounded-lg border border-gray-200 bg-white text-sm text-gray-500 shadow-xs dark:border-gray-600 dark:bg-gray-800 dark:text-gray-400">
            <div class="space-y-2 p-3">
              <h3 class="font-semibold text-gray-900 dark:text-white">Casualty Count</h3>
              <p>Recorded casualty count for each year filtered by disaster type.</p>
            </div>
          </Popover>
        </h5>
        <p class="text-2xl leading-none font-bold text-(--color-theme-5)">{totalCasualty}</p>
      </div>
      <div>
        <h5 class="mb-2 inline-flex items-center leading-none font-normal text-gray-900 dark:text-gray-300">
          Injured
          <InfoCircleSolid id="b2" class="ms-1 h-3 w-3 cursor-pointer text-gray-400 hover:text-gray-900 dark:hover:text-white" />
          <Popover triggeredBy="#b2" class="z-10 w-72 rounded-lg border border-gray-200 bg-white text-sm text-gray-500 shadow-xs dark:border-gray-600 dark:bg-gray-800 dark:text-gray-400">
            <div class="space-y-2 p-3">
              <h3 class="font-semibold text-gray-900 dark:text-white">Accumulated Injuries</h3>
              <p>Counted injuries, including minor classification, for each year filtered by disaster type.</p>
            </div>
          </Popover>
        </h5>
        <p class="text-2xl leading-none font-bold text-(--color-theme-4)">{totalInjured}</p>
      </div>
      <div>
        <h5 class="mb-2 inline-flex items-center leading-none font-normal text-gray-900 dark:text-gray-300">
          Missing
          <InfoCircleSolid id="b3" class="ms-1 h-3 w-3 cursor-pointer text-gray-400 hover:text-gray-900 dark:hover:text-white" />
          <Popover triggeredBy="#b3" class="z-10 w-72 rounded-lg border border-gray-200 bg-white text-sm text-gray-500 shadow-xs dark:border-gray-600 dark:bg-gray-800 dark:text-gray-400">
            <div class="space-y-2 p-3">
              <h3 class="font-semibold text-gray-900 dark:text-white">Missing People</h3>
              <p>Reported missing people, found or not found, for each year filtered by disaster type.</p>
            </div>
          </Popover>
        </h5>
        <p class="text-2xl leading-none font-bold text-(--color-theme-3)">{totalMissing}</p>
      </div>
    </div>
    <div class="flex items-center justify-between">
      <Button class="px-3 py-2 w-48 bg-(--color-theme-2)!">
        {selectedType} <ChevronDownOutline class="ms-1.5 h-2.5 w-2.5" />
      </Button>
      
      <Dropdown simple class="w-48 overflow-y-auto max-h-60">
        {#each seriesDropdown as name}
          <DropdownItem onclick={() => handleSelect(name)}>
            {name}
          </DropdownItem>
        {/each}
      </Dropdown>
    </div>
  </div>
  <Chart {options}/>
</Card>