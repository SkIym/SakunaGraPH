<script lang="ts">
  //@ts-nocheck
  import type { ApexOptions } from "apexcharts";
  import { Chart } from "@flowbite-svelte-plugins/chart";
  import { Card, Button, Dropdown, DropdownItem } from "flowbite-svelte";
  import { ChevronDownOutline } from "flowbite-svelte-icons";

  // Data imports
  import { getSortedDisasterList, seriesNames, regionList } from '$lib/components/chart-shapes/chart.svelte.js';
  
  let selectedRegion = $state("All Regions");
  let seriesDropdown = $derived(
    ["All Regions", ...regionList]
      .filter(name => name !== selectedRegion)
  );

  let sortedData = $derived(getSortedDisasterList(selectedRegion));
  let categories = $derived(sortedData.map(([k]) => k));
  let values = $derived(sortedData.map(([, v]) => v));

  let options = $derived({
    series: [
      {
        name: "Disasters",
        color: "var(--color-theme-4)",
        data: values
      }
    ],
    tooltip: {
      enabled: true,
      // Forces white background and dark text for readability
      custom: function({ series, seriesIndex, dataPointIndex, w }) {
        const disasterName = w.globals.labels[dataPointIndex];
        const val = series[seriesIndex][dataPointIndex];
        const color = w.config.series[seriesIndex].color;

        return `
          <div style="background: #ffffff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); font-family: 'Inter', sans-serif;">
            <div style="display: flex; align-items: center; gap: 8px;">
              <span style="background: ${color}; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></span>
              <span style="font-size: 13px; font-weight: 700; color: #111827;">${disasterName}</span>
            </div>
            <div style="margin-top: 4px; font-size: 12px; color: #4b5563;">
              Total Casualties: <span style="font-weight: 600; color: #111827;">${val}</span>
            </div>
          </div>
        `;
      }
    },
    chart: {
      type: "bar",
      width: "100%",
      height: 600,
      toolbar: { show: false },
      animations: { enabled: true }
    },
    plotOptions: {
      bar: {
        horizontal: true,
        borderRadius: 6,
        dataLabels: { position: "center" }
      }
    },
    dataLabels: {
      enabled: true,
      style: { fontSize: '14px', fontWeight: 'bold' },
    },
    xaxis: {
      categories: categories,
      labels: {
        show: true,
        formatter: (val) => String(val),
        style: {cssClass: "dark:fill-gray-300"}
      }
    },
    yaxis: {
      labels: {
        show: true,
        formatter: (val) => String(val),
        style: {cssClass: "dark:fill-gray-300"}
      }
    },
    grid: {
      show: true,
      strokeDashArray: 3,
      padding: { left: 2, right: 2, top: -20 },
    }
  });

  function handleSelect(name: string) {
    selectedRegion = name;
  }
</script>

<Card class="p-4 md:p-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
  <div class="flex flex-row items-center justify-between border-b border-gray-200 pb-3 dark:border-gray-700">
    <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300 py-2">Disaster Rankings for Casualties</h5>
    
    <div class="flex items-center justify-between">
      <Button class="px-3 py-2 w-32 bg-(--color-theme-2)!">
        {selectedRegion} <ChevronDownOutline class="ms-1.5 h-2.5 w-2.5" />
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

  <Chart {options} />
</Card>