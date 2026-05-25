<script lang="ts">
  //@ts-nocheck
  import type { ApexOptions } from "apexcharts";
  import { Chart } from "@flowbite-svelte-plugins/chart";
  import { Card, A, Button, Dropdown, DropdownItem, Popover, Tooltip } from "flowbite-svelte";
  import { InfoCircleSolid, ArrowDownToBracketOutline, ChevronDownOutline, ChevronRightOutline } from "flowbite-svelte-icons";
  
  // lucide icons
  import { UsersRound, PhilippinePeso } from 'lucide-svelte';

  // chart.js
  //import disaster from '$lib/demo_data/disaster_data.csv';
  import { getFilteredCsvData } from "$lib/demo_data/filteredData.svelte";
  let disaster = $derived(getFilteredCsvData());
  import { disasterColors, subType, generalizeType, allTypes, seriesColors, seriesNames } from '$lib/components/chart-shapes/chart.svelte.js';
  
  // data prepocessing
  let tollCount = $derived(disaster.reduce((acc, item) => acc + (Number(item.casualties) || 0), 0));

  let monetaryDamage = $derived(disaster.reduce((acc, item) => acc + (Number(item.monetary_damage) || 0), 0));

  const formatter = new Intl.NumberFormat('en-US', {
    notation: 'compact',
    compactDisplay: 'short'
  });
  let formattedMonetaryDamage = $derived(formatter.format(monetaryDamage));

  let affectedFamilies = $derived(disaster.reduce((acc, item) => acc + (Number(item.affected_families) || 0), 0));
</script>

<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
    <Card class="p-4 md:p-4 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
        <div class="flex flex-row">
            <UsersRound 
                color="var(--color-theme-2)" 
                size={72} 
                strokeWidth={2} 
                class="mr-1 shrink-0 w-12 h-12 md:w-[72px] md:h-[72px]"
            />
            <div class="flex flex-col gap-1">
                <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">
                    Toll Count
                </h5>
                <h1 class="font-extrabold text-(--color-theme-2) text-[clamp(1.8rem,5vw,3rem)] leading-none tabular-nums">
                    {tollCount}
                </h1>
            </div>
        </div>
    </Card>

    <Card class="p-4 md:p-4 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
        <div class="flex flex-row">
            <PhilippinePeso 
                color="var(--color-theme-2)" 
                size={72} 
                strokeWidth={2} 
                class="mr-1 shrink-0 w-12 h-12 md:w-[72px] md:h-[72px]"
            />
            <div class="flex flex-col gap-1">
                <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">
                    Economic Damage
                </h5>
                <h1 class="font-extrabold text-(--color-theme-2) text-[clamp(1.8rem,5vw,3rem)] leading-none tabular-nums">
                    {formattedMonetaryDamage}
                </h1>
            </div>
        </div>
    </Card>

    <Card class="p-4 md:p-4 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] max-w-full!">
        <div class="flex flex-row">
            <svg 
                class="mr-1 shrink-0 w-12 h-12 md:w-[72px] md:h-[72px]"
                xmlns="http://www.w3.org/2000/svg" 
                viewBox="0 0 48 48"
            >
                <g fill="none" stroke="#049" stroke-linecap="round" stroke-width="3.5">
                    <path d="M10 19s-5.143 2-6 9m34-9s5.143 2 6 9m-26-9s4.8 1.167 6 7m6-7s-4.8 1.167-6 7m-4 8s-4.2.75-6 6m14-6s4.2.75 6 6"/>
                    <circle cx="24" cy="31" r="5" stroke-linejoin="round"/>
                    <circle cx="34" cy="14" r="6" stroke-linejoin="round"/>
                    <circle cx="14" cy="14" r="6" stroke-linejoin="round"/>
                </g>
            </svg>
            <div class="flex flex-col gap-1">
                <h5 class="me-1 text-xl leading-none font-bold text-gray-900 dark:text-gray-300">
                    Affected Families
                </h5>
                <h1 class="font-extrabold text-(--color-theme-2) text-[clamp(1.8rem,5vw,3rem)] leading-none tabular-nums">
                    {affectedFamilies}
                </h1>
            </div>
        </div>
    </Card>
</div>