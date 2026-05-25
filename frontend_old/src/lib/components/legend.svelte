<script lang="ts">
    import { seriesColors, seriesCount, seriesNames, subType } from '$lib/components/chart-shapes/chart.svelte.js';
</script>

<section>
    <!-- Legend -->
<div class="flex flex-row items-center bg-white dark:bg-gray-700 p-3 w-full rounded-lg border-l-4 border-(--color-theme-1)">
    <p class="text-sm font-medium dark:text-gray-300 mr-4 shrink-0">Legend:</p>
    <div class="flex flex-wrap gap-x-4 gap-y-2">
        {#each {length: seriesCount} as _, i}
            <div class="group relative flex items-center gap-2 cursor-default">
                <!-- Legend item -->
                <div class="h-3.5 w-3.5 rounded-full shrink-0" style="background-color: {seriesColors[i]}"></div>
                <p class="text-sm dark:text-gray-300">{seriesNames[i]}</p>

                <!-- Tooltip on hover -->
                <div class="absolute bottom-full left-0 mb-2 hidden group-hover:block z-10
                            bg-white border border-gray-200 rounded-lg shadow-lg p-4 min-w-max">
                    <!-- Tooltip header -->
                    <div class="flex items-center gap-2 mb-2.5 pb-2 border-b border-gray-200">
                        <div class="h-3.5 w-3.5 rounded-full shrink-0" style="background-color: {seriesColors[i]}"></div>
                        <span class="text-sm font-semibold text-gray-700">{seriesNames[i]}</span>
                    </div>
                    <!-- Subtypes -->
                    <ul class="space-y-1.5">
                        {#each subType[seriesNames[i]] ?? [] as sub}
                            <li class="text-sm text-gray-500">• {sub}</li>
                        {/each}
                    </ul>
                </div>
            </div>
        {/each}
    </div>
</div>
</section>