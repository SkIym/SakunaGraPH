<script>
	import { Table } from "@flowbite-svelte-plugins/datatable";
    let { data = [] } = $props();

    let stateData = $derived(data);
</script>

<section class="w-full">
	<div class="w-full h-auto min-h-75 sm:min-h-100 md:min-h-125 lg:min-h-156.25 dark:text-gray-300 overflow-x-auto">
        {#if stateData.length === 0}
            <p>No data available.</p>
        {:else}
            {#key stateData}
                <Table items={stateData} dataTableOptions={{selectable: true, sortable: true}} />
            {/key}
        {/if}            
	</div>
</section>

<!-- <style>
    .chart-container {
        width: 100%;
        height: 250px;
    }
</style> -->