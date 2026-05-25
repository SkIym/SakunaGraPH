<script lang="ts">
    import { setAllData } from '$lib/demo_data/filteredData.svelte';

    let { data, children } = $props();

    $effect(() => {
        if (data.events) setAllData(data.events);
    });
</script>

{#if !data.events}
    <div class="flex justify-center items-center h-64 text-gray-500 dark:text-gray-400">
        <p class="text-sm">Loading disaster data from GraphDB…</p>
    </div>
{/if}

{@render children()}
