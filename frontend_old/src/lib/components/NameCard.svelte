<script>
    let { picture = null, name, department, email, college, university } = $props();
</script>

<div class="mt-2 bg-gray-50 p-6 rounded-lg border-t-4 border-(--color-theme-1) flex flex-col items-center text-center w-64 dark:bg-gray-700">
    <div class="w-40 h-40 rounded-full overflow-hidden bg-gray-200 flex items-center justify-center shrink-0">
        {#if picture}
            <img class="w-full h-full object-cover" src={picture} alt="Profile photo of {name}" />
        {:else}
            <span class="text-gray-400 text-sm">No Image</span>
        {/if}
    </div>

    <p class="mt-4 text-2xl font-semibold tracking-tight text-heading dark:text-white">{name}</p>

    <div class="mt-2 space-y-1">
        <p class="italic text-sm text-gray-700 dark:text-gray-300">{department}</p>
        <p class="text-sm text-gray-700 dark:text-gray-300">{college}</p>
        <p class="text-sm text-gray-700 dark:text-gray-300">{university}</p>
    </div>
</div>