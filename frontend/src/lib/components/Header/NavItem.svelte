<script>
    // Use $props() to receive data in Svelte 5
    let { label, href, children, onclick } = $props();
</script>

<a
    {href}
    {onclick}
    class="block px-4 py-3 text-base text-white font-semibold transition-colors duration-200 hover:text-(--color-theme-1)"
>
    {@render children?.()}
    {label}
</a>