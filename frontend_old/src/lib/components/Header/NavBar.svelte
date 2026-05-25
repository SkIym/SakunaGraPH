<script>
	import { page } from '$app/state';

    let { label, image, href, match = null, children = null } = $props();

    let isActive = $derived(
	href === '/'
		? page.url.pathname === '/'
		: match
			? page.url.pathname.startsWith(match)
			: page.url.pathname === href);
</script>

<ul class="group relative h-full">
	
	<a
		href={href === '/' || href === '/about' ? href : ""}
		class="flex h-full items-center px-2 text-base font-bold tracking-wider
		       transition-colors duration-200
		       {isActive ? 'text-yellow-400' : 'text-white hover:text-yellow-400'}"
	>
		{#if image}
			<img src={image} alt="Profile" class="h-12 w-12 p-1 rounded-full object-cover" />
			<span class="ml-1">{label}</span>
		{:else}
			{label}
		{/if}
		{#if href !== '/'}
			<svg class="w-4 h-4 ms-1.5 -me-0.5" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" 
			width="24" height="24" fill="none" viewBox="0 0 24 24"><path stroke="currentColor" stroke-linecap="round"
			stroke-linejoin="round" stroke-width="2" d="m19 9-7 7-7-7"/></svg>
		{/if}
	</a>

	{#if isActive}
		{#if href === '/'}
			<span
				class="absolute bottom-0 left-[50%] h-1 w-[75%] -translate-x-1/2
					rounded bg-(--color-theme-1)"
			></span>
		{:else}
			<span
				class="absolute bottom-0 left-[40%] h-1 w-[75%] -translate-x-1/2
					rounded bg-(--color-theme-1)"
			></span>
		{/if}
	{/if}

	{#if children}
		<ul
			class="absolute left-0 top-full z-50 hidden min-w-45 flex-col
			       rounded bg-(--color-theme-2) shadow-md group-hover:flex border-2 border-white"
		>
			{@render children()}
		</ul>
	{/if}
</ul>
