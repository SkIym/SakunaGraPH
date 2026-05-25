<script>
	import { page } from '$app/stores';

	const links = [
		{ href: '/', label: 'Home' },
		{ href: '/map', label: 'Map' }
	];
</script>

<nav
	class="fixed left-0 right-0 top-0 z-20 flex items-center justify-center gap-1 px-6"
	style="height:52px; background:rgba(255,255,255,0.75); backdrop-filter:blur(12px); border-bottom:1px solid rgba(226,232,240,0.7);"
>
	{#each links as link}
		{@const active = $page.url.pathname === link.href}
		<a
			href={link.href}
			class="relative px-4 pt-1.5 pb-0 text-sm font-medium transition-colors duration-150 rounded-lg
			{active
				? 'text-slate-800'
				: 'text-slate-400 hover:text-slate-600 hover:bg-slate-100/70'}"
		>
			{link.label}
			{#if active}
				<!-- Dot sits 3px below the text baseline -->
				<span
					class="absolute top-full left-1/2 mt-0.5 h-1 w-1 -translate-x-1/2 rounded-full bg-slate-800"
				></span>
			{/if}
		</a>
	{/each}
</nav>
