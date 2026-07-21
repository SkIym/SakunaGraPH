<script>
	import { page } from '$app/stores';

	const links = [
		{ href: '/', label: 'Home' },
		{ href: '/map', label: 'Map' },
		{ href: '/ontology', label: 'Ontology' },
		{ href: '/ask', label: 'Ask' },
		{ href: '/analysis', label: 'Analysis' },
	];

	function isActive(href, pathname) {
		return href === '/' ? pathname === href : pathname === href || pathname.startsWith(`${href}/`);
	}
</script>

<nav
	class="fixed left-0 right-0 top-0 z-20 flex items-center justify-center gap-0.5 bg-transparent px-1 sm:gap-1 sm:px-6"
	style="height:52px;"
>
	{#each links as link}
		{@const active = isActive(link.href, $page.url.pathname)}
		<a
			href={link.href}
			class="relative rounded-lg px-2.5 pb-0 text-xs transition-colors duration-150 sm:px-4 sm:text-sm
			{active ? 'text-slate-800' : 'text-slate-600 hover:text-slate-800 hover:bg-slate-100/70'}"
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
