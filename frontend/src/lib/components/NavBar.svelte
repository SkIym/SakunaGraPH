<script>
	import { page } from '$app/stores';

	const links = [
		{ href: '/', label: 'Home' },
		{ href: '/map', label: 'Map' },
		{ href: '/ontology', label: 'Ontology' },
		// { href: '/ask', label: 'Ask' }
	];
</script>

<nav
	class="fixed left-0 right-0 top-0 z-20 flex items-center justify-center gap-1 px-6 bg-transparent"
    style="height:52px;"
>
	{#each links as link}
		{@const active = $page.url.pathname === link.href}
		<a
			href={link.href}
			class="relative px-4 pb-0 text-sm  transition-colors duration-150 rounded-lg
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
