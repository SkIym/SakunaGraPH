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
	class="primary-nav fixed top-0 right-0 left-0 z-20 w-full max-w-full overflow-x-auto bg-transparent px-1 sm:px-6"
	style="height:var(--app-nav-height);"
	aria-label="Primary navigation"
>
	<div class="mx-auto flex h-full w-max min-w-max items-center gap-0.5 sm:gap-1">
		{#each links as link}
			{@const active = isActive(link.href, $page.url.pathname)}
			<a
				href={link.href}
				aria-current={active ? 'page' : undefined}
				class="touch-target relative flex items-center rounded-lg px-2.5 text-xs whitespace-nowrap transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-700 focus-visible:ring-offset-2 sm:px-4 sm:text-sm
				{active ? 'text-slate-800' : 'text-slate-600 hover:text-slate-800 hover:bg-slate-100/70'}"
			>
				{link.label}
				{#if active}
					<span
						aria-hidden="true"
						class="absolute top-full left-1/2 mt-0.5 h-1 w-1 -translate-x-1/2 rounded-full bg-slate-800"
					></span>
				{/if}
			</a>
		{/each}
	</div>
</nav>

<style>
	.primary-nav {
		width: 100vw;
		max-width: 100vw;
		scrollbar-width: none;
		overscroll-behavior-inline: contain;
	}

	.primary-nav::-webkit-scrollbar {
		display: none;
	}
</style>
