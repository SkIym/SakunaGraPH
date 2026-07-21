<script>
	let { tabs = [], active = '', onChange = () => {} } = $props();
</script>

<div
	class="absolute top-4 left-1/2 z-30 flex -translate-x-1/2 items-center gap-1 rounded-full border border-slate-200/70 bg-white/88 px-2 py-1.5 shadow-md"
	style="backdrop-filter:blur(12px);"
	aria-label="Ontology views"
>
	<span
		class="border-r border-slate-200 pr-3 pl-2 text-[10px] font-bold tracking-widest text-slate-600 uppercase"
		>SakunaGraPH</span
	>
	{#each tabs as tab}
		<button
			type="button"
			aria-pressed={active === tab.id}
			onclick={() => onChange(tab.id)}
			class="rounded-full px-4 py-1.5 text-[11px] font-semibold whitespace-nowrap transition-all"
			style={active === tab.id ? 'background:#1e293b; color:white;' : 'color:#64748b;'}
		>
			{tab.label}
		</button>
	{/each}
</div>
