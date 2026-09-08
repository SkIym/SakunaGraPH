<script>
	let { tabs = [], active = '', onChange = () => {} } = $props();
</script>

<div
	class="ontology-tabs absolute z-30 flex items-center gap-1 border border-slate-200/70 bg-white/95 px-2 py-1.5 shadow-md"
	style="backdrop-filter:blur(12px);"
	aria-label="Ontology views"
>
	<span
		class="ontology-tabs-brand border-r border-slate-200 pr-3 pl-2 text-[10px] font-bold tracking-widest text-slate-600 uppercase"
		>SakunaGraPH</span
	>
	{#each tabs as tab}
		<button
			type="button"
			aria-pressed={active === tab.id}
			onclick={() => onChange(tab.id)}
			class="min-h-11 rounded-full px-4 py-2 text-[11px] font-semibold whitespace-nowrap transition-all"
			style={active === tab.id ? 'background:#1e293b; color:white;' : 'color:#64748b;'}
		>
			{tab.label}
		</button>
	{/each}
</div>

<style>
	.ontology-tabs {
		top: 0.75rem;
		left: 0.75rem;
		right: 0.75rem;
		overflow-x: auto;
		border-radius: 0.875rem;
		scrollbar-width: thin;
	}

	@media (max-width: 479px) {
		.ontology-tabs-brand {
			display: none;
		}
	}

	@media (min-width: 768px) {
		.ontology-tabs {
			top: 1rem;
		left: 50%;
		right: auto;
		transform: translateX(-50%);
		border-radius: 9999px;
		overflow: visible;
	}
	}
</style>
