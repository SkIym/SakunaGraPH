<script>
	import { page } from '$app/stores';
	import { onMount } from 'svelte';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';

	// ── Data ──────────────────────────────────────────────────────────────────
	let event   = $state(null);
	let loading = $state(true);
	let err     = $state('');

	onMount(async () => {
		const id = $page.params.id;
		try {
			const res = await fetch(`/api/events/${id}`);
			const data = await res.json();
			if (!res.ok) { err = data.message ?? 'Event not found.'; return; }
			event = data;
		} catch {
			err = 'Could not reach server.';
		} finally {
			loading = false;
		}
	});

	// ── Helpers ───────────────────────────────────────────────────────────────
	function formatType(raw) {
		if (!raw) return '—';
		return raw.replace(/([a-z])([A-Z])/g, '$1 $2');
	}

	function sourceLabel(s) {
		// "ndrrmc/SitRep_No_..." → "NDRRMC — SitRep No ..."
		const [src, ...rest] = s.split('/');
		const title = rest.join('/').replace(/_/g, ' ').replace(/-+/g, ' ').trim();
		return title ? `${src.toUpperCase()} — ${title}` : src.toUpperCase();
	}

	function eventPath(iri) {
		return '/events/' + iri.replace('https://sakuna.ph/', '');
	}

	const BADGE = {
		MajorEvent: 'bg-red-100 text-red-700',
		Incident:   'bg-amber-100 text-amber-700',
	};

	const CAS_COLOR = {
		DEAD:    'text-red-600',
		INJURED: 'text-amber-600',
		MISSING: 'text-slate-500',
	};
</script>

<svelte:head>
	<title>{event?.name ?? 'Event'} · SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<div class="mx-auto max-w-4xl px-6 py-10">

	<!-- Back link -->
	<a
		href="/map"
		class="inline-flex items-center gap-1.5 text-xs font-semibold uppercase tracking-widest text-slate-400 hover:text-slate-600 transition-colors mb-8"
	>
		<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M19 12H5"/><path d="m12 5-7 7 7 7"/></svg>
		Back to Map
	</a>

	<!-- ── Loading / error states ──────────────────────────────────────────── -->
	{#if loading}
		<div class="flex items-center gap-2 text-slate-400 text-sm">
			<svg class="animate-spin" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-6.219-8.56"/></svg>
			Loading event…
		</div>
	{:else if err}
		<div class="rounded-xl border border-red-200 bg-red-50 px-5 py-4 text-sm text-red-700">{err}</div>
	{:else if event}

		<!-- ── Header ──────────────────────────────────────────────────────── -->
		<div class="mb-8">
			<div class="flex flex-wrap items-center gap-2 mb-3">
				{#if event.type}
					<span class="rounded-full px-2.5 py-0.5 text-[11px] font-bold uppercase tracking-wide {BADGE[event.type] ?? 'bg-slate-100 text-slate-600'}">
						{event.type === 'MajorEvent' ? 'Major Event' : event.type}
					</span>
				{/if}
				{#each event.disasterTypes as dt}
					<span class="rounded-full bg-slate-100 px-2.5 py-0.5 text-[11px] font-semibold text-slate-500">
						{formatType(dt)}
					</span>
				{/each}
			</div>

			<h1
				class="font-black text-slate-800 leading-tight mb-2"
				style="font-family:'Playfair Display',Georgia,serif; font-size:clamp(1.5rem,3vw,2.2rem);"
			>
				{event.name ?? 'Unnamed Event'}
			</h1>

			<div class="flex flex-wrap items-center gap-x-5 gap-y-1 text-sm text-slate-500">
				{#if event.startDate}
					<span>
						<span class="font-medium text-slate-600">{event.startDate}</span>
						{#if event.endDate && event.endDate !== event.startDate}
							→ <span class="font-medium text-slate-600">{event.endDate}</span>
						{/if}
					</span>
				{/if}
				{#if event.locations.length}
					<span class="truncate max-w-xs">{event.locations.slice(0,3).join(' · ')}{event.locations.length > 3 ? ` +${event.locations.length - 3} more` : ''}</span>
				{/if}
			</div>
		</div>

		<!-- ── Two-column grid ─────────────────────────────────────────────── -->
		<div class="grid grid-cols-1 gap-5 sm:grid-cols-2">

			<!-- Affected Population -->
			{#if event.impact.affectedFamilies || event.impact.affectedPersons}
			<div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
				<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Affected Population</p>
				<div class="grid grid-cols-2 gap-3">
					{#if event.impact.affectedFamilies}
					<div>
						<p class="text-2xl font-black text-slate-800">{event.impact.affectedFamilies.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Families</p>
					</div>
					{/if}
					{#if event.impact.affectedPersons}
					<div>
						<p class="text-2xl font-black text-slate-800">{event.impact.affectedPersons.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Persons</p>
					</div>
					{/if}
					{#if event.impact.displacedFamilies}
					<div>
						<p class="text-xl font-bold text-slate-700">{event.impact.displacedFamilies.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Displaced families</p>
					</div>
					{/if}
					{#if event.impact.displacedPersons}
					<div>
						<p class="text-xl font-bold text-slate-700">{event.impact.displacedPersons.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Displaced persons</p>
					</div>
					{/if}
				</div>
			</div>
			{/if}

			<!-- Casualties -->
			{#if event.casualties.length}
			<div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
				<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Casualties</p>
				<div class="flex flex-col gap-3">
					{#each event.casualties as c}
					<div>
						<p class="text-2xl font-black {CAS_COLOR[c.type] ?? 'text-slate-800'}">{c.total.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">{c.type}</p>
						{#if c.causes.length}
							<p class="text-[11px] text-slate-400 mt-1 leading-snug">{c.causes.slice(0,3).join(', ')}</p>
						{/if}
					</div>
					{/each}
				</div>
			</div>
			{/if}

			<!-- Housing Damage -->
			{#if event.impact.totallyDamaged || event.impact.partiallyDamaged}
			<div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
				<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Housing Damage</p>
				<div class="grid grid-cols-2 gap-3">
					{#if event.impact.totallyDamaged}
					<div>
						<p class="text-2xl font-black text-red-600">{event.impact.totallyDamaged.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Totally damaged</p>
					</div>
					{/if}
					{#if event.impact.partiallyDamaged}
					<div>
						<p class="text-2xl font-black text-amber-600">{event.impact.partiallyDamaged.toLocaleString()}</p>
						<p class="text-xs text-slate-400 mt-0.5">Partially damaged</p>
					</div>
					{/if}
				</div>
			</div>
			{/if}

			<!-- Evacuation -->
			{#if event.impact.evacuationCenters}
			<div class="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
				<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Evacuation</p>
				<p class="text-2xl font-black text-slate-800">{event.impact.evacuationCenters.toLocaleString()}</p>
				<p class="text-xs text-slate-400 mt-0.5">Evacuation center{event.impact.evacuationCenters !== 1 ? 's' : ''}</p>
			</div>
			{/if}

		</div>

		<!-- ── Locations (full list) ────────────────────────────────────────── -->
		{#if event.locations.length > 3}
		<div class="mt-5 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
			<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">All Affected Locations</p>
			<div class="flex flex-wrap gap-1.5">
				{#each event.locations as loc}
					<span class="rounded-full bg-slate-100 px-2.5 py-1 text-xs text-slate-600">{loc}</span>
				{/each}
			</div>
		</div>
		{/if}

		<!-- ── Remarks ─────────────────────────────────────────────────────── -->
		{#if event.remarks}
		<div class="mt-5 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
			<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Remarks</p>
			<p class="text-sm text-slate-600 leading-relaxed whitespace-pre-line">{event.remarks}</p>
		</div>
		{/if}

		<!-- ── Alternate Records ───────────────────────────────────────────── -->
		{#if event.alternates.length}
		<div class="mt-5 rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
			<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-3">Same Event — Other Sources</p>
			<div class="flex flex-col divide-y divide-slate-100">
				{#each event.alternates as alt}
				<a
					href={eventPath(alt.iri)}
					class="flex items-center gap-3 py-2.5 text-sm hover:bg-slate-50 -mx-5 px-5 transition-colors rounded-xl"
				>
					<span class="rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wide {BADGE[alt.type] ?? 'bg-slate-100 text-slate-500'}">
						{alt.type === 'MajorEvent' ? 'Major' : alt.type ?? '?'}
					</span>
					<span class="flex-1 text-slate-700 truncate">{alt.name ?? alt.iri}</span>
					<span class="text-[10px] text-slate-400 font-mono">{alt.iri.replace('https://sakuna.ph/','').split('/')[0]}</span>
					<svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="text-slate-300"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg>
				</a>
				{/each}
			</div>
		</div>
		{/if}

		<!-- ── Data Sources ────────────────────────────────────────────────── -->
		{#if event.sources.length}
		<div class="mt-5 rounded-2xl border border-slate-100 bg-slate-50 p-5">
			<p class="text-[10px] font-bold uppercase tracking-widest text-slate-400 mb-2">Data Source{event.sources.length > 1 ? 's' : ''}</p>
			{#each event.sources as src}
				<p class="text-xs text-slate-500 leading-relaxed">{sourceLabel(src)}</p>
			{/each}
		</div>
		{/if}

	{/if}
</div>
