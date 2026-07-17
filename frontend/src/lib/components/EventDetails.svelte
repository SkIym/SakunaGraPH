<script>
	import { apiJson, withQuery } from '$lib/api.js';

	let { event = '', onclose = () => {} } = $props();

	let details = $state(null);
	let loading = $state(true);
	let error = $state('');
	let retryToken = $state(0);
	let locationsExpanded = $state(false);
	let incidentsExpanded = $state(false);
	let expandedForEvent = $state('');
	const requestVersion = $derived({ event, retry: retryToken });
	const dateFormat = new Intl.DateTimeFormat('en-PH', {
		year: 'numeric',
		month: 'long',
		day: 'numeric'
	});

	$effect(() => {
		if (event && event !== expandedForEvent) {
			expandedForEvent = event;
			locationsExpanded = false;
			incidentsExpanded = false;
		}
	});

	$effect(() => {
		const request = requestVersion;
		if (!request.event) return;
		const controller = new AbortController();
		loading = true;
		error = '';
		details = null;

		void apiJson(withQuery('/api/disasters/details', { uri: request.event }), {
			signal: controller.signal
		})
			.then((data) => (details = data))
			.catch((requestError) => {
				if (requestError.name !== 'AbortError') {
					error = requestError.message || 'Could not load event details.';
				}
			})
			.finally(() => {
				if (!controller.signal.aborted) loading = false;
			});

		return () => controller.abort();
	});

	function formatDate(value) {
		if (!value) return 'Not recorded';
		const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
		return Number.isNaN(parsed.valueOf()) ? value : dateFormat.format(parsed);
	}

	function safeReportLink(value) {
		if (!value) return null;
		try {
			const url = new URL(value);
			return ['http:', 'https:'].includes(url.protocol) ? url.href : null;
		} catch {
			return null;
		}
	}

	function handleKeydown(keyboardEvent) {
		if (keyboardEvent.key === 'Escape') onclose();
	}
</script>

<svelte:window onkeydown={handleKeydown} />

<button
	type="button"
	onclick={onclose}
	class="fixed inset-0 z-50 cursor-default bg-slate-950/30 backdrop-blur-[2px]"
	aria-label="Close event details"
></button>

<div
	role="dialog"
	aria-modal="true"
	aria-labelledby="event-details-title"
	class="fixed inset-y-0 right-0 z-[60] flex w-[min(94vw,580px)] flex-col border-l border-slate-200 bg-white shadow-2xl"
>
	<header class="flex shrink-0 items-start justify-between gap-4 border-b border-slate-200 px-5 py-4 sm:px-6">
		<div class="min-w-0">
			<p class="text-[9px] font-semibold uppercase text-indigo-600" style="letter-spacing:0.12em;">Event details</p>
			<h2 id="event-details-title" class="mt-1 line-clamp-2 text-lg font-semibold leading-6 text-slate-800">
				{details?.name ?? (loading ? 'Loading event…' : 'Event details')}
			</h2>
			{#if details}
				<span class="mt-2 inline-flex rounded px-2 py-1 text-[10px] font-semibold {details.eventType === 'MajorEvent' ? 'bg-indigo-50 text-indigo-700' : 'bg-amber-50 text-amber-700'}">
					{details.eventType === 'MajorEvent' ? 'Major event' : 'Incident'}
				</span>
			{/if}
		</div>
		<button
			type="button"
			onclick={onclose}
			class="flex h-9 w-9 shrink-0 items-center justify-center rounded-md text-xl text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
			aria-label="Close event details"
		>
			&times;
		</button>
	</header>

	<div class="min-h-0 flex-1 overflow-y-auto">
		{#if loading}
			<div class="space-y-6 p-5 sm:p-6" aria-label="Loading event details">
				{#each [1, 2, 3, 4] as section}
					<div>
						<div class="h-3 w-24 animate-pulse rounded bg-slate-100"></div>
						<div class="mt-3 h-16 animate-pulse rounded-lg bg-slate-50" style="width:{92 - section * 3}%"></div>
					</div>
				{/each}
			</div>
		{:else if error}
			<div class="m-5 rounded-lg border border-red-200 bg-red-50 p-4 sm:m-6" role="alert">
				<p class="text-sm font-semibold text-red-800">Event details are unavailable</p>
				<p class="mt-1 text-xs leading-5 text-red-600">{error}</p>
				<button type="button" onclick={() => (retryToken += 1)} class="mt-3 text-xs font-semibold text-red-700 underline underline-offset-2">Try again</button>
			</div>
		{:else if details}
			<div class="divide-y divide-slate-100">
				<section class="px-5 py-5 sm:px-6">
					<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Overview</h3>
					<div class="mt-3 grid grid-cols-2 gap-3">
						<div class="rounded-lg border border-slate-200 bg-slate-50/60 p-3">
							<p class="text-[9px] font-semibold uppercase text-slate-400">Start date</p>
							<p class="mt-1 text-xs font-medium text-slate-700">{formatDate(details.startDate)}</p>
						</div>
						<div class="rounded-lg border border-slate-200 bg-slate-50/60 p-3">
							<p class="text-[9px] font-semibold uppercase text-slate-400">End date</p>
							<p class="mt-1 text-xs font-medium text-slate-700">{formatDate(details.endDate)}</p>
						</div>
					</div>
					<p class="mt-3 break-all font-mono text-[9px] leading-4 text-slate-400">{details.event}</p>
				</section>

				<section class="px-5 py-5 sm:px-6">
					<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Remarks</h3>
					{#if details.remarks.length}
						<div class="mt-3 space-y-2">
							{#each details.remarks as remark, index (`${remark}-${index}`)}
								<p class="whitespace-pre-wrap break-words rounded-lg border border-slate-200 bg-slate-50/60 p-3 text-xs leading-5 text-slate-600">{remark}</p>
							{/each}
						</div>
					{:else}
						<p class="mt-2 text-xs text-slate-400">No remarks were recorded.</p>
					{/if}
				</section>

				<section class="px-5 py-5 sm:px-6">
					<button
						type="button"
						onclick={() => (locationsExpanded = !locationsExpanded)}
						aria-expanded={locationsExpanded}
						class="flex w-full items-center justify-between gap-3 text-left"
					>
						<span class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Locations affected <span class="normal-case">({details.locations.length})</span></span>
						<span aria-hidden="true" class="text-sm text-slate-400 transition-transform {locationsExpanded ? 'rotate-180' : ''}">⌄</span>
					</button>
					{#if locationsExpanded}
						{#if details.locations.length}
							<div class="mt-3 flex flex-wrap gap-1.5">
								{#each details.locations as location (location.uri)}
									<span class="rounded-md border border-slate-200 bg-white px-2 py-1 text-[11px] text-slate-600" title={location.id}>{location.label}</span>
								{/each}
							</div>
						{:else}
							<p class="mt-2 text-xs text-slate-400">No locations were recorded.</p>
						{/if}
					{/if}
				</section>

				<section class="px-5 py-5 sm:px-6">
					<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Disaster type</h3>
					{#if details.disasterTypes.length}
						<div class="mt-3 flex flex-wrap gap-1.5">
							{#each details.disasterTypes as disasterType (disasterType.uri)}
								<span class="rounded-md bg-sky-50 px-2 py-1 text-[11px] font-medium text-sky-700" title={disasterType.id}>{disasterType.label}</span>
							{/each}
						</div>
					{:else}
						<p class="mt-2 text-xs text-slate-400">No disaster type was recorded.</p>
					{/if}
				</section>

				{#if details.eventType === 'Incident'}
					<section class="px-5 py-5 sm:px-6">
						<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Major event derived from</h3>
						{#if details.majorEvents.length}
							<div class="mt-3 space-y-2">
								{#each details.majorEvents as related (related.uri)}
									<div class="rounded-lg border border-slate-200 p-3">
										<p class="text-xs font-medium text-slate-700">{related.name}</p>
										<p class="mt-1 text-[10px] text-slate-400">{formatDate(related.startDate)}</p>
									</div>
								{/each}
							</div>
						{:else}
							<p class="mt-2 text-xs text-slate-400">No related major event was recorded.</p>
						{/if}
					</section>
				{:else}
					<section class="px-5 py-5 sm:px-6">
						<button
							type="button"
							onclick={() => (incidentsExpanded = !incidentsExpanded)}
							aria-expanded={incidentsExpanded}
							class="flex w-full items-center justify-between gap-3 text-left"
						>
							<span class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Derived incidents <span class="normal-case">({details.incidents.length})</span></span>
							<span aria-hidden="true" class="text-sm text-slate-400 transition-transform {incidentsExpanded ? 'rotate-180' : ''}">⌄</span>
						</button>
						{#if incidentsExpanded}
							{#if details.incidents.length}
								<div class="mt-3 space-y-2">
									{#each details.incidents as related (related.uri)}
										<div class="rounded-lg border border-slate-200 p-3">
											<p class="line-clamp-3 text-xs font-medium leading-5 text-slate-700">{related.name}</p>
											<p class="mt-1 text-[10px] text-slate-400">{formatDate(related.startDate)}</p>
										</div>
									{/each}
								</div>
							{:else}
								<p class="mt-2 text-xs text-slate-400">No derived incidents were recorded.</p>
							{/if}
						{/if}
					</section>
				{/if}

				<section class="px-5 py-5 sm:px-6">
					<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Sources</h3>
					{#if details.sources.length}
						<div class="mt-3 space-y-3">
							{#each details.sources as source (source.uri)}
								{@const reportLink = safeReportLink(source.reportLink)}
								<article class="rounded-lg border border-slate-200 bg-slate-50/40 p-3.5">
									<p class="break-words text-xs font-semibold leading-5 text-slate-700">{source.reportName}</p>
									{#if source.attributedTo.length}
										<p class="mt-1 text-[10px] leading-4 text-slate-500">Source: {source.attributedTo.map((item) => item.label).join(', ')}</p>
									{/if}
									<dl class="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-[10px]">
										<div><dt class="text-slate-400">Obtained</dt><dd class="mt-0.5 text-slate-600">{formatDate(source.obtainedDate)}</dd></div>
										<div><dt class="text-slate-400">Last updated</dt><dd class="mt-0.5 text-slate-600">{formatDate(source.lastUpdateDate)}</dd></div>
										{#if source.format}<div><dt class="text-slate-400">Format</dt><dd class="mt-0.5 uppercase text-slate-600">{source.format}</dd></div>{/if}
									</dl>
									{#if reportLink}
										<a href={reportLink} target="_blank" rel="noreferrer" class="mt-3 inline-flex text-[11px] font-semibold text-indigo-600 hover:text-indigo-800">Open source report ↗</a>
									{/if}
								</article>
							{/each}
						</div>
					{:else}
						<p class="mt-2 text-xs text-slate-400">No source record was linked to this event.</p>
					{/if}
				</section>

				<section class="px-5 py-5 sm:px-6">
					<h3 class="text-[10px] font-semibold uppercase text-slate-400" style="letter-spacing:0.1em;">Alternate events</h3>
					{#if details.alternates.length}
						<div class="mt-3 space-y-2">
							{#each details.alternates as alternate (alternate.uri)}
								<div class="rounded-lg border border-violet-100 bg-violet-50/50 p-3">
									<div class="flex items-start justify-between gap-3">
										<p class="text-xs font-medium leading-5 text-slate-700">{alternate.name}</p>
										{#if alternate.eventType}<span class="shrink-0 text-[9px] font-semibold text-violet-600">{alternate.eventType === 'MajorEvent' ? 'Major' : 'Incident'}</span>{/if}
									</div>
									<p class="mt-1 text-[10px] text-slate-400">{formatDate(alternate.startDate)}</p>
								</div>
							{/each}
						</div>
					{:else}
						<p class="mt-2 text-xs text-slate-400">No alternate events were recorded.</p>
					{/if}
				</section>
			</div>
		{/if}
	</div>
</div>
