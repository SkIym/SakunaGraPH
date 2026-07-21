<script>
	import { manageDrawerFocus } from '../../actions/focus.js';

	let {
		datePrefix = '',
		items = [],
		loading = false,
		error = '',
		onclose = () => {},
		onselect = () => {},
	} = $props();
	const formatter = new Intl.DateTimeFormat('en-PH', {
		year: 'numeric',
		month: 'long',
		day: 'numeric',
	});
	function formatDate(value) {
		if (!value) return 'No date recorded';
		const parsed = new Date(`${value.slice(0, 10)}T00:00:00`);
		return Number.isNaN(parsed.valueOf()) ? value : formatter.format(parsed);
	}
</script>

<aside
	use:manageDrawerFocus
	class="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm"
	aria-label="Events for selected date"
>
	<header class="flex items-start justify-between gap-3 border-b border-slate-100 px-4 py-3.5">
		<div>
			<p class="text-[10px] font-semibold uppercase text-indigo-600" style="letter-spacing:0.1em;">
				Date events
			</p>
			<h2 class="mt-1 text-sm font-semibold text-slate-800">{datePrefix}</h2>
		</div>
		<button
			type="button"
			data-focus-first
			onclick={onclose}
			class="rounded p-1 text-lg leading-none text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
			aria-label="Close date events">&times;</button
		>
	</header>
	<div class="max-h-[470px] overflow-y-auto p-3">
		{#if loading}
			<div class="space-y-2">
				{#each [1, 2, 3]}<div class="h-16 animate-pulse rounded-lg bg-slate-50"></div>{/each}
			</div>
		{:else if error}
			<p class="rounded-md bg-red-50 p-3 text-xs text-red-600">{error}</p>
		{:else if items.length}
			<div class="space-y-2">
				{#each items as item (item.event)}
					<button
						type="button"
						onclick={() => onselect(item.event)}
						class="w-full rounded-lg border border-slate-200 p-3 text-left transition hover:border-indigo-300 hover:bg-indigo-50/40 focus:outline-none focus:ring-2 focus:ring-indigo-400"
					>
						<div class="flex items-start justify-between gap-3">
							<p class="line-clamp-2 text-xs font-semibold leading-5 text-slate-700">
								{item.eventName}
							</p>
							<span
								class="shrink-0 rounded px-1.5 py-0.5 text-[9px] font-semibold {item.eventType ===
								'MajorEvent'
									? 'bg-indigo-50 text-indigo-700'
									: 'bg-amber-50 text-amber-700'}"
								>{item.eventType === 'MajorEvent' ? 'Major' : 'Incident'}</span
							>
						</div>
						<p class="mt-1 text-[10px] text-slate-400">{formatDate(item.startDate)}</p>
						{#if item.disasterTypes.length}<p class="mt-2 truncate text-[10px] text-slate-500">
								{item.disasterTypes.map((type) => type.label).join(', ')}
							</p>{/if}
					</button>
				{/each}
			</div>
		{:else}
			<p class="py-8 text-center text-xs text-slate-400">
				No events start in this selected period.
			</p>
		{/if}
	</div>
</aside>
