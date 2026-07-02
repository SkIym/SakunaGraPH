<script>
	import { tick } from 'svelte';
	import { PUBLIC_API_URL } from '$env/static/public';
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';

	let messages = $state([]);
	let input    = $state('');
	let sending  = $state(false);
	let bottomEl = $state(null);
	let textareaEl = $state(null);

	const SUGGESTIONS = [
		'How many flood events were recorded in 2023?',
		'Which region had the most casualties from typhoons?',
		'List the top 5 disaster events by affected population.',
		'What types of disasters occurred in Mindanao?'
	];

	function displayVal(b) {
		if (!b) return '—';
		const v = b.value ?? '';
		if (b.type === 'uri') return v.split(/[/#]/).pop() || v;
		return v;
	}

	async function scrollBottom() {
		await tick();
		bottomEl?.scrollIntoView({ behavior: 'smooth' });
	}

	async function send(query = input.trim()) {
		if (!query || sending) return;
		input   = '';
		sending = true;

		messages = [...messages, { role: 'user', text: query }];
		const idx = messages.length; // index of the assistant slot we're about to add
		messages = [...messages, { role: 'assistant', loading: true }];
		await scrollBottom();

		try {
			const res  = await fetch(`${PUBLIC_API_URL}/ask`, {
				method:  'POST',
				headers: { 'Content-Type': 'application/json' },
				body:    JSON.stringify({ query })
			});
			const json = await res.json();

			messages = messages.map((m, i) =>
				i !== idx ? m :
				res.ok
					? { role: 'assistant', text: json.answer, sparql: json.sparql, bindings: json.bindings ?? [] }
					: { role: 'assistant', error: json.detail ?? 'Request failed.' }
			);
		} catch {
			messages = messages.map((m, i) =>
				i !== idx ? m : { role: 'assistant', error: 'Could not reach server.' }
			);
		} finally {
			sending = false;
			await scrollBottom();
		}
	}

	function handleKeydown(e) {
		if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); send(); }
	}
</script>

<svelte:head>
	<title>Ask · SakunaGraPH</title>
</svelte:head>

<NodeCanvas interactive={false} />

<div class="relative flex flex-col" style="height:calc(100vh - 52px); z-index:1;">

	<!-- ── Messages ──────────────────────────────────────────────────────────── -->
	<div class="flex-1 overflow-y-auto px-4 py-6" id="messages-scroll">
		<div class="mx-auto w-full max-w-3xl flex flex-col gap-6">

			{#if messages.length === 0}
				<!-- Empty state -->
				<div class="flex flex-col items-center justify-center" style="min-height:calc(100vh - 220px);">
					<div class="mb-2 h-12 w-12 rounded-2xl bg-slate-800 flex items-center justify-center shadow-lg">
						<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
							<circle cx="12" cy="12" r="10"/><path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/><path d="M12 17h.01"/>
						</svg>
					</div>
					<h1 class="font-black text-slate-800 text-2xl mb-1" style="font-family:'Playfair Display',Georgia,serif;">Ask SakunaGraPH</h1>
					<p class="text-slate-400 text-sm mb-8">Query Philippine disaster data in plain language.</p>
					<div class="grid grid-cols-1 sm:grid-cols-2 gap-2 w-full max-w-xl">
						{#each SUGGESTIONS as s}
							<button
								onclick={() => send(s)}
								class="rounded-xl border border-slate-200 bg-white/80 px-4 py-3 text-left text-sm text-slate-600 hover:border-slate-300 hover:bg-white hover:shadow-sm transition-all"
								style="backdrop-filter:blur(8px);"
							>{s}</button>
						{/each}
					</div>
				</div>

			{:else}
				{#each messages as msg}
					{#if msg.role === 'user'}
						<!-- User bubble -->
						<div class="flex justify-end">
							<div class="max-w-[75%] rounded-2xl rounded-br-sm bg-slate-800 px-5 py-3 text-sm text-white shadow-sm leading-relaxed">
								{msg.text}
							</div>
						</div>

					{:else}
						<!-- Assistant card -->
						<div class="flex justify-start">
							<div class="max-w-[90%] w-full">
								<div class="rounded-2xl rounded-bl-sm border border-slate-200/70 bg-white/92 shadow-sm overflow-hidden" style="backdrop-filter:blur(12px);">

									{#if msg.loading}
										<!-- Typing indicator -->
										<div class="px-5 py-4 flex items-center gap-2">
											<span class="text-xs text-slate-400">Querying knowledge graph…</span>
											<div class="flex gap-1">
												{#each [0, 150, 300] as delay}
													<span
														class="block h-1.5 w-1.5 rounded-full bg-slate-400"
														style="animation: ask-dot 1.2s ease-in-out {delay}ms infinite;"
													></span>
												{/each}
											</div>
										</div>

									{:else if msg.error}
										<div class="px-5 py-4 text-sm text-red-600">{msg.error}</div>

									{:else}
										<!-- Answer -->
										<div class="px-5 pt-4 pb-3 text-sm text-slate-700 leading-relaxed whitespace-pre-wrap">{msg.text}</div>

										<!-- SPARQL -->
										{#if msg.sparql}
											<details class="border-t border-slate-100">
												<summary class="cursor-pointer select-none px-5 py-2.5 text-[11px] font-semibold uppercase tracking-widest text-slate-400 hover:text-slate-600 transition-colors list-none flex items-center gap-1.5">
													<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="chevron"><path d="m9 18 6-6-6-6"/></svg>
													SPARQL Query
												</summary>
												<pre class="px-5 pb-4 text-[11px] font-mono text-slate-500 overflow-x-auto whitespace-pre leading-relaxed">{msg.sparql}</pre>
											</details>
										{/if}

										<!-- Results table -->
										{#if msg.bindings?.length > 0}
											{@const cols = Object.keys(msg.bindings[0])}
											<details class="border-t border-slate-100">
												<summary class="cursor-pointer select-none px-5 py-2.5 text-[11px] font-semibold uppercase tracking-widest text-slate-400 hover:text-slate-600 transition-colors list-none flex items-center gap-1.5">
													<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="chevron"><path d="m9 18 6-6-6-6"/></svg>
													Results · {msg.bindings.length} row{msg.bindings.length === 1 ? '' : 's'}
												</summary>
												<div class="px-5 pb-4 overflow-x-auto">
													<table class="w-full text-xs border-collapse">
														<thead>
															<tr>
																{#each cols as col}
																	<th class="py-1.5 pr-4 text-left font-semibold text-slate-500 uppercase tracking-wider whitespace-nowrap border-b border-slate-100">{col}</th>
																{/each}
															</tr>
														</thead>
														<tbody>
															{#each msg.bindings as row, i}
																<tr class="{i % 2 === 1 ? 'bg-slate-50/60' : ''}">
																	{#each cols as col}
																		<td class="py-1.5 pr-4 text-slate-600 max-w-[200px] truncate" title={row[col]?.value ?? ''}>
																			{displayVal(row[col])}
																		</td>
																	{/each}
																</tr>
															{/each}
														</tbody>
													</table>
												</div>
											</details>
										{:else if msg.bindings}
											<div class="border-t border-slate-100 px-5 py-2.5 text-[11px] text-slate-400">No matching records in the knowledge graph.</div>
										{/if}
									{/if}
								</div>
							</div>
						</div>
					{/if}
				{/each}
			{/if}

			<div bind:this={bottomEl}></div>
		</div>
	</div>

	<!-- ── Input bar ─────────────────────────────────────────────────────────── -->
	<div class="flex-shrink-0 border-t border-slate-200/80 bg-white/88 px-4 py-3" style="backdrop-filter:blur(12px);">
		<div class="mx-auto flex w-full max-w-3xl items-end gap-3">
			<textarea
				bind:this={textareaEl}
				bind:value={input}
				onkeydown={handleKeydown}
				rows="1"
				placeholder="Ask a question about Philippine disaster data…"
				disabled={sending}
				class="flex-1 resize-none rounded-xl border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-800 placeholder-slate-400 shadow-sm outline-none focus:border-slate-400 focus:ring-2 focus:ring-slate-200 disabled:opacity-50 transition-colors leading-relaxed"
				style="max-height:140px; overflow-y:auto; field-sizing:content;"
			></textarea>
			<button
				onclick={() => send()}
				disabled={sending || !input.trim()}
				class="flex-shrink-0 rounded-xl bg-slate-800 px-4 py-2.5 text-sm font-semibold text-white shadow-sm hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed transition-all"
			>
				{#if sending}
					<svg class="animate-spin" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
						<path d="M21 12a9 9 0 1 1-6.219-8.56"/>
					</svg>
				{:else}
					Send
				{/if}
			</button>
		</div>
		<p class="mx-auto mt-1.5 max-w-3xl text-[10px] text-slate-400">Enter to send · Shift+Enter for new line</p>
	</div>
</div>

<style>
	@keyframes ask-dot {
		0%, 80%, 100% { transform: scale(0.6); opacity: 0.4; }
		40%            { transform: scale(1);   opacity: 1;   }
	}
	details[open] .chevron {
		transform: rotate(90deg);
	}
	.chevron {
		transition: transform 0.15s ease;
		flex-shrink: 0;
	}
</style>
