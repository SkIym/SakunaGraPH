<script>
	import AskAnswerMeta from './AskAnswerMeta.svelte';

	let {
		messages = [],
		suggestions = [],
		announcement = '',
		onSend = () => {},
		bottomElement = $bindable(null),
	} = $props();
</script>

<div class="flex-1 overflow-y-auto px-4 py-6" id="messages-scroll">
	<p class="sr-only" role="status" aria-live="polite" aria-atomic="true">{announcement}</p>
	<div class="mx-auto flex w-full max-w-3xl flex-col gap-6">
		{#if messages.length === 0}
			<div
				class="flex flex-col items-center justify-center"
				style="min-height:calc(100vh - 220px);"
			>
				<div
					class="mb-2 flex h-12 w-12 items-center justify-center rounded-2xl bg-slate-800 shadow-lg"
				>
					<svg
						xmlns="http://www.w3.org/2000/svg"
						width="22"
						height="22"
						viewBox="0 0 24 24"
						fill="none"
						stroke="white"
						stroke-width="2"
						stroke-linecap="round"
						stroke-linejoin="round"
					>
						<circle cx="12" cy="12" r="10" />
						<path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3" />
						<path d="M12 17h.01" />
					</svg>
				</div>
				<h1
					class="mb-1 text-2xl font-black text-slate-800"
					style="font-family:'Playfair Display',Georgia,serif;"
				>
					Ask SakunaGraPH
				</h1>
				<p class="mb-8 text-sm text-slate-400">Query Philippine disaster data in plain language.</p>
				<div class="grid w-full max-w-xl grid-cols-1 gap-2 sm:grid-cols-2">
					{#each suggestions as suggestion}
						<button
							type="button"
							onclick={() => onSend(suggestion)}
							class="rounded-xl border border-slate-200 bg-white/80 px-4 py-3 text-left text-sm text-slate-600 transition-all hover:border-slate-300 hover:bg-white hover:shadow-sm"
							style="backdrop-filter:blur(8px);">{suggestion}</button
						>
					{/each}
				</div>
			</div>
		{:else}
			{#each messages as message}
				{#if message.role === 'user'}
					<div class="flex justify-end">
						<div
							class="max-w-[75%] rounded-2xl rounded-br-sm bg-slate-800 px-5 py-3 text-sm leading-relaxed text-white shadow-sm"
						>
							{message.text}
						</div>
					</div>
				{:else}
					<div class="flex justify-start">
						<div class="w-full max-w-[90%]">
							<div
								class="overflow-hidden rounded-2xl rounded-bl-sm border border-slate-200/70 bg-white/92 shadow-sm"
								style="backdrop-filter:blur(12px);"
							>
								{#if message.loading}
									<div class="flex items-center gap-2 px-5 py-4">
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
								{:else if message.error}
									<div class="px-5 py-4 text-sm text-red-600">{message.error}</div>
								{:else}
									{#if message.text}
										<div
											class="whitespace-pre-wrap px-5 pt-4 pb-3 text-sm leading-relaxed text-slate-700"
										>
											{message.text}
										</div>
									{/if}

									{#if message.streaming}
										<div class="flex items-center gap-2 px-5 py-3 text-xs text-slate-400">
											<span>Generating response…</span>
											<span class="h-1.5 w-1.5 animate-pulse rounded-full bg-slate-400"></span>
										</div>
									{:else if message.cancelled}
										<div class="px-5 py-3 text-xs text-slate-400">Response cancelled.</div>
									{/if}

									<AskAnswerMeta citations={message.citations} retrieval={message.retrieval} />

									{#if message.sparql}
										<details class="border-t border-slate-100">
											<summary
												class="flex cursor-pointer list-none items-center gap-1.5 px-5 py-2.5 text-[11px] font-semibold tracking-widest text-slate-400 uppercase transition-colors select-none hover:text-slate-600"
											>
												<svg
													viewBox="0 0 24 24"
													width="10"
													height="10"
													fill="none"
													stroke="currentColor"
													stroke-width="2.5"
													class="chevron"><path d="m9 18 6-6-6-6" /></svg
												>
												SPARQL Query
											</summary>
											<pre
												class="overflow-x-auto px-5 pb-4 font-mono text-[11px] leading-relaxed whitespace-pre text-slate-500">{message.sparql}</pre>
										</details>
									{/if}

									{#if message.rows?.length > 0}
										{@const columns = Object.keys(message.rows[0])}
										<details class="border-t border-slate-100">
											<summary
												class="flex cursor-pointer list-none items-center gap-1.5 px-5 py-2.5 text-[11px] font-semibold tracking-widest text-slate-400 uppercase transition-colors select-none hover:text-slate-600"
											>
												<svg
													viewBox="0 0 24 24"
													width="10"
													height="10"
													fill="none"
													stroke="currentColor"
													stroke-width="2.5"
													class="chevron"><path d="m9 18 6-6-6-6" /></svg
												>
												Results · {message.rows.length} row{message.rows.length === 1 ? '' : 's'}
											</summary>
											<div class="overflow-x-auto px-5 pb-4">
												<table class="w-full border-collapse text-xs">
													<thead>
														<tr>
															{#each columns as column}
																<th
																	class="border-b border-slate-100 py-1.5 pr-4 text-left font-semibold tracking-wider whitespace-nowrap text-slate-500 uppercase"
																	>{column}</th
																>
															{/each}
														</tr>
													</thead>
													<tbody>
														{#each message.rows as row, index}
															<tr class={index % 2 === 1 ? 'bg-slate-50/60' : ''}>
																{#each columns as column}
																	<td
																		class="max-w-[200px] truncate py-1.5 pr-4 text-slate-600"
																		title={row[column] ?? ''}>{row[column] ?? '—'}</td
																	>
																{/each}
															</tr>
														{/each}
													</tbody>
												</table>
											</div>
										</details>
									{:else if message.rows}
										<div class="border-t border-slate-100 px-5 py-2.5 text-[11px] text-slate-400">
											No matching records in the knowledge graph.
										</div>
									{/if}
								{/if}
							</div>
						</div>
					</div>
				{/if}
			{/each}
		{/if}

		<div bind:this={bottomElement}></div>
	</div>
</div>

<style>
	@keyframes ask-dot {
		0%,
		80%,
		100% {
			transform: scale(0.6);
			opacity: 0.4;
		}
		40% {
			transform: scale(1);
			opacity: 1;
		}
	}
	details[open] .chevron {
		transform: rotate(90deg);
	}
	.chevron {
		flex-shrink: 0;
		transition: transform 0.15s ease;
	}
</style>
