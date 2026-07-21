<script>
	let { citations = [], retrieval = null } = $props();

	const modeLabels = {
		graphrag: 'GraphRAG',
		legacy: 'Legacy SPARQL',
		fallback: 'Fallback',
	};

	function citationHref(uri) {
		try {
			const parsed = new URL(uri);
			return ['http:', 'https:'].includes(parsed.protocol) ? parsed.href : null;
		} catch {
			return null;
		}
	}
</script>

{#if retrieval?.mode && modeLabels[retrieval.mode]}
	<div class="border-t border-slate-100 px-5 py-2.5 text-[11px] text-slate-500">
		<span class="font-semibold tracking-wider text-slate-600 uppercase">
			{modeLabels[retrieval.mode]}
		</span>
		{#if retrieval.sourceCount !== undefined}
			<span> · {retrieval.sourceCount} source{retrieval.sourceCount === 1 ? '' : 's'}</span>
		{/if}
		{#if retrieval.indexVersion}
			<span class="ml-2 text-slate-400">Index {retrieval.indexVersion}</span>
		{/if}
	</div>
{/if}

{#if citations?.length}
	<section class="border-t border-slate-100 px-5 py-3" aria-label="Answer sources">
		<h2 class="mb-2 text-[11px] font-semibold tracking-widest text-slate-500 uppercase">Sources</h2>
		<ol class="space-y-2">
			{#each citations as citation, index (citation.id)}
				<li class="text-xs leading-relaxed text-slate-600">
					<div class="flex items-start gap-2">
						<span class="mt-0.5 text-[10px] font-semibold text-slate-400">{index + 1}</span>
						<div class="min-w-0">
							{#if citationHref(citation.uri)}
								<a
									href={citationHref(citation.uri)}
									target="_blank"
									rel="noreferrer"
									class="font-semibold text-slate-700 underline decoration-slate-300 underline-offset-2 hover:text-slate-900"
								>
									{citation.label}
								</a>
							{:else}
								<span class="font-semibold text-slate-700">{citation.label}</span>
							{/if}
							{#if citation.excerpt}
								<p class="mt-0.5 text-slate-500">{citation.excerpt}</p>
							{/if}
							{#if citation.sourceRecord}
								<p class="mt-0.5 truncate font-mono text-[10px] text-slate-400">
									{citation.sourceRecord}
								</p>
							{/if}
						</div>
					</div>
				</li>
			{/each}
		</ol>
	</section>
{/if}
