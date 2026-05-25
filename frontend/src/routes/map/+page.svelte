<script>
	import NodeCanvas from '$lib/components/NodeCanvas.svelte';
</script>

<svelte:head>
	<title>Map · SakunaGraPH</title>
</svelte:head>

<NodeCanvas />

<main
	class="relative flex min-h-[calc(100vh-52px)] flex-col items-center justify-center px-4"
	style="z-index:1;"
>
	<div
		class="flex flex-col items-center text-center"
	>
		<!-- Icon -->
		<div
			class="mb-6 flex h-16 w-16 items-center justify-center rounded-2xl border border-slate-200/80 bg-white/70 shadow-lg shadow-slate-200/60"
			style="backdrop-filter:blur(12px);"
		>
			<svg
				xmlns="http://www.w3.org/2000/svg"
				width="30"
				height="30"
				viewBox="0 0 24 24"
				fill="none"
				stroke="#6366f1"
				stroke-width="1.6"
				stroke-linecap="round"
				stroke-linejoin="round"
			>
				<polygon points="3 6 9 3 15 6 21 3 21 18 15 21 9 18 3 21" />
				<line x1="9" y1="3" x2="9" y2="18" />
				<line x1="15" y1="6" x2="15" y2="21" />
			</svg>
		</div>

		<h1
			style="font-family:'Playfair Display',Georgia,serif; font-weight:900; font-size:clamp(2.5rem,6vw,4.5rem); line-height:1.05; letter-spacing:-0.02em; color:#1e293b;"
		>
			Map
		</h1>

		<p
			class="mt-3 text-xs font-semibold uppercase text-slate-400"
			style="letter-spacing:0.18em;"
		>
			Interactive Philippine Disaster Map
		</p>

		<div
			class="mt-8 flex items-center gap-2 rounded-full border border-slate-200/80 bg-white/70 px-5 py-2.5 shadow-sm"
			style="backdrop-filter:blur(10px);"
		>
			<span class="h-2 w-2 animate-pulse rounded-full bg-indigo-400"></span>
			<span class="text-sm text-slate-500">Coming soon</span>
		</div>
	</div>
</main>
