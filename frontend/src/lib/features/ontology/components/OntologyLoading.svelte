<script>
	let { label } = $props();
</script>

<div class="absolute inset-0 z-10 flex items-center justify-center">
	<div class="text-center">
		<svg
			class="mx-auto mb-3 animate-spin text-slate-400"
			viewBox="0 0 24 24"
			width="24"
			height="24"
			fill="none"
			stroke="currentColor"
			stroke-width="2"
			aria-hidden="true"
		>
			<path d="M21 12a9 9 0 1 1-6.219-8.56" />
		</svg>
		<p class="text-sm text-slate-400">{label}</p>
	</div>
</div>
