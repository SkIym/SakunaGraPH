<a
	href="/map"
	class="map-figure group block min-w-0 overflow-hidden rounded-2xl border border-slate-200 bg-white/90 shadow-[0_20px_55px_-32px_rgba(15,23,42,0.45)] transition-colors hover:border-blue-300"
	aria-label="Open the interactive Philippine disaster map"
>
	<figure>
		<div class="flex items-center justify-between gap-4 border-b border-slate-200 px-4 py-3">
			<div class="min-w-0">
				<p
					class="truncate font-mono text-[0.6875rem] font-semibold uppercase tracking-[0.12em] text-slate-500"
				>
					Map explorer
				</p>
				<p class="mt-0.5 text-sm font-semibold text-slate-800">Philippine disaster records</p>
			</div>
			<span
				class="shrink-0 rounded-full bg-blue-50 px-2.5 py-1 text-xs font-semibold text-blue-800"
			>
				Public view
			</span>
		</div>

		<div class="relative min-h-[23rem] overflow-hidden bg-slate-50/80 sm:min-h-[26rem]">
			<svg
				aria-hidden="true"
				class="absolute inset-0 h-full w-full"
				viewBox="0 0 380 440"
				fill="none"
			>
				<defs>
					<pattern id="home-map-grid" width="28" height="28" patternUnits="userSpaceOnUse">
						<path d="M28 0H0V28" stroke="#cbd5e1" stroke-opacity="0.38" stroke-width="0.7" />
					</pattern>
				</defs>
				<rect width="380" height="440" fill="url(#home-map-grid)" />
				<g fill="#dbeafe" stroke="#305bb2" stroke-linejoin="round" stroke-width="1.6">
					<!-- A deliberately simplified archipelago: this is a navigation preview, not a data map. -->
					<path
						d="M207 48l-18 13-4 22-16 19 7 22-12 22 9 18-4 25 13 15 17-8 7-24 18-15-5-24 9-18-12-14 3-26-12-29Z"
					/>
					<path d="M156 191l-11 8 2 18 12 5 8-13-11-18Z" />
					<path d="M181 211l-10 10 8 14 14-4 2-15-14-5Z" />
					<path d="M211 205l-9 11 8 13 13-7-1-14-11-3Z" />
					<path d="M235 222l-8 9 7 12 13-3 1-12-13-6Z" />
					<path d="M157 235l-8 8 6 14 12 1 6-13-6-9-10-1Z" />
					<path d="M194 248l-16 14-2 23 15 18 28 8 25-10 12-20-14-18-25-5-23-10Z" />
					<path d="M132 220l-7 20-5 25-11 25-1 26 8-8 7-26 10-22 7-25-8-15Z" />
					<path d="M253 259l10 8 4 18-8 14-8-10 4-14-2-16Z" />
					<path d="M184 326l-19 8-12 18 9 19 25 11 30-2 23-18-5-22-25-14h-26Z" />
					<path d="M139 327l-11 5-2 14 9 6 10-9-6-16Z" />
					<path d="M252 333l9 5 1 13-10 7-7-10 7-15Z" />
				</g>

				<g stroke="#b91c1c" stroke-width="1.4">
					<path d="M197 130H85" />
					<path d="M219 278h91" />
				</g>
				<g fill="#b91c1c" stroke="white" stroke-width="3">
					<circle cx="197" cy="130" r="6" />
					<circle cx="219" cy="278" r="6" />
				</g>
			</svg>

			<div
				class="absolute top-[5.5rem] left-4 rounded-lg border border-slate-200 bg-white/95 px-3 py-2 shadow-sm"
			>
				<p class="text-xs font-bold text-slate-800">Choose a place</p>
				<p class="mt-0.5 text-[0.6875rem] text-slate-500">Region or province</p>
			</div>
			<div
				class="absolute right-4 bottom-[6.25rem] max-w-[9rem] rounded-lg border border-slate-200 bg-white/95 px-3 py-2 shadow-sm"
			>
				<p class="text-xs font-bold text-slate-800">Open its records</p>
				<p class="mt-0.5 text-[0.6875rem] leading-4 text-slate-500">
					Dates, impacts, and linked sources
				</p>
			</div>
		</div>

		<figcaption class="flex items-center justify-between gap-4 border-t border-slate-200 px-4 py-3">
			<span class="min-w-0 text-xs leading-5 text-slate-600">
				Illustrative preview—open the live map to select an area.
			</span>
			<svg
				aria-hidden="true"
				class="shrink-0 text-blue-800"
				width="18"
				height="18"
				viewBox="0 0 24 24"
				fill="none"
				stroke="currentColor"
				stroke-width="2"
				stroke-linecap="round"
				stroke-linejoin="round"
			>
				<path d="M5 12h14" /><path d="m13 6 6 6-6 6" />
			</svg>
		</figcaption>
	</figure>
</a>

<style>
	.map-figure:focus-visible {
		outline: 3px solid color-mix(in srgb, var(--color-brand) 45%, transparent);
		outline-offset: 4px;
	}

	@media (forced-colors: active) {
		.map-figure {
			border: 2px solid ButtonText;
		}
	}
</style>
