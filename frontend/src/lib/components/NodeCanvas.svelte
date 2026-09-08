<script>
	let { active = false } = $props();
</script>

<svg
	aria-hidden="true"
	class:query-active={active}
	class="schema-field"
	fill="none"
	focusable="false"
	pointer-events="none"
	viewBox="0 0 1440 900"
	preserveAspectRatio="xMidYMid slice"
>
	<g class="schema-cluster">
		<path fill="none" d="M78 132 188 84 276 158 372 104" />
		<path fill="none" d="M188 84 214 218 276 158 328 244" />
		<circle cx="78" cy="132" r="5" />
		<circle cx="188" cy="84" r="9" />
		<circle cx="214" cy="218" r="5" />
		<circle cx="276" cy="158" r="7" />
		<circle cx="328" cy="244" r="5" />
		<circle cx="372" cy="104" r="4" />
	</g>

	<g class="schema-cluster">
		<path fill="none" d="M1112 118 1218 86 1338 154 1284 278 1172 236 1112 118" />
		<path fill="none" d="M1218 86 1172 236 1284 278 1364 344" />
		<circle cx="1112" cy="118" r="5" />
		<circle cx="1218" cy="86" r="8" />
		<circle cx="1338" cy="154" r="5" />
		<circle cx="1284" cy="278" r="8" />
		<circle cx="1172" cy="236" r="5" />
		<circle cx="1364" cy="344" r="4" />
	</g>

	<g class="schema-cluster">
		<path fill="none" d="M122 716 246 648 354 734 474 666" />
		<path fill="none" d="M246 648 288 824 354 734 438 816" />
		<circle cx="122" cy="716" r="4" />
		<circle cx="246" cy="648" r="8" />
		<circle cx="288" cy="824" r="5" />
		<circle cx="354" cy="734" r="7" />
		<circle cx="438" cy="816" r="5" />
		<circle cx="474" cy="666" r="4" />
	</g>

	<g class="schema-cluster">
		<path fill="none" d="M1032 706 1140 642 1248 734 1360 662" />
		<path fill="none" d="M1140 642 1196 838 1248 734 1328 820" />
		<circle cx="1032" cy="706" r="4" />
		<circle cx="1140" cy="642" r="8" />
		<circle cx="1196" cy="838" r="5" />
		<circle cx="1248" cy="734" r="7" />
		<circle cx="1328" cy="820" r="5" />
		<circle cx="1360" cy="662" r="4" />
	</g>

	<g class="schema-spine">
		<path fill="none" d="M520 448 H650 L704 394 H790 L842 448 H930" />
		<rect x="507" y="435" width="26" height="26" rx="5" />
		<circle cx="650" cy="448" r="5" />
		<circle cx="704" cy="394" r="7" />
		<circle cx="790" cy="394" r="5" />
		<circle cx="842" cy="448" r="7" />
		<rect x="927" y="435" width="26" height="26" rx="5" />
	</g>

	<path class="query-trace" pathLength="100" d="M520 448 H650 L704 394 H790 L842 448 H940" />
</svg>

<style>
	.schema-field {
		position: fixed;
		inset: 0;
		z-index: 0;
		width: 100vw;
		max-width: 100vw;
		height: 100dvh;
		overflow: hidden;
		pointer-events: none;
	}

	.schema-cluster,
	.schema-spine {
		fill: none;
		stroke: var(--color-brand, #305bb2);
		stroke-linecap: round;
		stroke-linejoin: round;
		stroke-width: 1;
		vector-effect: non-scaling-stroke;
	}

	.schema-cluster {
		opacity: 0.13;
	}

	.schema-spine {
		opacity: 0.08;
	}

	.schema-cluster circle,
	.schema-spine circle,
	.schema-spine rect {
		fill: var(--color-canvas, #ffffff);
	}

	.schema-cluster circle:nth-of-type(2),
	.schema-cluster circle:nth-of-type(4),
	.schema-spine circle:nth-of-type(2),
	.schema-spine circle:nth-of-type(4) {
		fill: var(--color-brand, #305bb2);
		fill-opacity: 0.24;
	}

	.query-trace {
		fill: none;
		opacity: 0;
		stroke: var(--color-brand, #305bb2);
		stroke-dasharray: 18 82;
		stroke-linecap: round;
		stroke-width: 2;
		vector-effect: non-scaling-stroke;
	}

	.query-active .query-trace {
		animation: schema-trace 900ms cubic-bezier(0.16, 1, 0.3, 1) both;
	}

	@keyframes schema-trace {
		0% {
			opacity: 0;
			stroke-dashoffset: 22;
		}
		18% {
			opacity: 0.58;
		}
		100% {
			opacity: 0;
			stroke-dashoffset: -82;
		}
	}

	@media (max-width: 639px) {
		.schema-field {
			display: none;
		}
	}

	@media (prefers-reduced-motion: reduce) {
		.query-active .query-trace {
			animation: none;
			opacity: 0.22;
		}
	}

	@media (forced-colors: active) {
		.schema-field {
			display: none;
		}
	}
</style>
