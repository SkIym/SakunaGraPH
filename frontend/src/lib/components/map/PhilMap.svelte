<script>
	/**
	 * Pure SVG renderer for the Philippine map.
	 * All data loading and projection happen in the parent (map page).
	 */

	let {
		pathData = [], // [{d, gid, name, regionPsgc, feature}]
		viewBox = '0 0 700 800',
		view = 'regions', // 'regions' | 'provinces' | 'cities'
		selected = null, // {psgc?, id?, name}
		interactive = true,
		strokeWidth = 0.7,
		strokeColor = '#374151',
		hoverFill = 'var(--color-brand-medium)',
		selectedFill = 'var(--color-accent)',
		ariaLabel = 'Map of Philippine regions and provinces',
		getAreaLabel = (item) => `Select ${item.name}`,
		getAreaExpanded = () => undefined,
		getAreaControls = () => undefined,
		getAreaHitStrokeWidth = () => 0,
		colorMap = {}, // groupKey → default fill color (for region pastels)
		onselect = () => {},
		onhover = () => {}, // onhover(item | null, clientX, clientY)
	} = $props();

	let hoveredKey = $state(null);
	let focusedKey = $state(null);

	function groupKey(item) {
		return view === 'regions' ? item.regionPsgc : item.gid;
	}

	function selectedKey() {
		if (!selected) return null;
		return view === 'regions' ? selected.psgc : selected.id;
	}

	function getFill(item) {
		const gk = groupKey(item);
		const sk = selectedKey();
		if (sk && gk === sk) return selectedFill;
		if (interactive && (hoveredKey === gk || focusedKey === gk)) return hoverFill;
		return colorMap[gk] ?? 'var(--color-canvas)';
	}

	function isSelected(item) {
		const sk = selectedKey();
		return Boolean(sk && groupKey(item) === sk);
	}

	function isCurrent(item) {
		const key = groupKey(item);
		return interactive && (hoveredKey === key || focusedKey === key);
	}

	function isMuted(item) {
		const hasEmphasis = Boolean(selectedKey() || hoveredKey || focusedKey);
		return hasEmphasis && !isSelected(item) && !isCurrent(item);
	}

	function handleEnter(item, e) {
		if (!interactive) return;
		hoveredKey = groupKey(item);
		onhover(item, e.clientX, e.clientY);
	}

	function handleLeave() {
		hoveredKey = null;
		if (!focusedKey) onhover(null, 0, 0);
	}

	function handleFocus(item, event) {
		if (!interactive) return;
		focusedKey = groupKey(item);
		const bounds = event.currentTarget.getBoundingClientRect();
		onhover(item, bounds.right, bounds.top + bounds.height / 2);
	}

	function handleBlur() {
		focusedKey = null;
		if (!hoveredKey) onhover(null, 0, 0);
	}

	function handleClick(item) {
		if (!interactive) return;
		onselect(item);
	}

	function handleKeydown(item, event) {
		if (!interactive || !['Enter', ' '].includes(event.key)) return;
		event.preventDefault();
		onselect(item);
	}
</script>

<svg
	{viewBox}
	class="phil-map h-full w-full"
	preserveAspectRatio="xMidYMid meet"
	role={interactive ? 'group' : 'img'}
	aria-label={ariaLabel}
>
	<g class="map-emphasis-layer" aria-hidden="true">
		{#each pathData as item (`emphasis-${item.gid}`)}
			{#if isSelected(item) || isCurrent(item)}
				<path
					d={item.d}
					class:selected-halo={isSelected(item)}
					class:current-halo={!isSelected(item) && isCurrent(item)}
					class="map-halo"
					vector-effect="non-scaling-stroke"
				/>
			{/if}
		{/each}
	</g>

	<g class="map-geometry-layer">
		{#each pathData as item (item.gid)}
			{#if interactive}
				{#if getAreaHitStrokeWidth(item) > 0}
					<path
						d={item.d}
						class="map-hit-area"
						fill="transparent"
						stroke="transparent"
						stroke-width={getAreaHitStrokeWidth(item)}
						vector-effect="non-scaling-stroke"
						pointer-events="stroke"
						aria-hidden="true"
						onmouseenter={(event) => handleEnter(item, event)}
						onmouseleave={handleLeave}
						onclick={() => handleClick(item)}
					/>
				{/if}
				<path
					d={item.d}
					fill={getFill(item)}
					stroke={strokeColor}
					stroke-width={strokeWidth}
					stroke-linecap="round"
					stroke-linejoin="round"
					vector-effect="non-scaling-stroke"
					class:is-current={isCurrent(item)}
					class:is-selected={isSelected(item)}
					class:is-muted={isMuted(item)}
					class="map-area cursor-pointer outline-none"
					role="button"
					tabindex="0"
					aria-label={getAreaLabel(item)}
					aria-pressed={isSelected(item)}
					aria-expanded={getAreaExpanded(item)}
					aria-controls={getAreaControls(item)}
					data-area-id={groupKey(item)}
					onmouseenter={(e) => handleEnter(item, e)}
					onmouseleave={handleLeave}
					onfocus={(event) => handleFocus(item, event)}
					onblur={handleBlur}
					onclick={() => handleClick(item)}
					onkeydown={(event) => handleKeydown(item, event)}
				/>
			{:else}
				<path
					d={item.d}
					fill={getFill(item)}
					stroke={strokeColor}
					stroke-width={strokeWidth}
					stroke-linecap="round"
					stroke-linejoin="round"
					vector-effect="non-scaling-stroke"
					class:is-selected={isSelected(item)}
					class:is-muted={isMuted(item)}
					class="map-shape"
				/>
			{/if}
		{/each}
	</g>
</svg>

<style>
	.phil-map {
		overflow: visible;
		isolation: isolate;
	}

	.map-area,
	.map-shape {
		transform-box: fill-box;
		transform-origin: center;
		shape-rendering: geometricPrecision;
		transition:
			fill 220ms ease,
			stroke 180ms ease,
			stroke-width 180ms ease,
			opacity 220ms ease,
			filter 220ms ease,
			transform 140ms ease;
	}

	.map-hit-area {
		cursor: pointer;
	}

	.map-area.is-muted,
	.map-shape.is-muted {
		opacity: 0.52;
	}

	.map-area.is-current {
		stroke: var(--color-brand-hover);
		stroke-width: 1.15;
		filter: drop-shadow(0 0.08rem 0.12rem rgb(0 56 168 / 0.2));
	}

	.map-area.is-selected,
	.map-shape.is-selected {
		stroke: var(--color-brand-hover);
		stroke-width: 1.35;
		filter: drop-shadow(0 0.08rem 0.13rem rgb(30 41 59 / 0.2));
	}

	.map-halo {
		fill: none;
		stroke-linecap: round;
		stroke-linejoin: round;
		pointer-events: none;
	}

	.map-halo.current-halo {
		stroke: var(--color-brand-medium);
		stroke-width: 3.8;
		opacity: 0.9;
	}

	.map-halo.selected-halo {
		stroke: var(--color-accent);
		stroke-width: 4.2;
		opacity: 0.48;
	}

	.map-area:focus-visible {
		stroke: var(--color-focus);
		stroke-width: 1.7;
		filter: drop-shadow(0 0 0.16rem rgb(0 56 168 / 0.52));
	}

	.map-area:active {
		transform: scale(0.992);
		filter: drop-shadow(0 0.06rem 0.08rem rgb(30 41 59 / 0.24));
	}

	@media (prefers-reduced-motion: reduce) {
		.map-area,
		.map-shape {
			transition: none;
		}

		.map-area:active {
			transform: none;
		}
	}

	@media (forced-colors: active) {
		.map-halo {
			display: none;
		}

		.map-area:focus-visible,
		.map-area.is-selected,
		.map-shape.is-selected {
			stroke: Highlight;
			filter: none;
		}
	}
</style>
