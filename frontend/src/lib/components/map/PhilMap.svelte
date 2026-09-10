<script>
	/**
	 * Pure SVG renderer for the Philippine map.
	 * All data loading and projection happen in the parent (map page).
	 */

	let {
		pathData = [], // [{d, gid, name, regionPsgc, feature}]
		viewBox = '0 0 700 800',
		view = 'regions', // 'regions' | 'provinces'
		selected = null, // {psgc?, id?, name}
		interactive = true,
		strokeWidth = 0.7,
		strokeColor = '#374151',
		ariaLabel = 'Map of Philippine regions and provinces',
		getAreaLabel = (item) => `Select ${item.name}`,
		colorMap = {}, // groupKey → default fill color (for region pastels)
		onselect = () => {},
		onhover = () => {}, // onhover(item | null, clientX, clientY)
	} = $props();

	let hoveredKey = $state(null);

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
		if (sk && gk === sk) return '#93C5FD'; // selected — blue-300
		if (interactive && hoveredKey === gk) return '#BFDBFE'; // hovered  — blue-200
		return colorMap[gk] ?? '#ffffff'; // default  — region color or white
	}

	function handleEnter(item, e) {
		if (!interactive) return;
		hoveredKey = groupKey(item);
		onhover(item, e.clientX, e.clientY);
	}

	function handleLeave() {
		hoveredKey = null;
		onhover(null, 0, 0);
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
	class="w-full h-full"
	preserveAspectRatio="xMidYMid meet"
	style="overflow:visible;"
	aria-label={ariaLabel}
>
	{#each pathData as item (item.gid)}
		{#if interactive}
			<path
				d={item.d}
				fill={getFill(item)}
				stroke={strokeColor}
				stroke-width={strokeWidth}
				stroke-linejoin="round"
				class="map-area cursor-pointer outline-none"
				role="button"
				tabindex="0"
				aria-label={getAreaLabel(item)}
				aria-pressed={selectedKey() === groupKey(item)}
				onmouseenter={(e) => handleEnter(item, e)}
				onmouseleave={handleLeave}
				onclick={() => handleClick(item)}
				onkeydown={(event) => handleKeydown(item, event)}
			/>
		{:else}
			<path
				d={item.d}
				fill={getFill(item)}
				stroke={strokeColor}
				stroke-width={strokeWidth}
				stroke-linejoin="round"
			/>
		{/if}
	{/each}
</svg>

<style>
	.map-area:focus-visible {
		stroke: #1d4ed8;
		stroke-width: 1.5;
		filter: drop-shadow(0 0 1px rgb(29 78 216 / 0.45));
	}
</style>
