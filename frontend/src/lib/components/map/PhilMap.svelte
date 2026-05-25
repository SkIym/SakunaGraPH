<script>
	/**
	 * Pure SVG renderer for the Philippine map.
	 * All data loading and projection happen in the parent (map page).
	 */

	let {
		pathData = [],      // [{d, gid, name, regionPsgc, feature}]
		viewBox = '0 0 700 800',
		view = 'regions',   // 'regions' | 'provinces'
		selected = null,    // {psgc?, id?, name}
		interactive = true,
		strokeWidth = 0.7,
		onselect = () => {},
		onhover = () => {}  // onhover(item | null, clientX, clientY)
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
		if (sk && gk === sk) return '#93C5FD';
		if (interactive && hoveredKey === gk) return '#DBEAFE';
		return '#ffffff';
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
</script>

<svg
	{viewBox}
	class="w-full h-full transition-[viewBox] duration-500"
	preserveAspectRatio="xMidYMid meet"
	style="overflow:visible;"
>
	{#each pathData as item (item.gid)}
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<path
			d={item.d}
			fill={getFill(item)}
			stroke="#374151"
			stroke-width={strokeWidth}
			stroke-linejoin="round"
			class={interactive ? 'cursor-pointer' : ''}
			onmouseenter={(e) => handleEnter(item, e)}
			onmouseleave={handleLeave}
			onclick={() => handleClick(item)}
		/>
	{/each}
</svg>
