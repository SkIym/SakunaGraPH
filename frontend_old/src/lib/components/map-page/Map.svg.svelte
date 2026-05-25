<!--
  @component
  Generates an SVG map using the `geoPath` function from [d3-geo](https://github.com/d3/d3-geo).
 -->
<script>
  import { getContext } from 'svelte';
  import { geoPath } from 'd3-geo';
  import { raise } from 'layercake';
	import { setLocationToShow, setShowInfo } from '$lib/map/locationInfoStates.svelte';
	import { getHoveredLocation } from '$lib/map/adminLevelStates.svelte';

  const { data, width, height, zGet } = getContext('LayerCake');

  /**
   * @typedef {Object} Props
   * @property {Function} projection - A D3 projection function. Pass this in as an uncalled function, e.g. `projection={geoAlbersUsa}`.
   * @property {number|undefined} [fixedAspectRatio] - By default, the map fills to fit the $width and $height. If instead you want a fixed-aspect ratio, like for a server-side rendered map, set that here.
   * @property {string|undefined} [fill] - The shape's fill color. By default, the fill will be determined by the z-scale, unless this prop is set.
   * @property {string} [stroke='#333'] - The shape's stroke color.
   * @property {number} [strokeWidth=0.5] - The shape's stroke width.
   * @property {Array<Object>|undefined} [features] - A list of GeoJSON features. Use this if you want to draw a subset of the features in `$data` while keeping the zoom on the whole GeoJSON feature set. By default, it plots everything in `$data.features` if left unset.
   * @property {(e: MouseEvent, props: Object) => void} [onmousemove] - A function that gets called on mousemove events. The first argument is the event, and the second is the properties of the hovered feature.
   * @property {(e: MouseEvent) => void} [onmouseout] - A function that gets called on mouseout events.
   * @property {(e: MouseEvent, props: Object) => void} [onclick]
   * @property {number} [zoom]
   * @property {number} [translateX]
   * @property {number} [translateY]
   */

  /** @type {Props} */
  let {
    projection,
    fixedAspectRatio,
    fill,
    stroke = '#333',
    strokeWidth = 0.5,
    features,
    onmousemove = () => {},
    onmouseout = () => {},
    onclick = () => {},
    zoom = 1,
    translateX = 0,
    translateY = 0
  } = $props();

  /* --------------------------------------------
   * Here's how you would do cross-component hovers
   */
  let fitSizeRange = $derived(fixedAspectRatio ? [100, 100 / fixedAspectRatio] : [$width, $height]);

  let projectionFn = $derived(projection().fitSize(fitSizeRange, $data));

  let geoPathFn = $derived(geoPath(projectionFn));

  let translation = $derived({ x: translateX, y: translateY });

  function handleMousemove(feature) {
    return function handleMousemoveFn(e) {
      // @ts-ignore
      raise(this);
      // When the element gets raised, it flashes 0,0 for a second so skip that
      if (e.layerX !== 0 && e.layerY !== 0) {
        onmousemove(e, feature.properties);
      }
    };
  }

  function handleClick(e, props) {
    console.log('Clicked region:', props);
    setShowInfo(true);
    setLocationToShow(getHoveredLocation(props));
    onclick(e, props);
  }
</script>

<!-- svelte-ignore a11y_mouse_events_have_key_events -->
<g class="map-group" {onmouseout} role="tooltip" transform="scale({zoom}) translate({translation.x + -$width * (zoom - 1) / (2 * zoom)}, {translation.y + -$height * (zoom - 1) / (2 * zoom)})">
  {#each features || $data.features as feature}
    <path
      class="feature-path"
      fill={fill || $zGet(feature.properties)}
      {stroke}
      stroke-width={strokeWidth}
      d={geoPathFn(feature)}
      onclick={(e) => handleClick(e, feature.properties)}
      onkeydown={() => {return}}
      onmouseover={e => onmousemove(e, feature.properties)}
      onmousemove={handleMousemove(feature)}
      role="button"
      tabindex=0
    ></path>
  {/each}
</g>

<style>
  .map-group {
    opacity: 0.9;
  }
  .feature-path {
    stroke-opacity: 0.8;
  }
  .feature-path:hover {
    stroke: #000;
    stroke-width: 2px;
    cursor: pointer;
  }
  /**
   * Disable the outline on feature click.
   * Depending on map functionality and accessiblity issues,
   * you may not want this rule. Read more:
   * https://developer.mozilla.org/en-US/docs/Web/CSS/:focus
   * https://github.com/mhkeller/layercake/issues/63
   */
  .feature-path:focus {
    outline: none;
  }
</style>