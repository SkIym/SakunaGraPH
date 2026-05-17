<!-- 
TODO:
[] - fix location info modal for province, municity; could be problem with
     getHoveredLocation
 -->

<script lang="ts">
    import { LayerCake, Svg } from 'layercake';
    import { geoMercator } from 'd3-geo';
    import { scaleSequential } from 'd3-scale';
    import { interpolateReds } from 'd3-scale-chromatic';
    import { format } from 'd3-format';
    import MapSvg from '../../../lib/components/map-page/Map.svg.svelte';
    import logo from '../../../lib/images/philippines_logo.svg';
    import regions_json from '../../../lib/map/ph.json'; 
    import provinces_json from '../../../lib/map/gadm41_PHL_1.json';
    import municities_json from '../../../lib/map/gadm41_PHL_2.json';
    import { Label, Range, Radio, Dropdown, DropdownItem, Button } from 'flowbite-svelte';
    import { ChevronDownOutline, ZoomInOutline, ZoomOutOutline } from "flowbite-svelte-icons";
    import LocationInfo from '$lib/components/LocationInfo.svelte';
    import { getSelectedLocationNames, getSelectedDisasterTypeNames, getFilteredCsvData, getAllData } from '$lib/demo_data/filteredData.svelte';
    import { dateRange } from '$lib/filter/date_states.svelte';
    import { getSelectedAdminLevel, setSelectedAdminLevel, getMapJoinKey, getLocationFullNameKeys, getHoveredLocation } from '$lib/map/adminLevelStates.svelte';

	  const projection = geoMercator;
    const addCommas = format(',');

    let selectedLocations = $derived(getSelectedLocationNames());
    let selectedDisasterTypes = $derived(getSelectedDisasterTypeNames());
    let selectedAdminLevel = $derived(getSelectedAdminLevel());
    let zoom = $state(1);
    let dragging = $state(false);
    let translateX = $state(0);
    let translateY = $state(0);
    let filteredCsvData = $derived(getFilteredCsvData());

    let selectedMetric = $state('NoOfDisasters');
    let tooltipEvent = $state(null);
    let tooltipFeature = $state(null);
    let mapJoinKey = $derived(getMapJoinKey());

    let dataLookup = $derived.by(() => {
      if (selectedMetric === 'NoOfDisasters') {
          const lookup = calcNoOfDisasters();
          //console.log(lookup);
          return lookup;
      } else if (selectedMetric === 'AffectedFamilies') {
          const lookup = calcAffectedFamilies();
          //console.log(lookup);
          return lookup;
      } else if (selectedMetric === 'MonetaryDamages') {
          const lookup = calcMonetaryDamages();
          //console.log(lookup);
          return lookup;
      }else if (selectedMetric === 'Casualties') {
          const lookup = calcCasualties();
          //console.log(lookup);
          return lookup;
      } else if (selectedMetric === 'Injured') {
          const lookup = calcInjured();
          //console.log(lookup);
          return lookup;
      } else if (selectedMetric === 'Missing') {
          const lookup = calcMissing();
          //console.log(lookup);
          return lookup;
      }else {
          const lookup = new Map();
          return lookup;
      }
    });

    let selectedMetricLabel = $derived.by(() => {
      if (selectedMetric === 'NoOfDisasters') {
          return 'Number of Disasters';
        } else if (selectedMetric === 'AffectedFamilies') {
          return 'Affected Families';
        } else if (selectedMetric === 'MonetaryDamages') {
          return 'Monetary Damages (PHP)';
        } else if (selectedMetric === 'Casualties') {
          return 'Casualties';
        } else if (selectedMetric === 'Injured') {
          return 'Injured';
        } else if (selectedMetric === 'Missing') {
          return 'Missing';
        }else {
          return selectedMetric;
        }
    });

    let geojson = $derived.by(() => {
      if (selectedAdminLevel === 'regions') {
          return regions_json;
        } else if (selectedAdminLevel === 'provinces') {
          return provinces_json;
        } else if (selectedAdminLevel === 'municipalities_cities') {
          return municities_json;
        } else {
          return regions_json;
        }
      }
    );

    let filteredGeoFeatures = $derived(
      geojson.features.filter(feature => {
          let locationName = "";
          let checkLocations = [];
          if (selectedAdminLevel === 'regions') {
            locationName = feature.properties?.['name'];
            checkLocations = selectedLocations.map(loc => loc.split(', ').slice(2));
          } else if (selectedAdminLevel === 'provinces') {
            locationName = feature.properties?.['PROVINCE'] + ', ' + feature.properties?.['REGION'];
            checkLocations = selectedLocations.map(loc => loc.split(', ').slice(1));
          } else {
            locationName = feature.properties?.['CITY_MUNICIPALITY'] + ', ' + feature.properties?.['PROVINCE'] + ', ' + feature.properties?.['REGION'];
            checkLocations = selectedLocations.map(loc => loc.split(', '));
          }
          //console.log("checking location: " + locationName.split(', '));
          //console.log("selected locations: " + checkLocations);
          return locationName && (checkLocations.filter((loc) => loc.every((part, i) => part === locationName.split(', ')[i])).length > 0 || selectedLocations.length === 0);
      })
    );

    let filteredGeoJson = $derived(
        {
            type: "FeatureCollection",
            features: filteredGeoFeatures
        }
    );
    
    let flatData = $derived(filteredGeoJson.features.map(d => d.properties));

    let maxMetric = $derived(Math.max(...dataLookup.values()));

    function calcNoOfDisasters(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData();

        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + 1);
            } else {
                res.set(d[locationKey], 1);
            }
        });

        //console.log(res);
        return res;
    }
    
    function calcAffectedFamilies(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData()

        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            const affectedFamilies = parseInt(d['affected_families']) || 0;
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + affectedFamilies);
            } else {
                res.set(d[locationKey], affectedFamilies);
            }
        });

        //console.log(res);
        return res;
    }

    function calcMonetaryDamages(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData();
        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            const monetaryDamages = parseInt(d['monetary_damage']) || 0;
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + monetaryDamages);
            } else {
                res.set(d[locationKey], monetaryDamages);
            }
        });

        //console.log(res);
        return res;
    }

    function calcCasualties(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData();
        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            const casualties = parseInt(d['casualties']) || 0;
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + casualties);
            } else {
                res.set(d[locationKey], casualties);
            }
        });

        //console.log(res);
        return res;
    }

    function calcInjured(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData();

        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            const injured = parseInt(d['injured']) || 0;
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + injured);
            } else {
                res.set(d[locationKey], injured);
            }
        });

        //console.log(res);
        return res;
    }

    function calcMissing(){
        let res = new Map();
        let locationKey = "";
        let data = (selectedLocations.length || selectedDisasterTypes.length || dateRange.start != undefined || dateRange.end != undefined) ? filteredCsvData : getAllData();

        if (selectedAdminLevel === "regions"){
            locationKey = "region";
        } else if (selectedAdminLevel === "provinces"){
            locationKey = "province";
        } else if (selectedAdminLevel === "municipalities_cities"){
            locationKey = "city_municipality";
        }

        data.forEach(d => {
            const missing = parseInt(d['missing']) || 0;
            if (res.has(d[locationKey])){
                res.set(d[locationKey], res.get(d[locationKey]) + missing);
            } else {
                res.set(d[locationKey], missing);
            }
        });

        //console.log(res);
        return res;
    }

    function handleDrag(e) {
      if (dragging) {
        translateX += e.movementX;
        translateY += e.movementY;
      }
    }

    $effect(() => {
      //console.log("zoom: " + zoom);
      //setSelectedAdminLevel(selectedAdminLevel);
    });
</script>

<svelte:head>
    <title>Philippines Disaster Map</title>
    <meta name="description" content="Map view" />
    <link rel="icon" href={logo} />
</svelte:head>

<div class="flex justify-center">
	<div class="flex flex-col my-6 max-w-300">
		<h1 class = "mb-4 border-b-2 border-gray-300 dark:border-gray-800 pb-2 font-bold text-xl md:text-2xl lg:text-3xl text-gray-900 dark:text-gray-100">
			GEOGRAPHICAL MAP
		</h1>
		<p class="text-justify dark:text-gray-300 text-gray-700">
			The map displays real-time data on natural disasters occurring across the Philippines.
			Users can interact with the map to view detailed information about each disaster event,
			such as its type, location, severity, and time of occurrence.
		</p>
	</div>
</div>

<div class="p-6 rounded-xl shadow-md border border-black dark:border-gray-600 flex flex-col gap-6 [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)] w-full">
  <div class="flex flex-col md:flex-row justify-between items-start gap-4">
    <div class="w-full md:min-w-112.5 md:w-auto">
      <div class="mb-1.5">
        <div class="bg-[#003882] border-none shadow-sm flex items-center px-3 py-1.5 rounded-t-lg">
          <span class="text-xs font-bold text-white uppercase tracking-wide">Administration Level</span>
        </div>
        <ul class="w-full items-center divide-x divide-gray-200 rounded-b-lg border border-gray-300 flex bg-white/60 backdrop-blur-sm shadow-inner">
          <li class="w-full">
            <Radio name="hor-list" bind:group={selectedAdminLevel} onclick={() => setSelectedAdminLevel("regions")} value="regions" classes={{ label: "p-3 text-[11px] font-bold !text-black cursor-pointer" }}>
              <span class="text-black!">Regions</span>
            </Radio>
          </li>
          <li class="w-full">
            <Radio name="hor-list" bind:group={selectedAdminLevel} onclick={() => setSelectedAdminLevel("provinces")} value="provinces" classes={{ label: "p-3 text-[11px] font-bold !text-black cursor-pointer" }}>
              <span class="text-black!">Provinces</span>
            </Radio>
          </li>
          <li class="w-full">
            <Radio name="hor-list" bind:group={selectedAdminLevel} onclick={() => setSelectedAdminLevel("municipalities_cities")} value="municipalities_cities" classes={{ label: "p-3 text-[11px] font-bold !text-black cursor-pointer" }}>
              <span class="text-black!">Cities/Municipalities</span>
            </Radio>
          </li>
        </ul>
      </div>
    </div>

    <div class="w-full md:w-80">
      <div class="mb-1.5">
        <div class="bg-[#003882] border-none shadow-sm flex items-center px-3 py-1.5 rounded-t-lg">
          <span class="text-xs font-bold text-white uppercase tracking-wide">Metric</span>
        </div>
        <Button class="w-full bg-white/60 text-black border-none shadow-md flex justify-between items-center rounded-b-lg rounded-t-none px-4 py-2.5 transition-colors" id="metric_trigger">
          <span class="font-bold text-sm uppercase tracking-tight">{selectedMetricLabel}</span>
          <ChevronDownOutline class="h-5 w-5 text-black" />
        </Button>
        <Dropdown simple class="w-full md:w-80 shadow-2xl rounded-xl overflow-hidden z-50" triggeredBy="#metric_trigger">
          <div class="max-h-80 overflow-y-auto">
            {#each [
              { value: "NoOfDisasters", label: "Number of Disasters", desc: "Total disaster occurrences." },
              { value: "AffectedFamilies", label: "Affected Families", desc: "Total families impacted." },
              { value: "MonetaryDamages", label: "Monetary Damages (PHP)", desc: "Economic impact in Pesos." },
              { value: "Casualties", label: "Casualties", desc: "Total lives lost." },
              { value: "Injured", label: "Injured", desc: "Reported physical injuries." },
              { value: "Missing", label: "Missing", desc: "Unaccounted persons." }
            ] as metric}
              <DropdownItem class="p-3 border-b last:border-none">
                <Radio name="selectedMetric" bind:group={selectedMetric} value={metric.value} class="font-bold text-xs">
                  <span class="">{metric.label}</span>
                </Radio>
                <p class="ps-6 text-[10px]  italic leading-tight mt-1">{metric.desc}</p>
              </DropdownItem>
            {/each}
          </div>
        </Dropdown>
      </div>
    </div>
  </div>

  <button class="relative w-full rounded-xl bg-blue-300/30 h-[75vh] overflow-hidden {dragging ? "cursor-grabbing" : ""}" onmousedown={(e) => dragging = true} onmouseup={() => dragging = false} onmousemove={(e) => {handleDrag(e)}}>
    
    <div id="zoom" class="flex w-1/5 md:w-1/5 items-center justify-center absolute top-5 left-5 z-10 bg-white dark:bg-gray-400 backdrop-blur-md p-2 md:p-4 rounded-xl shadow-xl" >
      <div class="flex items-center gap-2 md:gap-3 justify-between">
        <ZoomOutOutline class="h-4 w-4 md:h-5 md:w-5 text-gray-700 cursor-pointer" onclick={() => zoom = Math.max(1, zoom - 0.2)}/>
        <Range id="range-steps" appearance="auto" color="blue" min=1 max=4 step=0.01 bind:value={zoom} onmousedown={(e) => { e.stopPropagation() }}/>
        <ZoomInOutline class="h-4 w-4 md:h-5 md:w-5 text-gray-700 cursor-pointer" onclick={() => zoom = Math.min(4, zoom + 0.2)}/>
      </div>
    </div>

    <div id="scale" class="absolute top-5 right-5 z-10 bg-white dark:bg-gray-400 backdrop-blur-md p-2 md:p-4 rounded-xl shadow-xl flex flex-col items-center">
      <p class="text-black font-black text-[8px] md:text-[10px] uppercase tracking-widest mb-2 md:mb-3 border-b border-gray-300 pb-1 w-full text-center">Color Scale</p>
      <div class="grid grid-cols-[auto_1fr] items-start gap-x-2 md:gap-x-3 h-32 md:h-52">
        <div class="text-[9px] md:text-[11px] font-black text-black text-right row-start-1 col-start-1">{addCommas(maxMetric)}</div>
        <div class="row-span-2 row-start-1 col-start-2 w-4 md:w-6 h-full rounded-full shadow-inner" 
             style="background: linear-gradient(to top, {interpolateReds(0)} 0%, {interpolateReds(0.5)} 50%, {interpolateReds(1)} 100%);">
        </div>
        <div class="text-[9px] md:text-[11px] font-black text-black text-right row-start-2 col-start-1 self-end">0</div>
      </div>
    </div>
        
    <div class="w-full h-full" >
      <LayerCake
        data={filteredGeoJson}
        z={d => dataLookup.get(d[mapJoinKey]) || 0}
        zScale={scaleSequential([0, maxMetric], interpolateReds)}
        {flatData}
      >
        <Svg>
          <MapSvg
            {projection}
            onmousemove={(event, feature) => { tooltipFeature = feature; tooltipEvent = event; }}
            onmouseout={() => { tooltipFeature = null; tooltipEvent = null; }}
            zoom={zoom}
            translateX={translateX}
            translateY={translateY}
          />
        </Svg>
      </LayerCake>
    </div>
  </button>
</div>

{#if tooltipFeature !== null && tooltipEvent !== null}
  {@const locationName = tooltipFeature[mapJoinKey]}
  {@const locationFullName = getHoveredLocation(tooltipFeature)}
  {@const metricValue = dataLookup.get(locationName) || 0}
  <div 
    class="fixed pointer-events-none z-9999 bg-white dark:bg-gray-800 dark:text-white p-3 rounded-lg border border-gray-700 min-w-48 shadow-2xl flex flex-col"
    style="left: {tooltipEvent.clientX}px; top: {tooltipEvent.clientY}px; transform: translate(-50%, calc(-100% - 20px));"
  >
    <div class="font-black text-sm mb-1 uppercase tracking-tight border-b border-gray-700 pb-1">
      {locationFullName}
    </div>
    <div class="flex flex-col gap-0.5 my-2">
      <span class="text-[10px] text-gray-400 font-bold uppercase">{selectedMetricLabel}</span>
      <span class="font-black text-xl text-[#0958c0]">
        {selectedMetric === 'MonetaryDamages' ? `₱${addCommas(metricValue)}` : addCommas(metricValue)}
      </span>
    </div>
    <div class="flex items-center gap-1.5 text-gray-500 text-[9px] italic font-medium">
      click for more information
    </div>
  </div>
{/if}

<LocationInfo></LocationInfo>