<!-- 
TODO:
[/] - fix searching
[] - finish applying filters to metrics
[/] - option to get current user location
[] - make fixed sidebar (?)
 -->

<script lang="ts">
  import { Datepicker, Badge, Card, Button, Dropdown, DropdownGroup, Checkbox, Search } from "flowbite-svelte";
  import { FilterSolid, ChevronDownOutline, ChevronRightOutline, MapPinAltSolid, CloseCircleSolid } from "flowbite-svelte-icons";
  import { Location } from "$lib/filter/location_arrays";
  import { DisasterType } from "$lib/filter/disaster_types";
  import { searchStates } from "$lib/filter/search_states.svelte";
  import { locations, getShowableLocations, setSelectedLocations, getCheckedLocations, findLocation } from "$lib/filter/location_states.svelte";
  import { disasterTypes, getShowableDisasterTypes, getCheckedDisasterTypes, setSelectedDisasterTypes } from "$lib/filter/disaster_states.svelte";
  import { dateRange } from "$lib/filter/date_states.svelte";
  import { getLocation, getUserLocation } from "$lib/filter/user_location.svelte";

  // initialize user location
  getLocation();

  type color = "blue" | "red" | "yellow" | "gray" | "primary" 
              | "secondary" | "orange" | "amber" | "lime" | "green" 
              | "emerald" | "teal" | "cyan" | "sky" | "indigo" | "violet" 
              | "purple" | "fuchsia" | "pink" | "rose";

  function getColorFromType(type: string | number): color {
    switch (type) {
      case "region":   return "blue";
      case 1:          return "blue";
      case "province": return "red";
      case 2:          return "red";
      case "municity": return "yellow";
      case 3:          return "yellow";
      default:         return "gray";
    }
  }

  function handleSelect(item: Location | DisasterType, arr: Location[] | DisasterType[], checked: boolean) {
    item.checked = checked;
    
    function updateChildren(arr: Location[] | DisasterType[], itemIndex: number, shouldCheck: boolean) {
      const item = arr[itemIndex];
      for (const childIndex of item.children) {
        arr[childIndex].checked = shouldCheck;
        if (arr[childIndex].children.length > 0) updateChildren(arr, childIndex, shouldCheck);
      }
    }

    function updateParent(arr: Location[] | DisasterType[], itemIndex: number, shouldCheck: boolean) {
      const item = arr[itemIndex];
      arr[item.parent].checked = shouldCheck;
      if (arr[item.parent].parent != null) updateParent(arr, item.parent, shouldCheck);
    }
    
    if (checked) {
      if (item.children.length > 0) updateChildren(arr, item.index, true);
      if (item.parent != null) {
        let checkedChildren = arr[item.parent].children.filter((child) => arr[child].checked);
        if (checkedChildren.length == arr[item.parent].children.length) updateParent(arr, item.index, true);
      }
    } else {
      if (item.children.length > 0) updateChildren(arr, item.index, false);
      if (item.parent != null) {
        let checkedChildren = arr[item.parent].children.filter((child) => arr[child].checked);
        if (checkedChildren.length < arr[item.parent].children.length) updateParent(arr, item.index, false);
      }
    }
  }

  function resetLocationChecked() {
    for (let location of locations) location.checked = false;
  }

  function resetDisasterChecked() {
    for (let type of disasterTypes) type.checked = false;
  }

  function selectUserLocation() {
    let fullname = getUserLocation();
    if (fullname == "") return;
    handleSelect(findLocation(fullname), locations, true);
  }

  let open = $state(false);
  let mobileFilterOpen = $state(false);

  let dateStartSelected = $derived(dateRange.start);
  let dateEndSelected = $derived(dateRange.end);
  let searchLocation = $derived(searchStates.location);
  let searchDisaster = $derived(searchStates.disaster);

  // Total active filter count for the mobile badge
  let activeFilterCount = $derived(
    getCheckedLocations().length + getCheckedDisasterTypes().length +
    (dateRange.start || dateRange.end ? 1 : 0)
  );

  $effect(() => {
    searchStates.location = searchLocation;
    searchStates.disaster = searchDisaster;
    dateRange.start = dateStartSelected;
    dateRange.end = dateEndSelected;
  });
</script>

<!-- Mobile filter toggle button (fixed, only visible on small screens) -->
<div class="fixed bottom-5 right-5 z-200 md:hidden">
  <button
    onclick={() => mobileFilterOpen = true}
    class="flex items-center gap-2 bg-(--color-theme-2) text-white font-bold text-sm px-4 py-2.5 rounded-full shadow-xl hover:opacity-90 active:scale-95 transition-all"
    aria-label="Open filters"
  >
    <FilterSolid class="h-4 w-4" />
    Filters
    {#if activeFilterCount > 0}
      <span class="bg-white text-(--color-theme-2) text-[10px] font-extrabold rounded-full w-4 h-4 flex items-center justify-center leading-none">
        {activeFilterCount}
      </span>
    {/if}
  </button>
</div>

<!-- Mobile backdrop -->
{#if mobileFilterOpen}
  <div
    class="fixed inset-0 bg-black/40 z-150 md:hidden backdrop-blur-sm"
    onclick={() => mobileFilterOpen = false}
    role="presentation"
  ></div>
{/if}

<!-- Filter panel: sidebar on desktop, bottom sheet on mobile -->
<div class="
  md:relative md:block md:mr-5 md:mt-10 md:w-1/6
  fixed bottom-0 left-0 right-0 z-200
  transition-transform duration-300 ease-in-out
  md:translate-y-0
  {mobileFilterOpen ? 'translate-y-0' : 'translate-y-full'}
  md:shadow-none
">
  <Card class="
    md:p-4 md:rounded-xl md:border md:border-gray-300 md:shadow-md
    p-4 rounded-t-2xl rounded-b-none border-t border-gray-200 shadow-2xl
    [background-image:var(--card-bg)] dark:[background-image:var(--card-bg-dark)]
    w-full md:w-full
    max-h-[85vh] md:max-h-none overflow-y-auto md:overflow-visible
  ">

    <div class="flex items-center justify-between mb-4">
      <h5 class="flex items-center gap-2 text-xl leading-none font-bold text-black dark:text-gray-300">
        Data Filter <FilterSolid class="h-5 w-5"/>
      </h5>
      <!-- Close button: mobile only -->
      <button
        class="md:hidden flex items-center justify-center text-gray-500 hover:text-gray-800 transition-colors"
        onclick={() => mobileFilterOpen = false}
        aria-label="Close filters"
      >
        <CloseCircleSolid class="h-5 w-5" />
      </button>
    </div>

    <!-- Drag handle pill: mobile only -->
    <div class="md:hidden flex justify-center -mt-2 mb-3">
      <div class="w-10 h-1 rounded-full bg-gray-300"></div>
    </div>

    <div class="space-y-4">
      <!-- Locations -->
      <div class="block">
        <div class="mb-1.5">
          <Button size="xs" 
            class="w-full bg-(--color-theme-2)! hover:opacity-90 border-none shadow-sm flex items-center justify-between px-3 py-1.5" 
            id="location_trigger">
            <span class="text-sm font-bold text-white">Locations</span>
            <ChevronRightOutline class="text-white h-3.5 w-3.5" />
          </Button>
        </div>

        <div class="bg-white/60 border border-gray-300 rounded-lg p-2 h-40 md:h-72 overflow-y-auto backdrop-blur-sm shadow-inner">
          {#if getCheckedLocations().length === 0}
            <div class="flex items-center justify-center h-full text-gray-600 text-[11px] italic font-medium">No locations selected</div>
          {:else}
            <div class="flex items-center mb-1 justify-between border-b border-gray-200 pb-1">
              <span class="text-gray-800 text-[9px] font-extrabold uppercase tracking-tight">Selected</span>
              <button class="text-(--color-theme-2) text-[10px] font-bold hover:underline" onclick={() => { setSelectedLocations([]); resetLocationChecked(); }}>
                Clear
              </button>
            </div>
            <div class="flex flex-wrap gap-1">
              {#each getCheckedLocations() as item (item.fullname)}
                <Badge size="small" color={getColorFromType(item.type)} dismissable onclose={() => { handleSelect(item, locations, false) }} class="text-[9px] font-bold px-1.5 py-0 shadow-sm">
                  {item.fullname}
                </Badge>
              {/each}
            </div>
          {/if}
        </div>
      </div>

      <!-- Disaster Types -->
      <div class="block">
        <div class="mb-1.5">
          <Button size="xs" 
            class="w-full bg-(--color-theme-2)! hover:opacity-90 border-none shadow-sm flex items-center justify-between px-3 py-1.5" 
            id="disaster_trigger">
            <span class="text-sm font-bold text-white">Disaster Types</span>
            <ChevronRightOutline class="text-white h-3.5 w-3.5" />
          </Button>
        </div>

        <div class="bg-white/60 border border-gray-300 rounded-lg p-2 h-40 md:h-72 overflow-y-auto backdrop-blur-sm shadow-inner">
          {#if getCheckedDisasterTypes().length === 0}
            <div class="flex items-center justify-center h-full text-gray-600 text-[11px] italic font-medium">No types selected</div>
          {:else}
            <div class="flex items-center mb-1 justify-between border-b border-gray-200 pb-1">
              <span class="text-gray-800 text-[9px] font-extrabold uppercase tracking-tight">Selected</span>
              <button class="text-(--color-theme-2) text-[10px] font-bold hover:underline" onclick={() => { setSelectedDisasterTypes([]); resetDisasterChecked(); }}>
                Clear
              </button>
            </div>
            <div class="flex flex-wrap gap-1">
              {#each getCheckedDisasterTypes() as item (item.name)}
                <Badge size="small" color={getColorFromType(item.hierarchyLevel)} dismissable onclose={() => { handleSelect(item, disasterTypes, false) }} class="text-[9px] font-bold px-1.5 py-0 shadow-sm">
                  {item.name}
                </Badge>
              {/each}
            </div>
          {/if}
        </div>
      </div>

      <!-- Date Range -->
      <div class="block">
        <div class="mb-1.5">
          <div class="w-full bg-(--color-theme-2)! border-none shadow-sm flex items-center px-3 py-1.5 rounded-lg">
            <span class="text-sm font-bold text-white tracking-wide">Date Range</span>
          </div>
        </div>
        <div class="p-0.5 rounded-lg border border-gray-300 shadow-inner">
          <Datepicker range showActionButtons dateFormat={{ year: "numeric", month: "2-digit", day: "2-digit" }} bind:rangeFrom={dateStartSelected} bind:rangeTo={dateEndSelected} class="text-[11px] border-none py-1 text-gray-900 font-semibold" />
        </div>
      </div>
    </div>

    <!-- Mobile apply button -->
    <div class="md:hidden mt-4 pb-2">
      <Button
        class="w-full bg-(--color-theme-2)! text-white font-bold"
        onclick={() => mobileFilterOpen = false}
      >
        Apply Filters
      </Button>
    </div>
  </Card>
</div>

<!-- Dropdowns (unchanged) -->
<Dropdown class="text-xs w-72 shadow-2xl rounded-xl" placement="right-start" triggeredBy="#location_trigger">
  <div class="p-2 flex flex-col gap-2 border-b rounded-t-xl">
    <Search size="sm" bind:value={searchLocation} class="text-xs focus:ring-(--color-theme-2)"/>
    <Button size="xs" class="bg-(--color-theme-2)! font-bold text-white flex items-center justify-center" onclick={async () => {if (getUserLocation() == "") getLocation(); selectUserLocation()}}>
      <MapPinAltSolid size="xs" class="mr-1 text-white h-3 w-3" /> My Location
    </Button>
  </div>
  <DropdownGroup class="max-h-64 overflow-y-auto p-1 rounded-b-xl">
    {#each getShowableLocations() as item (item.fullname)}
      <li class="rounded transition-colors list-none">
        <div class="flex items-center p-1" style="padding-left: {item.type === 'province' ? '1.25rem' : item.type === 'municity' ? '3.25rem' : '0.4rem'}">
          {#if item.type !== 'municity'}
            <button class="shrink-0 p-0.5 mr-1 transition-colors" onclick={() => { item.expand = !item.expand; }}>
              <ChevronDownOutline class="h-3 w-3 transition-transform {item.expand ? 'rotate-0' : '-rotate-90'}" />
            </button>
          {/if}
          <Checkbox onclick={() => handleSelect(item, locations, !item.checked)} bind:checked={item.checked} class="text-[11px] {item.type === 'region' ? 'font-bold' : 'font-medium'}">
            <p>{item.name}</p>
          </Checkbox>
        </div>
      </li>
    {/each}
  </DropdownGroup>
</Dropdown>

<Dropdown class="text-xs w-72 shadow-2xl rounded-xl" placement="right-start" triggeredBy="#disaster_trigger">
  <div class="p-2 border-b rounded-t-xl">
    <Search size="sm" bind:value={searchDisaster} class="text-xs focus:ring-(--color-theme-2)" />
  </div>
  <DropdownGroup class="max-h-64 overflow-y-auto p-1 rounded-b-xl">
    {#each getShowableDisasterTypes() as item (item.name)}
      <li class="rounded transition-colors list-none" style="margin-left: {(item.hierarchyLevel - 1) * 1 + (item.hierarchyLevel == 4 ? 0.75 : 0) + (item.hierarchyLevel < 4 && item.children.length == 0 ? 1.25 : 0)}rem">
        <div class="flex items-center p-1">
          {#if item.children?.length > 0}
            <button class="shrink-0 p-0.5 mr-1 transition-colors" onclick={() => { item.expand = !item.expand; }}>
              <ChevronDownOutline class="h-3 w-3 transition-transform {item.expand ? 'rotate-0' : '-rotate-90'}" />
            </button>
          {/if}
          <Checkbox onclick={() => handleSelect(item, disasterTypes, !item.checked)} bind:checked={item.checked} class="text-[11px] {item.hierarchyLevel === 1 ? 'font-bold' : 'font-medium'}">
            <p>{item.name}</p>
          </Checkbox>
        </div>
      </li>
    {/each}
  </DropdownGroup>
</Dropdown>