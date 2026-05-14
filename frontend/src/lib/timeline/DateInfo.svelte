<script>
	import { getFilteredCsvData } from "$lib/demo_data/filteredData.svelte";
	import { getShowInfo, getDateToShow, setShowInfo, setDateToShow } from "$lib/timeline/dateInfoStates.svelte";
	import { Heading, Drawer, Card, List, Li, Span } from "flowbite-svelte";

    let open = $derived(getShowInfo());
	let dateName = $derived(getDateToShow());
	let filteredCsvData = $derived(getFilteredCsvData().filter((row) => {return row.date.includes(dateName)}));
</script>

<Drawer bind:open={open} placement="right" onclose={() => setShowInfo(false)} class="text-white">
    <Heading tag="h2" class="p-3 text-4xl font-extrabold ">{dateName}</Heading>
	{#each filteredCsvData as event}
		<Card class="p-2 m-2">
			<h5 class="mb-2 text-2xl font-bold tracking-tight text-gray-900 dark:text-white">{event.name}</h5>
  			<p class="leading-tight font-normal text-gray-700 dark:text-gray-400">
				This 
				<Span underline class="font-black">{event.type}</Span>
				 event hit 
				<Span underline class="font-black">{event.city_municipality}, {event.province}, {event.region}</Span>
				 on the day of 
				<Span underline class="font-black">{event.date}</Span>
				. It had the following effects on the area:
			</p>
			<List tag="ul" class="leading-tight font-normal text-gray-700 dark:text-gray-400">
				<Li><Span class="font-black">{event.monetary_damage} PHP</Span> in monetary damages</Li>
				<Li><Span class="font-black">{event.affected_families}</Span> affected families</Li>
				<Li><Span class="font-black">{event.casualties}</Span> casualties</Li>
				<Li><Span class="font-black">{event.injured}</Span> injured</Li>
				<Li><Span class="font-black">{event.missing}</Span> missing</Li>
			</List>
		</Card>
	{/each}
</Drawer>