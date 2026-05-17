import { getSelectedLocations } from '$lib/filter/location_states.svelte';
import { getSelectedDisasterTypes } from '$lib/filter/disaster_states.svelte';
import { dateRange } from '$lib/filter/date_states.svelte';

let allData: any[] = $state.raw([]);

export function setAllData(data: any[]) {
    allData = data;
}

let selectedLocations = $derived.by(() => {
    let res = getSelectedLocations().filter((loc) => loc.type == 'municity').map(loc => loc.fullname);
    return res.filter((item, index) => res.indexOf(item) === index);
});

let selectedDisasterTypes = $derived.by(() => {
    let res = getSelectedDisasterTypes().map(type => type.name);
    return res.filter((item, index) => res.indexOf(item) === index);
});

let filteredData = $derived.by(() => {
    return allData.filter(d => {
        let locationName = d['city_municipality'] + ', ' + d['province'] + ', ' + d['region'];
        const disasterType = d['type'];
        const date = new Date(d['date']);
        return locationName &&
            (selectedLocations.includes(locationName) || selectedLocations.length === 0) &&
            (selectedDisasterTypes.includes(disasterType) || selectedDisasterTypes.length === 0) &&
            ((dateRange.start <= date || dateRange.start == undefined) &&
                (date <= dateRange.end || dateRange.end == undefined));
    });
});

function getSelectedLocationNames() {
    return selectedLocations;
}

function getSelectedDisasterTypeNames() {
    return selectedDisasterTypes;
}

function getFilteredCsvData() {
    return filteredData;
}

function getAllData() {
    return allData;
}

export { getSelectedLocationNames, getSelectedDisasterTypeNames, getFilteredCsvData, getAllData };
