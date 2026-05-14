import { getSelectedLocations, locations } from '$lib/filter/location_states.svelte';
import { getSelectedDisasterTypes } from '$lib/filter/disaster_states.svelte';
import csv_data from './disaster_data.csv';
import { dateRange } from '$lib/filter/date_states.svelte';

let selectedLocations = $derived.by(() => {
    let res = getSelectedLocations().filter((loc) => loc.type == 'municity').map(loc => loc.fullname);
    //console.log("selected locations: " + res);
    return res.filter((item, index) => res.indexOf(item) === index);
});

let selectedDisasterTypes = $derived.by(() => {
        let res = getSelectedDisasterTypes().map(type => type.name);
        return res.filter((item, index) => res.indexOf(item) === index);
    }
);

let filteredCsvData = $derived.by(() => {
    return csv_data.filter(d => {
        let locationName = d['city_municipality'] + ', ' + d['province'] + ', ' + d['region'];
        const disasterType = d['type'];
        const date = new Date(d['date']);
        return locationName && 
                (selectedLocations.includes(locationName) || 
                    selectedLocations.length === 0) &&
                (selectedDisasterTypes.includes(disasterType) || 
                    selectedDisasterTypes.length === 0) &&
                ((dateRange.start <= date || dateRange.start == undefined) && 
                    (date <= dateRange.end || dateRange.end == undefined)) 
        ;
    });
});

function getSelectedLocationNames() {
    console.log(selectedLocations);
    return selectedLocations;
}

function getSelectedDisasterTypeNames() {
    return selectedDisasterTypes;
}

function getFilteredCsvData() {
    //console.log("filtered csv data: " + filteredCsvData);
    return filteredCsvData;
}

export { getSelectedLocationNames, getSelectedDisasterTypeNames, getFilteredCsvData }