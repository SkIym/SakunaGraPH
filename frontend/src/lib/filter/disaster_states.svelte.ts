import { disasterTypesArray, DisasterType } from './disaster_types';
import {  searchStates } from './search_states.svelte';

let disasterTypes = $state(disasterTypesArray);

let filteredDisasterTypes = $derived.by(() =>
    {
        let disasters = searchStates.disaster != undefined ? disasterTypes.filter((item) =>
            item.name.toLowerCase().indexOf(searchStates.disaster?.toLowerCase()) !== -1
        ) : disasterTypes;

        if (searchStates.disaster != undefined) {
            for (let disaster of [...disasters]) {
                if (disaster.hierarchyLevel == 2) {
                    let l1 = disasterTypes[disaster.parent];

                    if (!disasters.includes(l1)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(disaster)), l1, ...disasters.slice(disasters.indexOf(disaster))];
                        l1.expand = true;
                    }
                } else if (disaster.hierarchyLevel == 3) {
                    let l2 = disasterTypes[disaster.parent];
                    let l1 = disasterTypes[l2.parent];

                    if (!disasters.includes(l2)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(disaster)), l2, ...disasters.slice(disasters.indexOf(disaster))];
                        l2.expand = true;
                    }

                    if (!disasters.includes(l1)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(l2)), l1, ...disasters.slice(disasters.indexOf(l2))];
                        l1.expand = true;
                    }
                } else if (disaster.hierarchyLevel == 4) {
                    let l3 = disasterTypes[disaster.parent];
                    let l2 = disasterTypes[l3.parent];
                    let l1 = disasterTypes[l2.parent];

                    if (!disasters.includes(l3)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(disaster)), l3, ...disasters.slice(disasters.indexOf(disaster))];
                        l3.expand = true;
                    }

                    if (!disasters.includes(l2)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(l3)), l2, ...disasters.slice(disasters.indexOf(l3))];
                        l2.expand = true;
                    }

                    if (!disasters.includes(l1)) {
                        disasters = [...disasters.slice(0, disasters.indexOf(l2)), l1, ...disasters.slice(disasters.indexOf(l2))];
                        l1.expand = true;
                    }
                }
            }
        }

        return disasters;
    }
  );

let showableDisasterTypes = $derived(
filteredDisasterTypes.filter((item) =>
    (item.hierarchyLevel == 1 || 
    (item.hierarchyLevel == 2 && disasterTypes[item.parent].expand) || 
    (item.hierarchyLevel == 3 && disasterTypes[item.parent].expand && disasterTypes[disasterTypes[item.parent].parent].expand) || 
    (item.hierarchyLevel == 4 && disasterTypes[item.parent].expand && disasterTypes[disasterTypes[item.parent].parent].expand && disasterTypes[disasterTypes[disasterTypes[item.parent].parent].parent].expand))
)
);

let selectedDisasterTypes = $derived(
    disasterTypes.filter((item) =>item.checked)
);

let checkedDisasterTypes = $derived(
    selectedDisasterTypes.filter((item) =>
    (item.hierarchyLevel == 1 ||
    (item.hierarchyLevel == 2 && !disasterTypes[item.parent].checked) ||
    (item.hierarchyLevel == 3 && !disasterTypes[item.parent].checked && !disasterTypes[disasterTypes[item.parent].parent].checked) ||
    (item.hierarchyLevel == 4 && !disasterTypes[item.parent].checked && !disasterTypes[disasterTypes[item.parent].parent].checked && !disasterTypes[disasterTypes[disasterTypes[item.parent].parent].parent].checked)))
);

function setSelectedDisasterTypes(disasterTypes: DisasterType[]) {
    selectedDisasterTypes = disasterTypes;
}

function getShowableDisasterTypes() {
    return showableDisasterTypes;
}

function getSelectedDisasterTypes() {
    return selectedDisasterTypes;
}

function getCheckedDisasterTypes() {
    return checkedDisasterTypes;
}

export { disasterTypes, getShowableDisasterTypes, getSelectedDisasterTypes, setSelectedDisasterTypes, getCheckedDisasterTypes };