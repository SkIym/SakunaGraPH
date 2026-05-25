/* based on disaster types found in sakunagraph.ttl
   https://drive.google.com/drive/folders/1VqV41zl1VNvrfgynETaelfK1KTBUsrzS */

const disaster_types_l1: string[] = [
    "Natural",
    "Technological",
];

const disaster_types_l2: Map<string, string[]> = new Map([
    ["Natural", ["Biological", "Climatological", "Extraterrestrial", "Geophysical", "Hydrological", "Meteorological" ]],
    ["Technological", ["Armed Conflict", "Industrial Accident", "Miscellaneous Accident", "Transport" ]]
]);

const disaster_types_l3: Map<string, string[]> = new Map([
    ["Biological", ["Animal Accident", "Epidemic", "Infestation"]],
    ["Climatological", ["Drought", "Glacial", "Wildfire"]],
    ["Extraterrestrial", ["Space Impact", "Space Weather"]],
    ["Geophysical", ["Earthquake", "Mass Movement (Dry)", "Volcanic Activity"]],
    ["Hydrological", ["Flood", "Mass Movement (Wet)", "Wave Action"]],
    ["Meteorological", ["Extreme Temperature", "Fog", "Storm"]],
    ["Industrial Accident", ["Chemical Spill", "Collapse (Industrial)", "Explosion (Industrial)", "Fire (Industrial)", "Gas Leak", "Industrial Accident (General)", "Oil Spill", "Poisoning", "Radiation"]],
    ["Miscellaneous Accident", ["Collapse (Misc)", "Explosion (Misc)", "Fire (Misc)", "Miscellaneous Accident (General)"]],
    ["Transport", ["Air", "Rail", "Road", "Water"]]
]);

const disaster_types_l4: Map<string, string[]> = new Map([
    ["Epidemic", ["Bacterial Disease", "Fungal Disease", "Infectious Disease (General)", "Parasitic Disease", "Prion Disease", "Viral Disease"]],
    ["Infestation", ["Grasshopper Infestation", "Infestation (General)", "Locust Infestation", "Worms Infestation"]],
    ["Wildfire", ["Forest Fire", "Land Fire", "Wildfire (General)"]],
    ["Earthquake", ["Ground Movement", "Tsunami"]],
    ["Mass Movement (Dry)", ["Avalanche (Dry)", "Landslide (Dry)", "Rockfall (Dry)", "Sudden Subsidence (Dry)"]],
    ["Volcanic Activity", ["Ashfall", "Lahar", "Lava Flow", "Pyroclastic Flow", "Volcanic Activity (General)"]],
    ["Flood", ["Coastal Flood", "Flash Flood", "Flood (General)", "Ice Jam Flood", "Riverine Flood"]],
    ["Mass Movement (Wet)", ["Avalanche (Wet)", "Landslide (Wet)", "Mudslide", "Rockfall (Wet)", "Sudden Subsidence (Wet)"]],
    ["Wave Action", ["Rogue Wave", "Seiche"]],
    ["Extreme Temperature", ["Cold Wave", "Heat Wave", "Severe Winter Conditions"]],
    ["Storm", ["Blizzard Storm", "Derecho", "Extratropical Storm", "Hail", "Sand Storm", "Severe Weather", "Storm (General)", "Storm Surge", "Thunderstorms", "Tornado", "Tropical Cyclone"]]
]);

class DisasterType {
    name : string;
    index : number;
    hierarchyLevel : number;
    parent: number | null;
    children: number[];
    checked: boolean;
    expand: boolean;
    show: boolean;
};

let disasterTypesArray: DisasterType[] = [];
for (const l1_elem of disaster_types_l1) {
    let l1 = {
        name: l1_elem,
        index: disasterTypesArray.length,
        hierarchyLevel: 1,
        parent: null,
        children: [],
        checked: false,
        expand: false,
        show: true
    };
    disasterTypesArray.push(l1);

    for (const l2_elem of disaster_types_l2.get(l1_elem) || []) {
        let l2 = {
            name: l2_elem,
            index: disasterTypesArray.length,
            hierarchyLevel: 2,
            parent: l1.index,
            children: [],
            checked: false,
            expand: false,
            show: false
        };
        disasterTypesArray.push(l2);
        l1.children.push(l2.index);

        for (const l3_elem of disaster_types_l3.get(l2_elem) || []) {
            let l3 = {
                name: l3_elem,
                index: disasterTypesArray.length,
                hierarchyLevel: 3,
                parent: l2.index,
                children: [],
                checked: false,
                expand: false,
                show: false
            };
            disasterTypesArray.push(l3);
            l2.children.push(l3.index);

            for (const l4_elem of disaster_types_l4.get(l3_elem) || []) {
                let l4 = {
                    name: l4_elem,
                    index: disasterTypesArray.length,
                    hierarchyLevel: 4,
                    parent: l3.index,
                    children: [],
                    checked: false,
                    expand: false,
                    show: false
                };
                disasterTypesArray.push(l4);
                l3.children.push(l4.index);
            }
        }
    }
}

export { disasterTypesArray, DisasterType };