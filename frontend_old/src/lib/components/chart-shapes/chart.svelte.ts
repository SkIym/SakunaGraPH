//@ts-nocheck
// import type { _ } from '$env/static/private';
//import disaster from '$lib/demo_data/disaster_data.csv';
import { getFilteredCsvData } from "$lib/demo_data/filteredData.svelte";
let disaster = $derived(getFilteredCsvData());


// TODO: ADD MORE COLORS IF NEEDED
export const disasterColors = {
    "Typhoon": "#2C3E50",
    "Flood": "#3498DB",
    "Earthquake": "#8E44AD",
    "Volcanic Activity": "#E67E22",
    "Fire": "#E74C3C",
    "Landslide": "#6E2C00",
    "Climate": "#F1C40F",
    "Epidemic": "#27AE60",
    "Tsunami": "#2980B9",
    "Other": "#95A5A6",
}

export const subType = {
    "Typhoon": ["Trophical Cyclone"],
    "Flood": ["Flood (General)", "Riverine Flood", "Flash Flood", "Coastal Flood"],
    "Earthquake": ["Earthquake"],
    "Volcanic Activity": ["Ashfall", "Lava Flow", "Volcanic Activity (General)", "Lahar", "Ash Plume"],
    "Fire": ["Fire (Misc)", "Fire (Industrial)"],
    "Landslide": ["Landslide (Wet)", "Landslide (Dry)", "Mudslide"],
    "Climate": ["Drought", "Heat Wave"],
    "Epidemic": ["Viral Disease", "Bacterial Disease", "Infectious Disease (General)"],
    "Tsunami": ["Storm Surge", "Tsunami", "Seiche", "Local Wave"],
    "Other": ["Armed Clash", "Oil Spill", "Miscellaneous Accident (General)"],
}

export function generalizeType(input){
    for (const type of Object.keys(subType)){
        if (subType[type].includes(input)){
            return type;
        }
    }
    return "Other";
}

// === Data Processing Logic ===

// for CSYears
// Process disaster data - count by year and type
const date_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
function buildProcessedYears(disaster) {
    const acc = disaster.reduce((acc, item) => {
        if (!item.date || !item.type) return acc;
        const year = item.date.slice(0, 4);
        const month = Number.parseInt(item.date.slice(5, 7));
        const year_month = date_labels[month - 1] + " " + year;
        const type = item.type.trim();
        if (!acc[year_month]) {
            acc[year_month] = {};
        }
        const generalType = generalizeType(type);
        acc[year_month][generalType] = (acc[year_month][generalType] || 0) + 1;
        return acc;
    }, {});

    // fill missing months for each year
    const years = [...new Set(disaster.map(item => item.date?.slice(0, 4)).filter(Boolean))];
    years.forEach(year => {
        date_labels.forEach(month => {
            const key = `${month} ${year}`;
            if (!acc[key]) {
                acc[key] = {};
            }
        });
    });

    return acc;
}

let processedYears = $derived(buildProcessedYears(disaster));

export const allTypes = Array.from(
    new Set(
        Object.values(processedYears).flatMap(month => Object.keys(month))
    )
);

export function getProcessedYears(){
    // console.log("Processed Years:", processedYears);
    return processedYears;
}

// Transformed for LayerCake input (sorted + mapped)
let fullData = $derived(Object.entries(processedYears)
    .sort(([a], [b]) => {
      const [aMonth, aYear] = a.split(' ');
      const [bMonth, bYear] = b.split(' ');
      if (Number(aYear) !== Number(bYear)) return Number(aYear) - Number(bYear);
      return date_labels.indexOf(aMonth) - date_labels.indexOf(bMonth);
    })
    .map(([year, types]) => {
      const obj = {
        year: year,
        yearNumber: Number(year),
        ...types
      };
      allTypes.forEach(type => {
        if (obj[type] === undefined) {
          obj[type] = 0;
        }
      });
      return obj;
    }
  ));

export function getFullData() {
   return fullData;
}
// for CSMonths
let processedMonths = $derived(disaster.reduce((acc, item) => {
        if (!item.date || !item.type) return acc;

        const month = item.date.slice(5, 7); // "01"–"12"
        const type = item.type.trim();

        if (!acc[month]) {
            acc[month] = {};
        }
        const generalType = generalizeType(type);
        acc[month][generalType] = (acc[month][generalType] || 0) + 1;

        return acc;
    }, {}));

export function getProcessedMonths(){
    return processedMonths;
}



export const seriesNames = allTypes;
export const seriesColors = seriesNames.map(name => disasterColors[name] || '#95A5A6');
export const seriesCount = seriesNames.length;

// for RankingLocation (metric page)

function getRegion(region){
    const firstSpaceIndex = region.indexOf(' ');
    if (firstSpaceIndex !== -1) {
        return region.slice(0, firstSpaceIndex);
    } else {
        return region;
    }
}

export function getSortedRegionList(dropdown){
    const regionList = disaster.reduce((acc, item) => {
        if (!item.region) return acc;

        const region = getRegion(item.region);

        if (!acc[region]) {
            acc[region] = 0;
        }
        if (dropdown === generalizeType(item.type) || dropdown === "All Types"){
            acc[region] = (acc[region] || 0) + 1;
        } 
        return acc;
    }, {});

    return Object.entries(regionList).sort(([, v1], [, v2]) => v2 - v1);
}


// for RankingDisaster (metric page)

export const regionList = ["I","II","III","IV-A","IV-B","V","VI","VII","VIII","IX",
                        "X","XI","XII","XIII","NCR","CAR","BARMM"];

export function getSortedDisasterList(dropdown){
    const disasterList = disaster.reduce((acc, item) => {
        if (!item.type) return acc;

        const type = generalizeType(item.type);
        const casualties = parseInt(item.casualties) || 0;
        if (!acc[type]) {
            acc[type] = 0;
        }
        if (dropdown === getRegion(item.region) || dropdown === "All Regions"){
            acc[type] = (acc[type] || 0) + casualties;
        } 
        return acc;
    }, {});

    return Object.entries(disasterList).sort(([, v1], [, v2]) => v2 - v1);
}

// for Line (Casualty, Injured, Missing)

export function getVictimCount(dropdown) {
  const acc = {};
  let totals = { casualty: 0, miss: 0, injury: 0 };

  allYears.forEach(year => {
    acc[year] = { casualty: 0, miss: 0, injury: 0 };
  });

  const data = disaster.reduce((acc, item) => {
    if (!item.date) return acc;

    const year = item.date.slice(0, 4);
    const typeMatches = dropdown === "All Types" || dropdown === generalizeType(item.type);

    if (typeMatches) {
        const c = parseInt(item.casualties) || 0;
        const m = parseInt(item.missing) || 0;
        const i = parseInt(item.injured) || 0;

        acc[year].casualty += c;
        acc[year].miss += m;
        acc[year].injury += i;

        totals.casualty += c;
        totals.miss += m;
        totals.injury += i;
    }

    return acc;
  }, acc); 
  return {data, totals}
}

/* export const allYears = (disaster.reduce((acc, item) => {
    if (!item.date ) return acc;

    const year = item.date.slice(0, 4); 
    if (!acc) {
      acc = [];
    }
    if (!acc.includes(year)) {
        acc.push(year);
    }

    return acc;
  }, [])).sort(([a], [b]) => a - b); */

  export const allYears = Array.from(
        new Set(
            disaster
                .filter(item => item.date)
                .map(item => item.date.slice(0, 4))
        )
    ).sort((a, b) => Number(a) - Number(b));
