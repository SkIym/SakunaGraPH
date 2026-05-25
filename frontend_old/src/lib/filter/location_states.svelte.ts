import { locationsArray, Location } from './location_arrays';
import { searchStates } from './search_states.svelte';

let locations = $state(locationsArray);

let searchedLocations = $derived.by(() =>
  {
    let locs = searchStates.location != undefined ? locations.filter((item) =>
      item.name.toLowerCase().indexOf(searchStates.location?.toLowerCase()) !== -1
    ) : locations;

    if (searchStates.location != undefined) {
      for (let loc of [...locs]) {
        if (loc.type == 'province') {
          let province = locations[loc.parent];

          if (!locs.includes(province)) {
            locs = [...locs.slice(0, locs.indexOf(loc)), province, ...locs.slice(locs.indexOf(loc))];
            province.expand = true;
          }
        } else if (loc.type == 'municity') {
          let province = locations[loc.parent];
          let region = locations[province.parent];

          if (!locs.includes(province)) {
            locs = [...locs.slice(0, locs.indexOf(loc)), province, ...locs.slice(locs.indexOf(loc))];
            province.expand = true;
          }

          if (!locs.includes(region)) {
            locs = [...locs.slice(0, locs.indexOf(province)), region, ...locs.slice(locs.indexOf(province))];
            region.expand = true;
          }
        }
      }
    }

    return locs;
  }
);

let showableLocations = $derived(
searchedLocations.filter((item) =>
    (item.type == "region" || 
    (item.type == "province" && locations[item.parent].expand) || 
    (item.type == "municity" && locations[item.parent].expand && locations[locations[item.parent].parent].expand))
)
);

let selectedLocations = $derived(
    locations.filter((item) =>item.checked)
);

let checkedLocations = $derived(
    selectedLocations.filter((item) => 
    (item.type == "region" ||
    (item.type == "province" && !locations[item.parent].checked) ||
    (item.type == "municity" && !locations[item.parent].checked && !locations[locations[item.parent].parent].checked)))
);

function setSelectedLocations(locations: Location[]) {
    selectedLocations = locations;
}

function getShowableLocations() {
    return showableLocations;
}

function getSelectedLocations() {
    return selectedLocations;
}

function getCheckedLocations() {
    return checkedLocations;
}

function findLocationWithName(arr: Location[], name: string): Location[] {
  let res = arr.filter((location) => location.name == name);
  if (!res.length) {
    res = arr;
  }
  return res;
}

function findLocationWithFullName(name: string): Location[] {
  return locations.filter((location) => location.fullname == name);
}

function findLocation(fullname: string) {
  // try full name first
  let possibleLocations = findLocationWithFullName(fullname);

  if (possibleLocations.length) {
    if (possibleLocations.length == 1) {
      return possibleLocations[0];
    }
  }else {
    // now try every level
    let separatedNames = fullname.split(", ");
    let municity = separatedNames[0];
    let province = separatedNames[1];
    let region = separatedNames[2];

    let sameRegion = findLocationWithName(locations, region);
    //console.log("with the same region: " + sameRegion.map((same) => same.name));
    let sameProvince = findLocationWithName((sameRegion.flatMap((location) => location.children.map((child) => locations[child]))), province);
    //console.log("with the same province: " + sameProvince.map((same) => same.name));
    let sameMunicity = findLocationWithName((sameProvince.flatMap((location) => location.children.map((child) => locations[child]))), municity);

    console.log(sameMunicity);

    if (sameMunicity.length == 1) {
        return sameMunicity[0];
    }
  }
}

export { locations, getShowableLocations, getSelectedLocations, getCheckedLocations, setSelectedLocations, findLocation };