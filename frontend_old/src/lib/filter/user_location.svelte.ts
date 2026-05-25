/* 
    note that data.localityInfo.administrative objects have the following mappings
    for adminLevel property:
        3: region
        4: province
    and that municipality/city can be found via the locality property of data
*/

let userLocation = $state("");

function getUserLocation() {
    return userLocation;
}

function getLocation() {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(success, error);
    } else {
        console.log("Geolocation is not supported by this browser.");
    }
}

async function getLocationName(lat: string, long: string) {
    const resp = await fetch('https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=' + lat + '&longitude=' + long + '&localityLanguage=en');

    if (!resp.ok) {
        throw new Error(`HTTP error! status: ${resp.status}`);
    }

    const data = await resp.json();

    if (data.status) {
        console.log("Reverse Geocoding: Error Code " + data.status + " (" + data.description + ")");

        if (data.status == 500) {
            console.log("Reverse Geocoding: Trying again...")
            getLocationName(lat, long);
        }

        return;
    } else {
        let level3Objects = data.localityInfo.administrative.filter((a) => a.adminLevel == 3);
        let level4Objects = data.localityInfo.administrative.filter((a) => a.adminLevel == 4);

        let municity = data.locality;
        let province = level4Objects.length ? 
                            level4Objects[0].name 
                            : level3Objects.length > 1 ? 
                                level3Objects.filter((o) => !o.description.includes("region"))[0].name
                                : "";
        let region = level3Objects.filter((o) => o.description.includes("region"))[0].name;
        let res = municity + ", " + province + ", " + region;

        console.log(data);
        console.log("Reverse Geocoding: returned location is " + res);
        userLocation = res;
    }
}

function success(position) {
    let lat = position.coords.latitude;
    let long = position.coords.longitude;
    console.log("Latitude: " + lat + " Longitude: " + long);

    getLocationName(lat, long);
}

function error() {
  console.log("Sorry, no position available.");
}

export { getLocation, getUserLocation }