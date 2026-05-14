let selectedAdminLevel = $state('regions');

let mapJoinKey = $derived.by(() =>
    {
        if (selectedAdminLevel === 'regions') {
            return 'name';
        } else if (selectedAdminLevel === 'provinces') {
            return 'PROVINCE';
        } else if (selectedAdminLevel === 'municipalities_cities') {
            return 'CITY_MUNICIPALITY';
        } else {
            return 'name';
        }
    }
);

let locationFullNameKeys = $derived.by(() =>
    {
        if (selectedAdminLevel === 'regions') {
            return ['name'];
        } else if (selectedAdminLevel === 'provinces') {
            return ['PROVINCE', 'REGION'];
        } else if (selectedAdminLevel === 'municipalities_cities') {
            return ['CITY_MUNICIPALITY', 'PROVINCE', 'REGION'];
        } else {
            return ['name'];
        }
    }
);

function getSelectedAdminLevel() {
    return selectedAdminLevel;
}

function setSelectedAdminLevel(value: string) {
    selectedAdminLevel = value;
}

function getMapJoinKey() {
    return mapJoinKey;
}

function getLocationFullNameKeys() {
    return locationFullNameKeys;
}

function getHoveredLocation(feature: object) {
    return getLocationFullNameKeys().reduce((acc, curr) => {return acc + feature[curr] + ', '}, '').slice(0, -2);
}

export { getSelectedAdminLevel, setSelectedAdminLevel, getMapJoinKey, getLocationFullNameKeys, getHoveredLocation }
