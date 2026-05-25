let showInfo = $state(false);
let locationToShow: string | null = $state(null);

function getShowInfo() {
    return showInfo;
}

function setShowInfo(value: boolean) {
    showInfo = value;
}

function getLocationToShow() {
    return locationToShow;
}

function setLocationToShow(value: string) {
    locationToShow = value;
}

export { getShowInfo, getLocationToShow, setShowInfo, setLocationToShow }