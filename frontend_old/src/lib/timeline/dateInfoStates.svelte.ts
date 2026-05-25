let showInfo = $state(false);
let dateToShow: string | null = $state(null);

function getShowInfo() {
    return showInfo;
}

function setShowInfo(value: boolean) {
    showInfo = value;
}

function getDateToShow() {
    return dateToShow;
}

function setDateToShow(value: string) {
    dateToShow = value;
}

export { getShowInfo, getDateToShow, setShowInfo, setDateToShow }