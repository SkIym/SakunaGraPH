let searchTermLocation = $state("");
let searchTermDisaster = $state("");

let searchStates = {
    get location() {
        return searchTermLocation;
    },
    get disaster() {
        return searchTermDisaster;
    },
    set location(value: string) {
        searchTermLocation = value;
    },
    set disaster(value: string) {
        searchTermDisaster = value;
    }
};

export { searchStates };