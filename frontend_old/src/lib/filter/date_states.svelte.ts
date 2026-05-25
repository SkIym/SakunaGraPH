let _start: Date | undefined = $state(undefined);
let _end: Date | undefined = $state(undefined);

let dateRange = {
    get start() {
        return _start;
    },
    get end() {
        return _end;
    },
    set start(value) {
        _start = value;
    },
    set end(value) {
        _end = value;
    },
};

export {  dateRange };