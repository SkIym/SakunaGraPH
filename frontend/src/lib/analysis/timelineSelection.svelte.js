class TimelineSelection {
	year = $state(null);
	month = $state(null);
	day = $state(null);
	showEvents = $state(false);

	get datePrefix() {
		if (!this.year) return '';
		if (!this.month) return String(this.year);
		const month = String(this.month).padStart(2, '0');
		if (!this.day) return `${this.year}-${month}`;
		return `${this.year}-${month}-${String(this.day).padStart(2, '0')}`;
	}

	setYear(value) {
		this.year = Number(value) || null;
		this.month = null;
		this.day = null;
		this.showEvents = false;
	}

	selectYear(value) {
		this.setYear(value);
		this.showEvents = true;
	}

	selectMonth(value) {
		if (!this.year) return;
		this.month = Number(value) || null;
		this.day = null;
		this.showEvents = true;
	}

	selectDay(value) {
		if (!this.year || !this.month) return;
		this.day = Number(value) || null;
		this.showEvents = true;
	}

	closeEvents() {
		this.showEvents = false;
	}

	reset() {
		this.year = null;
		this.month = null;
		this.day = null;
		this.showEvents = false;
	}
}

export const timelineSelection = new TimelineSelection();
