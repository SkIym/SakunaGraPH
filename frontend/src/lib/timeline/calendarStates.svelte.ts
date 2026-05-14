let selectedYear = $state(new Date().getFullYear());
let selectedMonth = $state(new Date().getMonth());
let selectedDay = $state(new Date().getDate());

function getSelectedYear() {
  return selectedYear;
}

function getSelectedMonth() {
  return selectedMonth;
}

function getSelectedDay() {
  return selectedDay;
}

function setSelectedYear(year: number) {
  selectedYear = year;
}

function setSelectedMonth(month: number) {
  selectedMonth = month;
}

function setSelectedDay(day: number) {
  selectedDay = day;
}

export { getSelectedYear, getSelectedMonth, getSelectedDay, setSelectedYear, setSelectedMonth, setSelectedDay };