"""Shared aliases for package-owned DROMIC parsed-data checks."""

from pathlib import Path

from sakunagraph_etl.sources.dromic.quality import (
    check_year,
    discover_year_directories,
    duplicate_csv_names,
)


def duplicate_dromic_csvs(event_directory: Path) -> list[str]:
    return duplicate_csv_names(event_directory)


def discover_dromic_years(root: Path) -> list[Path]:
    return discover_year_directories(root, single_directory=False)


def check_dromic_year(year_directory: Path) -> tuple[int, int]:
    return check_year(year_directory)


__all__ = ["check_dromic_year", "discover_dromic_years", "duplicate_dromic_csvs"]
