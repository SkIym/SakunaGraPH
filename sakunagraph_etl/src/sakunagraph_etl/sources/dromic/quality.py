"""Quality checks for parsed DROMIC event folders.

An event folder needs rerunning only when it contains both an original CSV and
a numbered copy of that CSV, for example ``damaged_houses.csv`` and
``damaged_houses_1.csv``. A filename that merely contains or ends in a number
is not considered a failure when its unnumbered counterpart does not exist.

Run from the standalone project::

    sakuna-etl check-dromic --all
    sakuna-etl check-dromic --year 2018
    sakuna-etl check-dromic --dir ../data/parsed/dromic/2018

With no selection argument, the script checks all year directories.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


from sakunagraph_etl.config import SETTINGS
from .state import DromicStateStore, EventStatus, EventStatusRecord


DEFAULT_DROMIC_DIR = SETTINGS.paths.parsed_root / "dromic"
NUMBERED_COPY_PATTERN = re.compile(r"^(?P<base>.+)_(?P<number>\d+)$")


def duplicate_csv_names(folder: Path) -> list[str]:
    """Return numbered CSVs whose unnumbered CSV exists in the same folder."""
    csv_files = sorted(
        (
            path
            for path in folder.iterdir()
            if path.is_file() and path.suffix.lower() == ".csv"
        ),
        key=lambda path: path.name.casefold(),
    )
    names = {path.name.casefold() for path in csv_files}
    duplicates: list[str] = []

    for csv_path in csv_files:
        match = NUMBERED_COPY_PATTERN.fullmatch(csv_path.stem)
        if match is None:
            continue

        original_name = f"{match.group('base')}.csv"
        if original_name.casefold() in names:
            duplicates.append(csv_path.name)

    return duplicates


def source_filename(folder: Path) -> str | None:
    """Return the source PDF name recorded for one parsed event folder."""
    source_path = folder / "source.json"
    if not source_path.exists():
        return None

    try:
        with source_path.open("r", encoding="utf-8") as source:
            data = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        print(f"[WARN] Failed reading {source_path}: {error}")
        return None

    filename = data.get("reportName")
    if not isinstance(filename, str) or not filename:
        return None

    if "pdf" in filename.lower():
        return re.sub(r"\.(?:docx|doc)$", "", filename, flags=re.IGNORECASE)
    return re.sub(r"\.(?:docx|doc)$", ".pdf", filename, flags=re.IGNORECASE)


def discover_year_directories(base: Path, *, single_directory: bool) -> list[Path]:
    """Resolve either one explicit year directory or all years below a root."""
    if not base.exists():
        raise FileNotFoundError(base)
    if not base.is_dir():
        raise NotADirectoryError(base)

    if single_directory:
        return [base]

    years = sorted(
        (path for path in base.iterdir() if path.is_dir() and path.name.isdigit()),
        key=lambda path: int(path.name),
    )
    if not years:
        raise FileNotFoundError(f"No year subdirectories found under {base}")
    return years


def write_lines(path: Path, values: list[str]) -> None:
    """Replace a generated list file, including clearing stale entries."""
    content = "\n".join(values)
    if content:
        content += "\n"
    path.write_text(content, encoding="utf-8")


def check_year(year_dir: Path) -> tuple[int, int]:
    """Check one parsed year and update its rerun and parsed-source lists."""
    event_folders = sorted(
        (path for path in year_dir.iterdir() if path.is_dir()),
        key=lambda path: path.name.casefold(),
    )
    print(f"\n[{year_dir.name}] Checking {len(event_folders)} event folders in {year_dir}")

    flagged: list[tuple[str, list[str]]] = []
    records: list[EventStatusRecord] = []

    for folder in event_folders:
        duplicates = duplicate_csv_names(folder)
        if duplicates:
            flagged.append((folder.name, duplicates))

        parsed_filename = source_filename(folder)
        records.append(
            EventStatusRecord.create(
                folder.name,
                EventStatus.DUPLICATE_CSV if duplicates else EventStatus.PARSED,
                "dromic-quality",
                reason=(
                    "Numbered CSV duplicates: " + ", ".join(duplicates)
                    if duplicates
                    else "Parsed folder passed duplicate-output checks"
                ),
                source_filename=parsed_filename,
            )
        )

    if flagged:
        print(f"  NEEDS RERUN ({len(flagged)} folders with duplicate CSVs):")
        for folder_name, csvs in flagged:
            print(f"    {folder_name}")
            for csv_name in csvs:
                print(f"      {csv_name}")
    else:
        print("  All folders look clean - no duplicate CSVs found.")

    DromicStateStore(year_dir).update(records)
    rerun_path = year_dir / "_needs_rerun.txt"
    print(f"  Rerun list updated: {rerun_path}")

    parsed_path = year_dir / "_parsed.txt"
    print(f"  Source filenames updated: {parsed_path}")

    return len(flagged), len(event_folders)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find parsed DROMIC events containing duplicate CSV outputs."
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--all", action="store_true", help="Check every parsed year")
    selection.add_argument("--year", help="Check one year under data/parsed/dromic")
    selection.add_argument(
        "--dir",
        type=Path,
        help="Check one explicit year directory",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.year:
        base = DEFAULT_DROMIC_DIR / args.year
        single_directory = True
    elif args.dir:
        base = args.dir
        single_directory = True
    else:
        base = DEFAULT_DROMIC_DIR
        single_directory = False

    try:
        year_directories = discover_year_directories(
            base,
            single_directory=single_directory,
        )
    except (FileNotFoundError, NotADirectoryError) as error:
        print(f"[ERROR] Directory not found: {error}")
        return 1

    total_flagged = 0
    total_folders = 0
    for year_dir in year_directories:
        flagged, checked = check_year(year_dir)
        total_flagged += flagged
        total_folders += checked

    print(
        f"\nSummary: {total_flagged} folders flagged / {total_folders} total "
        f"across {len(year_directories)} year(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
