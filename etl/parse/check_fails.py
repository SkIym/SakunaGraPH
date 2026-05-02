"""
check_parsed.py — Quality check for parsed DROMIC event folders.
Lists subfolders that contain a CSV with a number in its filename
(e.g. table_1.csv, number_of_affected_1.csv) which indicates a
duplicate/split table that needs reprocessing.

Usage:
    python check_parsed.py --year 2018
    python check_parsed.py --dir ../data/parsed/dromic/2018
"""

import argparse
import json
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default=None)
    parser.add_argument("--dir",  default=None)
    args = parser.parse_args()

    base = (
        Path(args.dir) if args.dir
        else Path(f"../data/parsed/dromic/{args.year}")
    )

    if not base.exists():
        print(f"[ERROR] Directory not found: {base}")
        return

    subfolders = sorted(p for p in base.iterdir() if p.is_dir())
    print(f"Checking {len(subfolders)} folders in {base}\n")

    flagged: list[tuple[str, list[str]]] = []

    already_parsed: list[str] = []

    for folder in subfolders:
        numbered = [
            c.name for c in folder.glob("*.csv")
            if re.search(r'_\d+\.csv$', c.name)
        ]
        if numbered:
            flagged.append((folder.name, numbered))

        source_path = folder / "source.json"
        if source_path.exists():
            try:
                with open(source_path, "r", encoding="utf-8") as s:
                    data = json.load(s)

                filename: str = data.get("reportName")
                if "pdf" in filename:

                    filename = filename.replace(".docx", "")
                    filename = filename.replace(".doc", "")
                else:
                    filename = filename.replace(".docx", ".pdf")
                    filename = filename.replace(".doc", ".pdf")

                if filename:
                    already_parsed.append(filename)

            except Exception as e:
                print(f"[WARN] Failed reading {source_path}: {e}")
                

    if flagged:
        print(f"{'='*60}")
        print(f"NEEDS RERUN ({len(flagged)} folders with numbered CSVs):")
        print(f"{'='*60}")
        for folder_name, csvs in flagged:
            print(f"  {folder_name}")
            for c in csvs:
                print(f"      {c}")

        rerun_path = base / "_needs_rerun.txt"
        rerun_path.write_text(
            "\n".join(f for f, _ in flagged), encoding="utf-8"
        )
        print(f"\nRerun list written to: {rerun_path}")
    else:
        print("All folders look clean - no numbered duplicate CSVs found.")

    if already_parsed:
        src_out = base / "_parsed.txt"
        src_out.write_text("\n".join(already_parsed), encoding="utf-8")
        print(f"Source filenames written to: {src_out}")

    print(f"\nSummary: {len(flagged)} folders flagged / {len(subfolders)} total")


if __name__ == "__main__":
    main()