import json
from pathlib import Path
import tempfile
import unittest

from sakunagraph_etl.sources.dromic.quality import (
    check_year,
    discover_year_directories,
    duplicate_csv_names,
)
from sakunagraph_etl.sources.dromic.state import DromicStateStore, EventStatus


class CheckFailsTests(unittest.TestCase):
    def test_only_numbered_copies_with_an_original_are_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            event = Path(temp)
            for filename in (
                "damaged_houses.csv",
                "damaged_houses_1.csv",
                "affected_2026.csv",
                "roads_2.csv",
            ):
                (event / filename).touch()

            self.assertEqual(duplicate_csv_names(event), ["damaged_houses_1.csv"])

    def test_all_year_discovery_ignores_non_year_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "2025").mkdir()
            (root / "2018").mkdir()
            (root / "archive").mkdir()

            years = discover_year_directories(root, single_directory=False)

            self.assertEqual([year.name for year in years], ["2018", "2025"])

    def test_check_year_replaces_both_control_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            year = Path(temp) / "2026"
            duplicate_event = year / "Duplicate event"
            clean_event = year / "Clean event"
            duplicate_event.mkdir(parents=True)
            clean_event.mkdir()
            (duplicate_event / "damaged_houses.csv").touch()
            (duplicate_event / "damaged_houses_1.csv").touch()
            (clean_event / "assistance_2026.csv").touch()
            (clean_event / "source.json").write_text(
                json.dumps({"reportName": "clean-report.docx"}),
                encoding="utf-8",
            )
            (year / "_needs_rerun.txt").write_text("stale\n", encoding="utf-8")
            (year / "_parsed.txt").write_text("stale.pdf\n", encoding="utf-8")

            flagged, checked = check_year(year)

            self.assertEqual((flagged, checked), (1, 2))
            self.assertEqual(
                (year / "_needs_rerun.txt").read_text(encoding="utf-8"),
                "Duplicate event\n",
            )
            self.assertEqual(
                (year / "_parsed.txt").read_text(encoding="utf-8"),
                "clean-report.pdf\n",
            )
            manifest = DromicStateStore(year).load()
            duplicate_status = manifest.events["Duplicate event"]["dromic-quality"]
            clean_status = manifest.events["Clean event"]["dromic-quality"]
            self.assertEqual(duplicate_status.status, EventStatus.DUPLICATE_CSV)
            self.assertIn("damaged_houses_1.csv", duplicate_status.reason or "")
            self.assertEqual(clean_status.status, EventStatus.PARSED)
            self.assertEqual(clean_status.source_filename, "clean-report.pdf")


if __name__ == "__main__":
    unittest.main()
