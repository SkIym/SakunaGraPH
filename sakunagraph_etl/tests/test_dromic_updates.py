from datetime import datetime
import json
import os
from pathlib import Path
import tempfile
import unittest

from sakunagraph_etl.sources.dromic.document_conversion import copy_pdfs_jsons
from sakunagraph_etl.sources.dromic.fetch import should_fetch_post
from sakunagraph_etl.sources.dromic.identity import (
    canonical_event_id,
    canonical_post_url,
    legacy_event_id,
    report_version_id,
)
from sakunagraph_etl.sources.dromic.manifest import (
    Manifest,
    ManifestEntry,
    entries_for_post_url,
    latest_entry_for_filename,
    load_manifest,
    save_manifest,
    source_version_for_filename,
)
from sakunagraph_etl.sources.dromic.state import (
    DromicStateStore,
    EventStatus,
    EventStatusRecord,
)
from sakunagraph_etl.sources.dromic.versioning import select_latest_report_folders


class DromicFetchUpdateTests(unittest.TestCase):
    def test_fetch_eligibility_depends_only_on_post_date(self) -> None:
        cutoff = datetime(2026, 7, 20)

        self.assertTrue(should_fetch_post(datetime(2026, 7, 21), cutoff))
        self.assertFalse(should_fetch_post(datetime(2026, 7, 20), cutoff))
        self.assertFalse(should_fetch_post(datetime(2026, 7, 19), cutoff))

    def test_manifest_retains_revisions_from_the_same_post(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.json"
            post_url = "https://dromic.dswd.gov.ph/effects-of-typhoon-uwan/"
            manifest = Manifest(
                last_scrape_date="2026-07-23, 18:00:00",
                entries=[
                    ManifestEntry(
                        filename="uwan-90.pdf",
                        download_url="https://example.test/uwan-90.pdf",
                        downloaded_at="2026-07-20T00:00:00Z",
                        post_url=post_url,
                        page=1,
                        post_date="2026-07-20",
                    ),
                    ManifestEntry(
                        filename="uwan-91.pdf",
                        download_url="https://example.test/uwan-91.pdf",
                        downloaded_at="2026-07-23T00:00:00Z",
                        post_url=post_url,
                        page=1,
                        post_date="2026-07-23",
                    ),
                ],
            )

            save_manifest(path, manifest)
            loaded = load_manifest(path)

            self.assertEqual(
                [entry.filename for entry in loaded.entries],
                ["uwan-90.pdf", "uwan-91.pdf"],
            )

    def test_latest_same_filename_manifest_entry_wins(self) -> None:
        payload = {
            "entries": [
                {"filename": "report.pdf", "post_url": "https://example.test/old"},
                {"filename": "report.pdf", "post_url": "https://example.test/new"},
            ]
        }

        entry = latest_entry_for_filename(payload, "report.pdf")

        self.assertIsNotNone(entry)
        self.assertEqual(entry["post_url"], "https://example.test/new")
        self.assertEqual(
            source_version_for_filename(
                {
                    "entries": [
                        {
                            "filename": "report.pdf",
                            "downloaded_at": "2026-07-20T00:00:00Z",
                        },
                        {
                            "filename": "report.pdf",
                            "downloaded_at": "2026-07-21T00:00:00Z",
                        },
                    ]
                },
                "report.pdf",
            ),
            "2026-07-21T00:00:00Z",
        )

    def test_post_history_uses_canonical_url_variants(self) -> None:
        payload = {
            "entries": [
                {
                    "filename": "report-1.pdf",
                    "post_url": "http://www.dromic.dswd.gov.ph/event?old=1",
                },
                {
                    "filename": "report-2.pdf",
                    "post_url": "https://dromic.dswd.gov.ph/event/",
                },
                {
                    "filename": "other.pdf",
                    "post_url": "https://dromic.dswd.gov.ph/other/",
                },
            ]
        }

        versions = entries_for_post_url(
            payload,
            "https://dromic.dswd.gov.ph/event/",
        )

        self.assertEqual(
            [entry["filename"] for entry in versions],
            ["report-1.pdf", "report-2.pdf"],
        )


class DromicCanonicalIdentityTests(unittest.TestCase):
    def test_post_url_is_canonicalized_before_event_identity(self) -> None:
        first = canonical_event_id(
            "http://WWW.dromic.dswd.gov.ph/example-event?download=1#report",
            event_name="Old title",
            start_date="2026-01-01",
        )
        second = canonical_event_id(
            "https://dromic.dswd.gov.ph/example-event/",
            event_name="Renamed event",
            start_date="2026-01-02",
        )

        self.assertEqual(
            canonical_post_url(
                "http://WWW.dromic.dswd.gov.ph/example-event?download=1#report"
            ),
            "https://dromic.dswd.gov.ph/example-event/",
        )
        self.assertEqual(first, second)

    def test_missing_post_url_preserves_legacy_identity(self) -> None:
        expected = legacy_event_id("Fixture Flood", "2026-01-02")

        actual = canonical_event_id(
            None,
            event_name="Fixture Flood",
            start_date="2026-01-02",
        )

        self.assertEqual(actual, expected)

    def test_report_versions_are_content_addressed_within_the_post_series(self) -> None:
        first = report_version_id(
            "https://dromic.dswd.gov.ph/example-event/",
            "report.pdf",
            sha256="a" * 64,
            downloaded_at="2026-07-20T00:00:00Z",
        )
        repeated = report_version_id(
            "http://www.dromic.dswd.gov.ph/example-event?download=1",
            "renamed.pdf",
            sha256="a" * 64,
            downloaded_at="2026-07-21T00:00:00Z",
        )
        updated = report_version_id(
            "https://dromic.dswd.gov.ph/example-event/",
            "report.pdf",
            sha256="b" * 64,
            downloaded_at="2026-07-21T00:00:00Z",
        )

        self.assertEqual(first, repeated)
        self.assertNotEqual(first, updated)


class DromicLatestFactsTests(unittest.TestCase):
    def test_only_latest_parsed_folder_is_selected_for_each_post(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixtures = {
                "old": {
                    "reportName": "report-1.pdf",
                    "reportLink": "http://www.dromic.dswd.gov.ph/event",
                    "postDate": "2026-07-20",
                    "obtainedDate": "2026-07-20T08:00:00Z",
                },
                "latest": {
                    "reportName": "report-2.pdf",
                    "reportLink": "https://dromic.dswd.gov.ph/event/",
                    "postDate": "2026-07-23",
                    "obtainedDate": "2026-07-23T08:00:00Z",
                },
                "unrelated": {
                    "reportName": "other.pdf",
                    "reportLink": "https://dromic.dswd.gov.ph/other/",
                    "postDate": "2026-07-21",
                },
            }
            for folder, source in fixtures.items():
                target = root / folder
                target.mkdir()
                (target / "source.json").write_text(
                    json.dumps(source),
                    encoding="utf-8",
                )

            selected, superseded = select_latest_report_folders(
                root,
                list(fixtures),
            )

            self.assertEqual(selected, ["latest", "unrelated"])
            self.assertEqual(superseded, 1)


class DromicConversionRefreshTests(unittest.TestCase):
    def test_updated_pdf_and_manifest_replace_conversion_copies(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "source"
            destination = root / "destination"
            source.mkdir()
            destination.mkdir()

            source_pdf = source / "report.pdf"
            destination_pdf = destination / "report.pdf"
            source_pdf.write_bytes(b"new")
            destination_pdf.write_bytes(b"old")
            os.utime(destination_pdf, (1, 1))
            os.utime(source_pdf, (2, 2))

            source_manifest = source / "manifest.json"
            destination_manifest = destination / "manifest.json"
            source_manifest.write_text(
                json.dumps({"entries": [{"filename": "new.pdf"}]}),
                encoding="utf-8",
            )
            destination_manifest.write_text(
                json.dumps({"entries": [{"filename": "old.pdf"}]}),
                encoding="utf-8",
            )

            copy_pdfs_jsons(source, destination)

            self.assertEqual(destination_pdf.read_bytes(), b"new")
            self.assertEqual(
                json.loads(destination_manifest.read_text(encoding="utf-8")),
                {"entries": [{"filename": "new.pdf"}]},
            )


class DromicParserVersionStateTests(unittest.TestCase):
    def test_same_filename_is_current_only_for_recorded_acquisition_version(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = DromicStateStore(Path(temp) / "2026")
            store.update([
                EventStatusRecord.create(
                    "report",
                    EventStatus.PARSED,
                    "dromic-parser",
                    source_filename="report.pdf",
                    source_version="2026-07-20T00:00:00Z",
                )
            ])
            manifest = store.load()

            self.assertTrue(
                store.source_is_current(
                    "report",
                    producer="dromic-parser",
                    source_filename="report.pdf",
                    source_version="2026-07-20T00:00:00Z",
                    manifest=manifest,
                )
            )
            self.assertFalse(
                store.source_is_current(
                    "report",
                    producer="dromic-parser",
                    source_filename="report.pdf",
                    source_version="2026-07-21T00:00:00Z",
                    manifest=manifest,
                )
            )


class DromicPublicationSafetyTests(unittest.TestCase):
    def test_active_dromic_graph_refuses_append_publication(self) -> None:
        from sakunagraph_etl.io import graphdb

        with tempfile.TemporaryDirectory() as temp:
            rdf_root = Path(temp)
            dromic_dir = rdf_root / "events" / "dromic"
            dromic_dir.mkdir(parents=True)
            (dromic_dir / "dromic-2026.ttl").write_text(
                "<https://sakuna.ph/event> a <https://sakuna.ph/Event> .\n",
                encoding="utf-8",
            )

            result = graphdb.main([
                "--rdf-root",
                str(rdf_root),
                "--scope",
                "events",
            ])

        self.assertEqual(result, 2)

    def test_replacement_includes_all_year_files_in_shared_dromic_graph(self) -> None:
        from sakunagraph_etl.io import graphdb

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            dromic_dir = root / "events" / "dromic"
            artifact_dir = root / "artifact"
            dromic_dir.mkdir(parents=True)
            artifact_dir.mkdir()
            old_year = dromic_dir / "dromic-2025.ttl"
            local_current = dromic_dir / "dromic-2026.ttl"
            artifact_current = artifact_dir / "dromic-2026.ttl"
            for path in (old_year, local_current, artifact_current):
                path.write_text("", encoding="utf-8")

            targets = graphdb.complete_dromic_replacement_targets(
                [
                    graphdb.LoadTarget(
                        artifact_current,
                        graphdb.DROMIC_GRAPH_IRI,
                    )
                ],
                root,
            )

        self.assertEqual(
            {target.path for target in targets},
            {old_year.resolve(), artifact_current},
        )


if __name__ == "__main__":
    unittest.main()
