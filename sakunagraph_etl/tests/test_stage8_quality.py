from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from sakunagraph_etl.cli import COMMANDS
from sakunagraph_etl.config import DeploymentProfile, load_settings
from sakunagraph_etl.quality import (
    FieldSchema,
    QualityGateError,
    QualityPolicy,
    TableSchema,
    ValueKind,
    enforce_production_quality,
    validate_table,
)
from sakunagraph_etl.quality.cli import main as quality_main
from sakunagraph_etl.quality.contracts import validate_source_input
from sakunagraph_etl.sources.ndrrmc import job as ndrrmc_job


class Stage8SchemaTests(unittest.TestCase):
    def test_contract_detects_schema_null_and_datatype_failures(self) -> None:
        schema = TableSchema(
            source="fixture",
            table="events",
            fields=(
                FieldSchema("id", ValueKind.STRING, nullable=False),
                FieldSchema("count", ValueKind.INTEGER, nullable=False),
            ),
        )

        report = validate_table(
            [{"id": "one", "count": "not-an-integer", "surprise": True}],
            schema,
        )

        self.assertEqual(report.status, "FAILED")
        self.assertEqual(report.rejected_count, 1)
        self.assertEqual(report.unexpected_columns, ("surprise",))
        self.assertIn(
            "INVALID_DATATYPE",
            {issue.code for issue in report.rejection_reasons},
        )
        self.assertIn(
            "UNEXPECTED_COLUMN",
            {issue.code for issue in report.rejection_reasons},
        )

    def test_rejection_threshold_can_accept_a_bounded_number_of_rows(self) -> None:
        schema = TableSchema(
            source="fixture",
            table="events",
            fields=(FieldSchema("id", ValueKind.STRING, nullable=False),),
            allow_unexpected_columns=True,
        )
        policy = QualityPolicy(
            minimum_records=1,
            maximum_rejected_records=1,
            maximum_rejected_ratio=0.5,
        )

        report = validate_table([{"id": "valid"}, {"id": None}], schema, policy=policy)

        self.assertEqual(report.status, "PASSED")
        self.assertEqual(report.accepted_count, 1)
        self.assertEqual(report.rejected_count, 1)
        self.assertIn("NULL_REQUIRED_VALUE", {item.code for item in report.rejection_reasons})

    def test_production_profiles_enforce_failed_reports(self) -> None:
        schema = TableSchema(
            source="fixture",
            table="empty",
            fields=(FieldSchema("id", nullable=False),),
        )
        report = validate_table([], schema)

        enforce_production_quality(report, DeploymentProfile.LOCAL)
        for profile in (DeploymentProfile.ONPREM, DeploymentProfile.CLOUD):
            with self.subTest(profile=profile), self.assertRaises(QualityGateError):
                enforce_production_quality(report, profile)


class Stage8SourceContractTests(unittest.TestCase):
    def test_event_directory_contract_rejects_missing_and_malformed_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid"
            valid.mkdir()
            (valid / "metadata.json").write_text(
                json.dumps(
                    {
                        "eventName": "Fixture Typhoon",
                        "startDate": "2026-01-02",
                        "id": "fixture-id",
                    }
                ),
                encoding="utf-8",
            )
            malformed = root / "malformed"
            malformed.mkdir()
            (malformed / "metadata.json").write_text("{", encoding="utf-8")
            (root / "missing").mkdir()

            report = validate_source_input("ndrrmc", root)

        self.assertEqual(report.record_count, 3)
        self.assertEqual(report.accepted_count, 1)
        self.assertEqual(report.rejected_count, 2)
        self.assertEqual(report.status, "FAILED")
        self.assertIn("NULL_REQUIRED_VALUE", {item.code for item in report.rejection_reasons})

    def test_onprem_source_job_stops_before_mapping_invalid_parsed_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event = root / "invalid-event"
            event.mkdir()
            (event / "metadata.json").write_text("{}", encoding="utf-8")
            settings = load_settings(
                "onprem",
                environ={
                    "SAKUNA_REPOSITORY_ROOT": str(root),
                    "SAKUNA_DATA_ROOT": str(root / "data"),
                    "SAKUNA_LOGS_ROOT": str(root / "logs"),
                },
            )

            with self.assertRaises(QualityGateError):
                ndrrmc_job.run(
                    data_dir=root,
                    out_dir=root / "rdf",
                    settings=settings,
                )

    def test_quality_cli_writes_a_machine_readable_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema_path = root / "schema.json"
            input_path = root / "rows.json"
            report_path = root / "report.json"
            schema_path.write_text(
                json.dumps(
                    TableSchema(
                        source="fixture",
                        table="events",
                        fields=(FieldSchema("id", ValueKind.STRING, nullable=False),),
                    ).to_dict()
                ),
                encoding="utf-8",
            )
            input_path.write_text('[{"id": "event-1"}]', encoding="utf-8")

            with contextlib.redirect_stdout(io.StringIO()):
                exit_code = quality_main(
                    [
                        "table",
                        "--schema",
                        str(schema_path),
                        "--input",
                        str(input_path),
                        "--format",
                        "json",
                        "--report",
                        str(report_path),
                    ]
                )
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(report["status"], "PASSED")
        self.assertEqual(COMMANDS["quality"].module, "sakunagraph_etl.quality.cli")


if __name__ == "__main__":
    unittest.main()
