from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest

from openpyxl import Workbook
import polars as pl
from rdflib import Graph

from pipeline import run_emdat as legacy_job
from sakunagraph_etl.config import load_settings
from sakunagraph_etl.rdf.validation import ShaclValidationService
from sakunagraph_etl.sources.emdat import job
from sakunagraph_etl.sources.emdat.parse import ParsedEmdatWorkbook, parse_workbook
from sakunagraph_etl.sources.emdat.rdf import (
    AffectedPopulation,
    Casualties,
    DamageGeneral,
    Recovery,
)
from sakunagraph_etl.sources.emdat.transform import EmdatTransformer
from sakunagraph_etl.quality.shacl import ShaclValidator


FIXED_MTIME = 1_767_225_600
LOCATION_IRI = "https://sakuna.ph/psgc/city-of-manila"

HEADERS = (
    "DisNo.",
    "Event Name",
    "Disaster Subtype",
    "Associated Types",
    "Location",
    "Start Year",
    "Start Month",
    "Start Day",
    "End Year",
    "End Month",
    "End Day",
    "Entry Date",
    "Last Update",
    "AID Contribution ('000 US$)",
    "OFDA/BHA Response",
    "Magnitude",
    "Magnitude Scale",
    "Latitude",
    "Longitude",
    "Total Deaths",
    "No. Injured",
    "No. Affected",
    "No. Homeless",
    "Reconstruction Costs, Adjusted ('000 US$)",
    "Insured Damage, Adjusted ('000 US$)",
    "Total Damage, Adjusted ('000 US$)",
    "CPI",
)


def _row(*, include_impacts: bool) -> tuple[str, ...]:
    impact_values = (
        "2",
        "3",
        "100",
        "10",
        "500",
        "",
        "1000",
        "100",
    ) if include_impacts else ("", "", "", "", "", "", "", "")
    return (
        "2026-0001-PHL",
        "Fixture Flood",
        "Flood (general)",
        "",
        "Manila",
        "2026",
        "01",
        "02",
        "2026",
        "01",
        "03",
        "2026-01-04",
        "2026-01-05",
        "",
        "No",
        "",
        "",
        "",
        "",
        *impact_values,
    )


def _write_workbook(path: Path, *, include_impacts: bool = True) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "EM-DAT Data"
    sheet.append(HEADERS)
    sheet.append(_row(include_impacts=include_impacts))
    workbook.save(path)
    os.utime(path, (FIXED_MTIME, FIXED_MTIME))


def _canonicalize_location(*, df: pl.DataFrame, col: str, threshold: int) -> pl.DataFrame:
    assert col == "hasLocation"
    assert threshold == 80
    return df.with_columns(pl.lit(LOCATION_IRI).alias("hasLocation_iri"))


def _validation_service() -> ShaclValidationService:
    shapes = Graph().parse(
        data="""
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix skg: <https://sakuna.ph/ontology/> .
            [] a sh:NodeShape ;
               sh:targetSubjectsOf skg:eventName ;
               sh:property [ sh:path skg:eventName ; sh:minCount 1 ] .
        """,
        format="turtle",
    )
    return ShaclValidationService(ShaclValidator(shapes_graph=shapes))


class EmdatStage3Tests(unittest.TestCase):
    def test_parser_returns_typed_workbook_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "fixture.xlsx"
            _write_workbook(source)

            parsed = parse_workbook(source)

        self.assertIsInstance(parsed, ParsedEmdatWorkbook)
        self.assertEqual(parsed.row_count, 1)
        self.assertEqual(parsed.source_path.name, "fixture.xlsx")

    def test_location_only_impact_rows_are_not_transformed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "fixture.xlsx"
            _write_workbook(source, include_impacts=False)
            result = EmdatTransformer(_canonicalize_location).transform(parse_workbook(source))

        for impact_class in (Recovery, DamageGeneral, Casualties, AffectedPopulation):
            with self.subTest(impact_class=impact_class.__name__):
                self.assertEqual(result.entities[impact_class], [])

    def test_legacy_runner_delegates_to_packaged_job(self) -> None:
        self.assertIs(legacy_job.run, job.run)
        self.assertIs(legacy_job.main, job.main)

    def test_end_to_end_output_matches_golden_and_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "fixture.xlsx"
            output = root / "rdf" / "emdat.ttl"
            manifest = root / "logs" / "emdat.json"
            _write_workbook(source)
            settings = load_settings(
                environ={
                    "SAKUNA_REPOSITORY_ROOT": str(root),
                    "SAKUNA_DATA_ROOT": str(root / "data"),
                    "SAKUNA_LOGS_ROOT": str(root / "logs"),
                }
            )

            result = job.run(
                output,
                input_path=source,
                transformer=EmdatTransformer(_canonicalize_location),
                validation_service=_validation_service(),
                validate_output=True,
                manifest_path=manifest,
                settings=settings,
            )

            graph = Graph().parse(output, format="turtle")
            actual = job.canonical_rdf_bytes(graph).decode("utf-8")
            golden_path = Path(__file__).parent / "golden" / "emdat_fixture.nt"
            expected = golden_path.read_text(encoding="utf-8")
            manifest_data = json.loads(manifest.read_text(encoding="utf-8"))

        self.assertEqual(actual, expected)
        self.assertGreater(result.triple_count, 0)
        self.assertEqual(manifest_data["pipeline"], "emdat")
        self.assertTrue(manifest_data["metadata"]["validated"])
        self.assertEqual(manifest_data["metadata"]["quality"]["status"], "PASSED")
        self.assertEqual(manifest_data["metadata"]["quality"]["record_count"], 1)
        self.assertEqual(
            manifest_data["metadata"]["canonical_rdf_sha256"],
            result.canonical_rdf_sha256,
        )


if __name__ == "__main__":
    unittest.main()
