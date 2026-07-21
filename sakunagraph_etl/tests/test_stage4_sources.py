from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import polars as pl
from rdflib import Graph, URIRef
from rdflib.compare import to_canonical_graph

from mappings import dromic as legacy_dromic_rdf
from mappings import gda as legacy_gda_rdf
from mappings import ndrrmc as legacy_ndrrmc_rdf
from parse import check_fails as legacy_dromic_quality
from pipeline import run_dromic as legacy_dromic_job
from pipeline import run_gda as legacy_gda_job
from pipeline import run_ndrrmc as legacy_ndrrmc_job
from pipeline import run_psgc as legacy_psgc_job
from sakunagraph_etl.cli import COMMANDS
from sakunagraph_etl.rdf.graph import create_graph
from sakunagraph_etl.rdf.validation import ShaclValidationService
from sakunagraph_etl.sources.dromic import job as dromic_job
from sakunagraph_etl.sources.dromic import quality as dromic_quality
from sakunagraph_etl.sources.dromic import rdf as dromic_rdf
from sakunagraph_etl.sources.gda import job as gda_job
from sakunagraph_etl.sources.gda import rdf as gda_rdf
from sakunagraph_etl.sources.ndrrmc import job as ndrrmc_job
from sakunagraph_etl.sources.ndrrmc import rdf as ndrrmc_rdf
from sakunagraph_etl.sources.psgc import job as psgc_job
from sakunagraph_etl.sources.psgc import rdf as psgc_rdf
from sakunagraph_etl.quality.shacl import ShaclValidator


GOLDEN_DIR = Path(__file__).parent / "golden"


def canonical_rdf(graph: Graph) -> str:
    value = to_canonical_graph(graph).serialize(format="nt")
    lines = sorted(line.strip() for line in value.splitlines() if line.strip())
    return ("\n".join(lines) + "\n") if lines else ""


def stage4_fixture_graphs() -> dict[str, Graph]:
    gda_graph = Graph()
    gda_rdf.event_mapping(
        [
            gda_rdf.Event(
                id="fixture-gda",
                eventClass="M",
                eventName="Fixture Flood",
                hasType="Flood",
                hasSubtype="FlashFlood",
                hasLocation=URIRef("https://sakuna.ph/psgc/1300000000"),
                startDate=date(2026, 1, 2),
                endDate=date(2026, 1, 3),
                rowNumber=1,
                reference="https://example.test/gda",
                remarks=None,
                otherDescription=None,
            )
        ],
        gda_graph,
        URIRef("https://sakuna.ph/source/gda"),
    )

    ndrrmc_graph = create_graph()
    ndrrmc_rdf.event_mapping(
        ndrrmc_graph,
        ndrrmc_rdf.Event(
            eventName="Fixture Typhoon",
            hasDisasterType="TropicalCyclone",
            startDate=datetime(2026, 1, 2),
            endDate=datetime(2026, 1, 3),
            id="fixture-ndrrmc",
        ),
    )

    dromic_graph = create_graph()
    dromic_rdf.event_mapping(
        dromic_graph,
        dromic_rdf.Event(
            eventName="Fixture Flood",
            hasDisasterType="Flood",
            startDate=None,
            endDate=None,
            id="fixture-dromic",
            remarks="Fixture remarks",
            hasBarangay=None,
            hasLocation=URIRef("https://sakuna.ph/psgc/1300000000"),
        ),
    )

    psgc_graph = Graph()
    rows = pl.DataFrame(
        [
            {
                "psgc_code": "1300000000",
                "name": "National Capital Region",
                "geo_level": "Reg",
                "correspondence_code": None,
                "old_names": None,
                "city_class": None,
                "income_class": None,
                "urban_rural": None,
                "population_2024": 14_000_000,
                "population_note": None,
                "status": "Active",
            }
        ]
    )
    psgc_rdf.build_abox(psgc_graph, rows, include_barangay=False)

    return {
        "gda": gda_graph,
        "ndrrmc": ndrrmc_graph,
        "dromic": dromic_graph,
        "psgc": psgc_graph,
    }


class Stage4SourceMigrationTests(unittest.TestCase):
    def test_legacy_modules_are_thin_reexports(self) -> None:
        self.assertIs(legacy_gda_job.run, gda_job.run)
        self.assertIs(legacy_psgc_job.run, psgc_job.run)
        self.assertIs(legacy_ndrrmc_job.run, ndrrmc_job.run)
        self.assertIs(legacy_dromic_job.run, dromic_job.run)
        self.assertIs(legacy_gda_rdf.Event, gda_rdf.Event)
        self.assertIs(legacy_ndrrmc_rdf.Event, ndrrmc_rdf.Event)
        self.assertIs(legacy_dromic_rdf.Event, dromic_rdf.Event)
        self.assertIs(legacy_dromic_quality.check_year, dromic_quality.check_year)

    def test_cli_uses_packaged_source_jobs(self) -> None:
        expected = {
            "gda": "sakunagraph_etl.sources.gda.job",
            "psgc": "sakunagraph_etl.sources.psgc.job",
            "ndrrmc": "sakunagraph_etl.sources.ndrrmc.job",
            "dromic": "sakunagraph_etl.sources.dromic.job",
        }
        self.assertEqual(
            {name: COMMANDS[name].module for name in expected},
            expected,
        )

    def test_jobs_accept_onprem_profile(self) -> None:
        for source_job in (gda_job, psgc_job, ndrrmc_job, dromic_job):
            with self.subTest(job=source_job.__name__):
                args = source_job.build_parser().parse_args(["--profile", "onprem"])
                self.assertEqual(args.profile, "onprem")

    def test_complex_parser_responsibilities_have_importable_modules(self) -> None:
        source_root = Path(dromic_job.__file__).parents[1]
        expected = {
            "dromic": (
                "metadata.py",
                "tables.py",
                "normalization.py",
                "output.py",
                "quality.py",
                "document_conversion.py",
            ),
            "ndrrmc": (
                "extraction.py",
                "metadata.py",
                "tables.py",
                "impacts.py",
                "impact_rdf.py",
            ),
        }
        for source, names in expected.items():
            for name in names:
                with self.subTest(source=source, module=name):
                    self.assertTrue((source_root / source / name).is_file())

    def test_source_mapping_output_matches_golden(self) -> None:
        for source, graph in stage4_fixture_graphs().items():
            with self.subTest(source=source):
                expected = (GOLDEN_DIR / f"{source}_fixture.nt").read_text(
                    encoding="utf-8"
                )
                self.assertEqual(canonical_rdf(graph), expected)

    def test_source_golden_graphs_pass_shacl(self) -> None:
        shapes = Graph().parse(
            data="""
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                @prefix sh: <http://www.w3.org/ns/shacl#> .
                [] a sh:NodeShape ;
                   sh:targetSubjectsOf rdf:type ;
                   sh:property [ sh:path rdf:type ; sh:minCount 1 ] .
            """,
            format="turtle",
        )
        service = ShaclValidationService(ShaclValidator(shapes_graph=shapes))

        for source, graph in stage4_fixture_graphs().items():
            with self.subTest(source=source):
                outcome = service.validate(graph, label=source, raise_on_error=False)
                self.assertTrue(outcome.conforms, outcome.details)

    def test_packaged_jobs_run_fixture_to_rdf(self) -> None:
        fixture_graphs = stage4_fixture_graphs()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            gda_output = root / "gda.ttl"
            gda_event = gda_rdf.Event(
                id="fixture-gda",
                eventClass="M",
                eventName="Fixture Flood",
                hasType="Flood",
                hasSubtype="FlashFlood",
                hasLocation=URIRef("https://sakuna.ph/psgc/1300000000"),
                startDate=date(2026, 1, 2),
                endDate=date(2026, 1, 3),
                rowNumber=1,
                reference="https://example.test/gda",
                remarks=None,
                otherDescription=None,
            )
            with mock.patch.object(
                gda_job,
                "_load_entities",
                return_value={gda_rdf.Event: [gda_event]},
            ):
                gda_job.run(
                    str(gda_output),
                    input_path=root / "gda.xlsx",
                    out_dir=root,
                )

            psgc_input = root / "psgc.xlsx"
            psgc_input.touch()
            psgc_output = root / "psgc.ttl"
            shapes_path = root / "shapes.ttl"
            shapes_path.write_text(
                """
                @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                @prefix sh: <http://www.w3.org/ns/shacl#> .
                [] a sh:NodeShape ; sh:targetSubjectsOf rdf:type ;
                   sh:property [ sh:path rdf:type ; sh:minCount 1 ] .
                """,
                encoding="utf-8",
            )
            psgc_rows = pl.DataFrame(
                [
                    {
                        "psgc_code": "1300000000",
                        "name": "National Capital Region",
                        "geo_level": "Reg",
                        "correspondence_code": None,
                        "old_names": None,
                        "city_class": None,
                        "income_class": None,
                        "urban_rural": None,
                        "population_2024": 14_000_000,
                        "population_note": None,
                        "status": "Active",
                    }
                ]
            )
            with mock.patch.object(psgc_rdf, "load_dataframe", return_value=psgc_rows):
                psgc_job.run(
                    input_path=psgc_input,
                    out_file=psgc_output,
                    out_dir=root,
                    shapes_path=shapes_path,
                )

            ndrrmc_data = root / "ndrrmc"
            ndrrmc_event_dir = ndrrmc_data / "Fixture Typhoon"
            ndrrmc_event_dir.mkdir(parents=True)
            (ndrrmc_event_dir / "metadata.json").write_text("{}", encoding="utf-8")
            ndrrmc_event = ndrrmc_rdf.Event(
                eventName="Fixture Typhoon",
                hasDisasterType="TropicalCyclone",
                startDate=datetime(2026, 1, 2),
                endDate=datetime(2026, 1, 3),
                id="fixture-ndrrmc",
            )
            def map_ndrrmc_fixture(data_dir, event, graph, *, debug_dir=None):
                del data_dir, debug_dir
                ndrrmc_rdf.event_mapping(graph, event)

            with (
                mock.patch.object(ndrrmc_job, "_load_events", return_value=[ndrrmc_event]),
                mock.patch.object(ndrrmc_job, "_map_event", side_effect=map_ndrrmc_fixture),
            ):
                ndrrmc_job.run(
                    "ndrrmc",
                    data_dir=ndrrmc_data,
                    out_dir=root,
                    batch_size=1,
                )

            dromic_data = root / "dromic"
            dromic_event_dir = dromic_data / "Fixture Flood"
            dromic_event_dir.mkdir(parents=True)
            (dromic_event_dir / "metadata.json").write_text("{}", encoding="utf-8")
            dromic_event = dromic_rdf.Event(
                eventName="Fixture Flood",
                hasDisasterType="Flood",
                startDate=None,
                endDate=None,
                id="fixture-dromic",
                remarks="Fixture remarks",
                hasBarangay=None,
                hasLocation=URIRef("https://sakuna.ph/psgc/1300000000"),
            )
            with (
                mock.patch.object(dromic_job, "load_event", return_value=dromic_event),
                mock.patch.object(dromic_job, "load_provenance", return_value=None),
                mock.patch.object(dromic_job, "prov_mapping"),
            ):
                dromic_job.run(
                    dromic_data,
                    root / "dromic.ttl",
                    batch_size=1,
                )

            outputs = {
                "gda": gda_output,
                "psgc": psgc_output,
                "ndrrmc": root / "ndrrmc-1.ttl",
                "dromic": root / "dromic.ttl",
            }
            for source, output in outputs.items():
                with self.subTest(source=source):
                    graph = Graph().parse(output, format="turtle")
                    self.assertGreater(len(graph), 0)
                    if source == "psgc":
                        self.assertTrue(set(fixture_graphs[source]).issubset(set(graph)))
                    elif source != "gda":
                        self.assertEqual(canonical_rdf(graph), canonical_rdf(fixture_graphs[source]))


if __name__ == "__main__":
    unittest.main()
