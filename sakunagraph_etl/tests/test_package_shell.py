from importlib.metadata import entry_points
from contextlib import redirect_stderr
from io import StringIO
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import etl_config
from mappings import graph as legacy_graph
from mappings import iris as legacy_iris
from sakunagraph_etl import __version__
from sakunagraph_etl import config
from sakunagraph_etl.cli import COMMANDS, delegate, main
from sakunagraph_etl.rdf import graph, iris


LEGACY_ETL_ROOT = Path(__file__).resolve().parents[2] / "etl"


def compatibility_environment() -> dict[str, str]:
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = os.pathsep.join(
        value for value in (str(LEGACY_ETL_ROOT), existing) if value
    )
    return environment


class PackageShellTests(unittest.TestCase):
    def test_legacy_modules_reexport_the_package_implementations(self) -> None:
        self.assertIs(etl_config.load_settings, config.load_settings)
        self.assertIs(legacy_graph.create_graph, graph.create_graph)
        self.assertIs(legacy_iris.event_uri, iris.event_uri)

    def test_unified_commands_point_at_existing_runners(self) -> None:
        self.assertEqual(
            COMMANDS["emdat"].module,
            "sakunagraph_etl.sources.emdat.job",
        )
        self.assertEqual(
            COMMANDS["align"].module,
            "sakunagraph_etl.resolution.job",
        )
        self.assertEqual(
            COMMANDS["load-graphdb"].module,
            "sakunagraph_etl.io.graphdb",
        )

    def test_delegate_preserves_runner_arguments_and_process_argv(self) -> None:
        original_argv = sys.argv
        with patch("sakunagraph_etl.cli.runpy.run_module") as run_module:
            result = delegate("emdat", ["--out", "events.ttl"])

            run_module.assert_called_once_with(
                "sakunagraph_etl.sources.emdat.job",
                run_name="__main__",
            )
            self.assertEqual(result, 0)
        self.assertIs(sys.argv, original_argv)

    def test_unknown_command_has_usage_error(self) -> None:
        with redirect_stderr(StringIO()):
            self.assertEqual(main(["unknown-source"]), 2)

    def test_package_module_runs_from_unrelated_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            result = subprocess.run(
                [sys.executable, "-m", "sakunagraph_etl", "--help"],
                cwd=Path(temp),
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("sakuna-etl <command>", result.stdout)

    def test_new_and_legacy_runner_help_work_from_unrelated_directory(self) -> None:
        commands = (
            [sys.executable, "-m", "sakunagraph_etl", "emdat", "--help"],
            [sys.executable, "-m", "sakunagraph_etl.sources.emdat.job", "--help"],
            [sys.executable, "-m", "pipeline.run_emdat", "--help"],
        )
        with tempfile.TemporaryDirectory() as temp:
            for command in commands:
                with self.subTest(command=command):
                    result = subprocess.run(
                        command,
                        cwd=Path(temp),
                        env=compatibility_environment(),
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("--out-dir", result.stdout)

    def test_stage5_commands_and_wrappers_work_from_unrelated_directory(self) -> None:
        commands = (
            ([sys.executable, "-m", "sakunagraph_etl", "align", "--help"], "--incremental"),
            ([sys.executable, "-m", "pipeline.build_alignment", "--help"], "--incremental"),
            ([sys.executable, "-m", "sakunagraph_etl", "load-graphdb", "--help"], "--validate"),
            ([sys.executable, "-m", "pipeline.load_graphdb", "--help"], "--validate"),
        )
        with tempfile.TemporaryDirectory() as temp:
            for command, expected in commands:
                with self.subTest(command=command):
                    result = subprocess.run(
                        command,
                        cwd=Path(temp),
                        env=compatibility_environment(),
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(expected, result.stdout)

    def test_console_entry_point_is_installed(self) -> None:
        scripts = {
            entry_point.name: entry_point.value
            for entry_point in entry_points(group="console_scripts")
        }
        self.assertEqual(scripts.get("sakuna-etl"), "sakunagraph_etl.cli:main")
        self.assertEqual(__version__, "0.9.0")


if __name__ == "__main__":
    unittest.main()
