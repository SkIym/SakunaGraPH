"""Unified command shell for packaged source jobs and compatibility services."""

from __future__ import annotations

from dataclasses import dataclass
import runpy
import sys
from typing import Sequence


@dataclass(frozen=True, slots=True)
class Command:
    module: str
    description: str


COMMANDS: dict[str, Command] = {
    "ndrrmc": Command("sakunagraph_etl.sources.ndrrmc.job", "Build NDRRMC event RDF"),
    "gda": Command("sakunagraph_etl.sources.gda.job", "Build GDA event RDF"),
    "emdat": Command("sakunagraph_etl.sources.emdat.job", "Build EM-DAT event RDF"),
    "dromic": Command("sakunagraph_etl.sources.dromic.job", "Build DROMIC event RDF"),
    "psgc": Command("sakunagraph_etl.sources.psgc.job", "Build and validate PSGC RDF"),
    "align": Command("sakunagraph_etl.resolution.job", "Resolve events across sources"),
    "load-graphdb": Command("sakunagraph_etl.io.graphdb", "Publish RDF to GraphDB"),
    "graphdb-admin": Command(
        "sakunagraph_etl.io.recovery",
        "Back up, restore, or fully rebuild GraphDB",
    ),
    "artifacts": Command("sakunagraph_etl.io.artifacts", "Verify or materialize a run"),
    "workflow": Command(
        "sakunagraph_etl.orchestration.cli",
        "Run, resume, or backfill production workflows",
    ),
    "quality": Command(
        "sakunagraph_etl.quality.cli",
        "Validate parsed-data contracts and quality thresholds",
    ),
    "check-dromic": Command(
        "sakunagraph_etl.sources.dromic.quality",
        "Check parsed DROMIC years",
    ),
    "fetch-dromic": Command(
        "sakunagraph_etl.sources.dromic.fetch",
        "Fetch DROMIC situation reports",
    ),
    "parse-dromic": Command(
        "sakunagraph_etl.sources.dromic.parse",
        "Parse DROMIC PDF reports",
    ),
    "convert-dromic": Command(
        "sakunagraph_etl.sources.dromic.document_conversion",
        "Convert DROMIC DOCX reports to PDF",
    ),
    "parse-ndrrmc": Command(
        "sakunagraph_etl.sources.ndrrmc.parse",
        "Parse NDRRMC PDF reports",
    ),
}


def help_text() -> str:
    width = max(len(name) for name in COMMANDS)
    commands = "\n".join(
        f"  {name:<{width}}  {command.description}"
        for name, command in COMMANDS.items()
    )
    return (
        "usage: sakuna-etl <command> [arguments]\n\n"
        "commands:\n"
        f"{commands}\n\n"
        "Run 'sakuna-etl <command> --help' for command-specific options."
    )


def delegate(command_name: str, arguments: Sequence[str]) -> int:
    command = COMMANDS[command_name]
    previous_argv = sys.argv
    sys.argv = [f"sakuna-etl {command_name}", *arguments]
    try:
        try:
            runpy.run_module(command.module, run_name="__main__")
        except SystemExit as error:
            if error.code is None:
                return 0
            if isinstance(error.code, int):
                return error.code
            raise
    finally:
        sys.argv = previous_argv
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] in {"-h", "--help", "help"}:
        print(help_text())
        return 0

    command_name = arguments.pop(0)
    if command_name not in COMMANDS:
        print(f"Unknown command: {command_name}\n", file=sys.stderr)
        print(help_text(), file=sys.stderr)
        return 2

    return delegate(command_name, arguments)


__all__ = ["COMMANDS", "Command", "delegate", "help_text", "main"]
