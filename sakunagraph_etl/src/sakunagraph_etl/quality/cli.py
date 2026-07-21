"""CLI for source and custom parsed-data quality contracts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from sakunagraph_etl.config import PROFILE_CHOICES, load_settings

from .contracts import SOURCE_SCHEMAS, validate_source_input
from .schemas import QualityPolicy, TableSchema, validate_table


def _load_rows(path: Path, input_format: str) -> list[dict[str, Any]]:
    if input_format == "csv":
        with path.open(newline="", encoding="utf-8-sig") as stream:
            return list(csv.DictReader(stream))
    if input_format == "jsonl":
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("JSON quality input must be an array of objects")
    return value


def _policy(args: argparse.Namespace) -> QualityPolicy:
    return QualityPolicy(
        minimum_records=args.minimum_records,
        maximum_rejected_records=args.maximum_rejected_records,
        maximum_rejected_ratio=args.maximum_rejected_ratio,
        fail_on_unexpected_columns=not args.allow_unexpected_columns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate parsed ETL data contracts.")
    subparsers = parser.add_subparsers(dest="action", required=True)

    source = subparsers.add_parser("source", help="Validate a built-in source contract.")
    source.add_argument("--source", required=True, choices=sorted(SOURCE_SCHEMAS))
    source.add_argument("--input", required=True, type=Path)
    source.add_argument("--profile", choices=PROFILE_CHOICES)

    custom = subparsers.add_parser("table", help="Validate CSV/JSON rows with a schema file.")
    custom.add_argument("--schema", required=True, type=Path)
    custom.add_argument("--input", required=True, type=Path)
    custom.add_argument("--format", choices=("csv", "json", "jsonl"), required=True)

    for command in (source, custom):
        command.add_argument("--report", type=Path)
        command.add_argument("--minimum-records", type=int, default=1)
        command.add_argument("--maximum-rejected-records", type=int, default=0)
        command.add_argument("--maximum-rejected-ratio", type=float, default=0.0)
        command.add_argument("--allow-unexpected-columns", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    policy = _policy(args)
    if args.action == "source":
        # Load configuration now so invalid profiles fail before expensive parsing.
        load_settings(args.profile)
        report = validate_source_input(args.source, args.input, policy=policy)
    else:
        schema = TableSchema.from_dict(json.loads(args.schema.read_text(encoding="utf-8")))
        report = validate_table(_load_rows(args.input, args.format), schema, policy=policy)

    payload = json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if report.status == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
