"""Compare the FastAPI contract consumed by the frontend with its Stage 0 snapshot."""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
from pathlib import Path
from typing import Any


FRONTEND_OPERATIONS = {
    ("POST", "/api/sparql"),
    ("POST", "/api/ask"),
    ("POST", "/api/ask/preview"),
    ("POST", "/api/ask/stream"),
    ("GET", "/api/map/events"),
    ("GET", "/api/disasters/details"),
    ("GET", "/api/ontology/graph"),
    ("GET", "/api/ontology/taxonomy"),
    ("GET", "/api/ontology/psgc"),
    ("GET", "/api/analysis/filter-options"),
    ("GET", "/api/analysis/events"),
    ("GET", "/api/analysis/events/export.csv"),
    ("GET", "/api/analysis/summary"),
    ("GET", "/api/analysis/disaster-counts"),
    ("GET", "/api/analysis/victim-trends"),
    ("GET", "/api/analysis/region-rankings"),
    ("GET", "/api/analysis/disaster-rankings"),
    ("GET", "/api/analysis/damage-histogram"),
    ("GET", "/api/analysis/damage-vs-affected"),
    ("GET", "/api/analysis/calendar/years"),
    ("GET", "/api/analysis/calendar/months"),
    ("GET", "/api/analysis/calendar/days"),
    ("GET", "/api/analysis/timeline/category-stacks"),
    ("GET", "/api/analysis/timeline/date-events"),
}


def _collect_schema_names(value: Any, names: set[str]) -> None:
    if isinstance(value, dict):
        reference = value.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/components/schemas/"):
            names.add(reference.rsplit("/", 1)[-1])
        for nested in value.values():
            _collect_schema_names(nested, names)
    elif isinstance(value, list):
        for nested in value:
            _collect_schema_names(nested, names)


def _operation_contract(operation: dict[str, Any]) -> dict[str, Any]:
    responses = {}
    for status, response in operation.get("responses", {}).items():
        responses[status] = {
            key: response[key]
            for key in ("content", "headers")
            if key in response
        }
    return {
        "parameters": operation.get("parameters", []),
        "requestBody": operation.get("requestBody"),
        "responses": responses,
    }


def build_contract() -> dict[str, Any]:
    frontend_dir = Path(__file__).resolve().parents[1]
    repository_dir = frontend_dir.parent
    api_dir = repository_dir / "api"
    sys.path.insert(0, str(api_dir))
    os.chdir(api_dir)

    from src.main import app  # pylint: disable=import-outside-toplevel

    openapi = app.openapi()
    paths: dict[str, Any] = {}
    missing: list[str] = []

    for method, path in sorted(FRONTEND_OPERATIONS, key=lambda item: (item[1], item[0])):
        operation = openapi.get("paths", {}).get(path, {}).get(method.lower())
        if operation is None:
            missing.append(f"{method} {path}")
            continue
        paths.setdefault(path, {})[method.lower()] = _operation_contract(operation)

    if missing:
        raise RuntimeError("FastAPI is missing frontend operations: " + ", ".join(missing))

    schema_names: set[str] = set()
    _collect_schema_names(paths, schema_names)
    schemas = openapi.get("components", {}).get("schemas", {})
    pending = list(schema_names)
    while pending:
        name = pending.pop()
        before = set(schema_names)
        _collect_schema_names(schemas[name], schema_names)
        pending.extend(sorted(schema_names - before))

    return {
        "paths": paths,
        "schemas": {name: schemas[name] for name in sorted(schema_names)},
    }


def serialized(contract: dict[str, Any]) -> str:
    return json.dumps(contract, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--print",
        action="store_true",
        dest="print_contract",
        help="Print the normalized current contract instead of comparing it.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Replace the snapshot after an explicitly reviewed compatible contract change.",
    )
    args = parser.parse_args()

    current = serialized(build_contract())
    if args.print_contract:
        sys.stdout.write(current)
        return 0

    snapshot_path = (
        Path(__file__).resolve().parents[1]
        / "tests"
        / "contracts"
        / "openapi.snapshot.json"
    )
    if args.update:
        snapshot_path.write_text(current, encoding="utf-8")
        print(f"Updated {snapshot_path}.")
        return 0

    expected = serialized(json.loads(snapshot_path.read_text(encoding="utf-8")))
    if current == expected:
        print(f"Frontend API contract matches {snapshot_path.relative_to(snapshot_path.parents[2])}.")
        return 0

    diff = difflib.unified_diff(
        expected.splitlines(),
        current.splitlines(),
        fromfile="recorded frontend API contract",
        tofile="current FastAPI contract",
        lineterm="",
    )
    print("\n".join(diff))
    print("\nThe consumed API contract changed. Review compatibility before updating the snapshot.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
