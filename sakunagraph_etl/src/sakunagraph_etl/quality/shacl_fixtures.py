"""Manifest-driven regression suite for the post-paper SHACL publication contract."""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from rdflib import Graph, Literal
from rdflib.namespace import RDF, SH
from rdflib.term import Identifier
from rdflib.util import from_n3

from sakunagraph_etl.config import SETTINGS
from sakunagraph_etl.quality.shacl import ShaclValidator


DEFAULT_MANIFEST_PATH = (
    SETTINGS.paths.ontology_root / "validation" / "shacl-manifest.json"
)
EXPECTED_CATEGORIES = {
    "inconsistent-date-range",
    "invalid-boundary-value",
    "invalid-geography-hierarchy",
    "invalid-unit-or-controlled-resource",
    "missing-required-property",
    "provenance-omission",
    "wrong-datatype",
}
SOURCE_PROFILES = {"dromic", "emdat", "gda", "ndrrmc", "psgc"}


class ShaclFixtureError(RuntimeError):
    """Raised when a fixture manifest or case violates the suite contract."""


@dataclass(frozen=True)
class ShaclCaseOutcome:
    case_id: str
    status: str
    conforms: bool | None
    blocks_publication: bool | None
    result_count: int | None
    duration_ms: float
    error: str | None = None


@dataclass(frozen=True)
class ShaclFixtureSuiteResult:
    report: dict[str, Any]
    failures: tuple[str, ...]


def sha256_lf(path: Path) -> str:
    payload = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _resolve_ontology_path(value: str) -> Path:
    ontology_root = SETTINGS.paths.ontology_root.resolve()
    path = (ontology_root / value).resolve()
    if not path.is_relative_to(ontology_root):
        raise ShaclFixtureError(f"SHACL suite path escapes ontology/: {value}")
    return path


def _term(value: str) -> Identifier:
    try:
        parsed = from_n3(value)
    except Exception as exc:  # pragma: no cover - RDFLib supplies the detail.
        raise ShaclFixtureError(f"Invalid N3 RDF term {value!r}: {exc}") from exc
    if parsed is None:
        raise ShaclFixtureError(f"Invalid N3 RDF term: {value!r}")
    return parsed


def _load_graph(path: Path) -> Graph:
    graph = Graph()
    try:
        graph.parse(path)
    except Exception as exc:
        raise ShaclFixtureError(f"Could not parse {path}: {exc}") from exc
    return graph


def _fixture_graph(catalog: Graph, subjects: Iterable[str] | None) -> Graph:
    if subjects is None:
        graph = Graph()
        for prefix, namespace in catalog.namespaces():
            graph.bind(prefix, namespace)
        for triple in catalog:
            graph.add(triple)
        return graph

    graph = Graph()
    for prefix, namespace in catalog.namespaces():
        graph.bind(prefix, namespace)
    for value in subjects:
        subject = _term(value)
        for triple in catalog.triples((subject, None, None)):
            graph.add(triple)
    return graph


def _isolated_validator(
    validator: ShaclValidator,
    shape: Identifier,
    focus: Identifier,
) -> ShaclValidator:
    """Select one target shape without hiding its referenced property/helper shapes.

    ``pyshacl``'s ``use_shapes`` option intentionally excludes unlisted property shapes.  These
    fixtures instead deactivate other independently targeted node shapes and add an explicit
    target for the case focus.  Referenced helper shapes remain available to the selected shape.
    """

    shape_graph = Graph()
    for prefix, namespace in validator.shapes_graph.namespaces():
        shape_graph.bind(prefix, namespace)
    for triple in validator.shapes_graph:
        shape_graph.add(triple)

    target_predicates = (SH.targetClass, SH.targetNode, SH.targetSubjectsOf, SH.targetObjectsOf)
    for node_shape in shape_graph.subjects(RDF.type, SH.NodeShape):
        if node_shape == shape:
            continue
        has_target = any(
            shape_graph.value(node_shape, predicate) is not None
            for predicate in target_predicates
        )
        if has_target:
            shape_graph.set((node_shape, SH.deactivated, Literal(True)))
    shape_graph.add((shape, SH.targetNode, focus))
    return ShaclValidator(
        shapes_graph=shape_graph,
        ontology_graph=validator.ontology_graph,
        context_graph=validator.context_graph,
    )


def _result_rows(results_graph: Graph) -> list[dict[str, str | None]]:
    rows: list[dict[str, str | None]] = []
    for result in results_graph.subjects(RDF.type, SH.ValidationResult):
        rows.append(
            {
                "focus_node": _n3_or_none(results_graph.value(result, SH.focusNode)),
                "path": _n3_or_none(results_graph.value(result, SH.resultPath)),
                "severity": _n3_or_none(results_graph.value(result, SH.resultSeverity)),
                "source_shape": _n3_or_none(results_graph.value(result, SH.sourceShape)),
                "component": _n3_or_none(
                    results_graph.value(result, SH.sourceConstraintComponent)
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: tuple(str(row[key]) for key in sorted(row)),
    )


def _n3_or_none(value: Identifier | None) -> str | None:
    return value.n3() if value is not None else None


def _matches(actual: Mapping[str, str | None], expected: Mapping[str, str]) -> bool:
    return all(actual.get(key) == value for key, value in expected.items())


def _required_property_shapes(shape_graphs: Mapping[str, Graph]) -> set[str]:
    required: set[str] = set()
    for graph in shape_graphs.values():
        node_shapes = set(graph.subjects(RDF.type, SH.NodeShape))
        for node_shape in node_shapes:
            for property_shape in graph.objects(node_shape, SH.property):
                minimum = graph.value(property_shape, SH.minCount)
                if minimum is not None and int(minimum) > 0:
                    required.add(property_shape.n3())
    return required


def load_shacl_manifest(path: Path = DEFAULT_MANIFEST_PATH) -> dict[str, Any]:
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ShaclFixtureError(f"Could not read {path}: {exc}") from exc

    if manifest.get("schema_version") != 1:
        raise ShaclFixtureError("shacl-manifest.json must use schema_version 1")
    if manifest.get("historical_scope") != "post-paper-publication-contract":
        raise ShaclFixtureError(
            "SHACL evidence must be labeled as a post-paper publication contract"
        )
    if set(manifest.get("source_profiles", [])) != SOURCE_PROFILES:
        raise ShaclFixtureError("The manifest must declare all five source profiles")
    if manifest.get("severity_policy") != {
        "Violation": "block",
        "Warning": "report",
        "Info": "report",
    }:
        raise ShaclFixtureError("The manifest severity policy does not match ODR-0018")

    shape_paths = manifest.get("shape_graphs")
    if not isinstance(shape_paths, dict) or set(shape_paths) != {"core", "psgc"}:
        raise ShaclFixtureError("shape_graphs must declare exactly core and psgc")
    shape_graphs = {
        key: _load_graph(_resolve_ontology_path(value))
        for key, value in shape_paths.items()
    }

    catalogs = manifest.get("fixture_catalogs")
    if not isinstance(catalogs, dict) or not catalogs:
        raise ShaclFixtureError("fixture_catalogs must be a non-empty object")
    for name, record in catalogs.items():
        if not isinstance(record, dict):
            raise ShaclFixtureError(f"Fixture catalog {name} must be an object")
        catalog_path = _resolve_ontology_path(str(record.get("path", "")))
        if not catalog_path.is_file():
            raise ShaclFixtureError(f"Missing SHACL fixture catalog: {catalog_path}")
        digest = sha256_lf(catalog_path)
        if digest != record.get("sha256_lf"):
            raise ShaclFixtureError(
                f"Fixture digest mismatch for {catalog_path}: "
                f"expected {record.get('sha256_lf')}, found {digest}"
            )
        _load_graph(catalog_path)

    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ShaclFixtureError("The SHACL manifest must contain cases")

    case_ids: set[str] = set()
    positive_shapes: list[str] = []
    covered_required_shapes: set[str] = set()
    negative_categories: set[str] = set()
    scopes: set[str] = set()
    has_boundary_value = False
    for case in cases:
        if not isinstance(case, dict):
            raise ShaclFixtureError("Every SHACL case must be an object")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not re.fullmatch(r"[a-z0-9-]+", case_id):
            raise ShaclFixtureError(f"Invalid SHACL case id: {case_id!r}")
        if case_id in case_ids:
            raise ShaclFixtureError(f"Duplicate SHACL case id: {case_id}")
        case_ids.add(case_id)

        scope = case.get("shape_graph")
        if scope not in shape_graphs:
            raise ShaclFixtureError(f"{case_id} has unknown shape_graph {scope!r}")
        scopes.add(scope)
        if case.get("fixture") not in catalogs:
            raise ShaclFixtureError(f"{case_id} references an unknown fixture catalog")

        profiles = set(case.get("profiles", []))
        if not profiles or not profiles.issubset(SOURCE_PROFILES):
            raise ShaclFixtureError(f"{case_id} must declare applicable source profiles")

        kind = case.get("kind")
        mode = case.get("mode", "isolated")
        if kind not in {"positive", "negative"} or mode not in {"isolated", "full"}:
            raise ShaclFixtureError(f"{case_id} has invalid kind or mode")
        if mode == "isolated":
            if not case.get("shape") or not case.get("focus"):
                raise ShaclFixtureError(f"{case_id} requires shape and focus terms")
            shape = _term(case["shape"])
            if (shape, RDF.type, SH.NodeShape) not in shape_graphs[scope]:
                raise ShaclFixtureError(f"{case_id} references a non-node shape")
        if kind == "positive":
            if case.get("expected_conforms") is not True:
                raise ShaclFixtureError(f"Positive case {case_id} must conform")
            if mode == "isolated":
                positive_shapes.append(case["shape"])
            if "boundary-value" in case.get("categories", []):
                has_boundary_value = True
        else:
            if case.get("expected_conforms") is not False:
                raise ShaclFixtureError(f"Negative case {case_id} must not conform")
            categories = set(case.get("categories", []))
            if not categories:
                raise ShaclFixtureError(f"Negative case {case_id} needs a category")
            negative_categories.update(categories)
            expected_results = case.get("expected_results")
            if not isinstance(expected_results, list) or not expected_results:
                raise ShaclFixtureError(f"Negative case {case_id} needs expected results")
            covered_required_shapes.update(
                result["source_shape"]
                for result in expected_results
                if "missing-required-property" in categories
            )

    if scopes != {"core", "psgc"}:
        raise ShaclFixtureError("Cases must exercise both core and PSGC shapes")
    if negative_categories != EXPECTED_CATEGORIES:
        missing = EXPECTED_CATEGORIES - negative_categories
        extra = negative_categories - EXPECTED_CATEGORIES
        raise ShaclFixtureError(
            f"Negative category mismatch; missing={sorted(missing)}, extra={sorted(extra)}"
        )
    if not has_boundary_value:
        raise ShaclFixtureError("At least one positive boundary-value case is required")

    declared_node_shapes = {
        shape.n3()
        for graph in shape_graphs.values()
        for shape in graph.subjects(RDF.type, SH.NodeShape)
    }
    if set(positive_shapes) != declared_node_shapes or len(positive_shapes) != len(
        declared_node_shapes
    ):
        missing = declared_node_shapes - set(positive_shapes)
        duplicated = sorted(
            shape for shape in set(positive_shapes) if positive_shapes.count(shape) > 1
        )
        raise ShaclFixtureError(
            "Every node shape needs exactly one isolated positive case; "
            f"missing={sorted(missing)}, duplicated={duplicated}"
        )

    required_shapes = _required_property_shapes(shape_graphs)
    if not required_shapes.issubset(covered_required_shapes):
        raise ShaclFixtureError(
            "Every sh:minCount property needs a missing-property case; "
            f"missing={sorted(required_shapes - covered_required_shapes)}"
        )
    return manifest


def run_shacl_fixture_suite(
    path: Path = DEFAULT_MANIFEST_PATH,
) -> ShaclFixtureSuiteResult:
    started_at = datetime.now(UTC)
    manifest = load_shacl_manifest(path)
    ontology_path = _resolve_ontology_path(manifest["ontology"])
    catalogs = {
        name: _load_graph(_resolve_ontology_path(record["path"]))
        for name, record in manifest["fixture_catalogs"].items()
    }
    validators = {
        scope: ShaclValidator.from_paths(
            shapes_graph=_resolve_ontology_path(shape_path),
            ontology_graph=ontology_path,
            include_context_graphs=False,
        )
        for scope, shape_path in manifest["shape_graphs"].items()
    }

    failures: list[str] = []
    outcomes: list[ShaclCaseOutcome] = []
    for case in manifest["cases"]:
        case_started = time.perf_counter()
        try:
            graph = _fixture_graph(catalogs[case["fixture"]], case.get("subjects"))
            isolated = case.get("mode", "isolated") == "isolated"
            validator = validators[case["shape_graph"]]
            if isolated:
                validator = _isolated_validator(
                    validator,
                    _term(case["shape"]),
                    _term(case["focus"]),
                )
            result = validator.validate_graph(
                graph,
                label=case["id"],
                advanced=True,
                allow_infos=False,
                allow_warnings=False,
                raise_on_error=False,
            )
            rows = _result_rows(result.results_graph)
            severities = {row["severity"] for row in rows}
            blocks_publication = SH.Violation.n3() in severities
            expected_blocks = bool(case.get("expected_blocks_publication", False))
            problems: list[str] = []
            if result.conforms is not case["expected_conforms"]:
                problems.append(
                    f"expected conforms={case['expected_conforms']}, found {result.conforms}"
                )
            if blocks_publication is not expected_blocks:
                problems.append(
                    f"expected blocks_publication={expected_blocks}, "
                    f"found {blocks_publication}"
                )
            for expected in case.get("expected_results", []):
                if not any(_matches(row, expected) for row in rows):
                    problems.append(f"missing expected result {expected}; actual={rows}")
            if problems:
                raise ShaclFixtureError("; ".join(problems))
        except Exception as exc:  # Keep running so CI reports every broken fixture.
            message = f"{case['id']}: {exc}"
            failures.append(message)
            outcomes.append(
                ShaclCaseOutcome(
                    case_id=case["id"],
                    status="failed",
                    conforms=None,
                    blocks_publication=None,
                    result_count=None,
                    duration_ms=round((time.perf_counter() - case_started) * 1000, 3),
                    error=str(exc),
                )
            )
        else:
            outcomes.append(
                ShaclCaseOutcome(
                    case_id=case["id"],
                    status="passed",
                    conforms=result.conforms,
                    blocks_publication=blocks_publication,
                    result_count=len(rows),
                    duration_ms=round((time.perf_counter() - case_started) * 1000, 3),
                )
            )

    completed_at = datetime.now(UTC)
    report = {
        "schema_version": 1,
        "suite": manifest["suite"],
        "historical_scope": manifest["historical_scope"],
        "status": "failed" if failures else "passed",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "engine": {
            "name": "pySHACL",
            "inference": "none",
            "production_equivalence": True,
        },
        "severity_policy": manifest["severity_policy"],
        "source_profiles": manifest["source_profiles"],
        "cases": [outcome.__dict__ for outcome in outcomes],
        "failures": failures,
    }
    return ShaclFixtureSuiteResult(report=report, failures=tuple(failures))
