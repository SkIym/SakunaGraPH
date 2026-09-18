"""Run reproducible competency-query and SHACL checks for SakunaGraPH.

The RDFLib competency lane is an offline preflight. Release evidence for inference must use the
GraphDB lane against a dedicated GraphDB 11.1.3 repository configured with the OWL2-RL ruleset.
The pySHACL lane checks the separate, post-paper publication contract.
"""

from __future__ import annotations

import argparse
import base64
import csv
import difflib
import hashlib
import json
import os
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import owlrl
import rdflib
from owlrl import DeductiveClosure, OWLRL_Semantics
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
from rdflib.plugins.sparql.operators import custom_function
from rdflib.term import Identifier


ROOT = Path(__file__).resolve().parents[1]
VALIDATION_ROOT = ROOT / "ontology" / "validation"
MANIFEST_PATH = VALIDATION_ROOT / "competency-manifest.json"
ONTOLOGY_PATH = ROOT / "ontology" / "sakunagraph.ttl"
TAXONOMY_PATH = ROOT / "ontology" / "disaster_type_scheme.ttl"
FIXTURE_MARKER = URIRef("https://sakuna.ph/fixture/competency-v1")
FIXTURE_LABEL = Literal("SakunaGraPH competency fixture v1")
SAKUNA = "https://sakuna.ph/"
FIXTURE = "https://sakuna.ph/fixture/"
EXPECTED_CATEGORIES = {
    "event-and-type-classification",
    "casualties-and-population",
    "damage",
    "service-disruptions",
    "response-and-preparedness",
    "provenance",
}
OFN_AS_HOURS = URIRef("http://www.ontotext.com/sparql/functions/asHours")


class ValidationError(RuntimeError):
    """A deterministic semantic validation failure."""


@custom_function(OFN_AS_HOURS, override=True)
def rdflib_as_hours(value: Literal) -> Literal:
    """Provide the GraphDB ``ofn:asHours`` function to the RDFLib preflight lane."""

    python_value = value.toPython()
    if not isinstance(python_value, timedelta):
        raise TypeError(f"ofn:asHours expected xsd:dayTimeDuration, found {value.n3()}")
    hours = Decimal(str(python_value.total_seconds())) / Decimal(3600)
    return Literal(hours, datatype=XSD.decimal)


def sha256_lf(path: Path) -> str:
    payload = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def resolve_validation_path(value: str) -> Path:
    path = (VALIDATION_ROOT / value).resolve()
    if not path.is_relative_to(VALIDATION_ROOT.resolve()):
        raise ValidationError(f"Validation path escapes ontology/validation: {value}")
    return path


def load_manifest() -> dict[str, Any]:
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(f"Could not read {relative(MANIFEST_PATH)}: {exc}") from exc

    if manifest.get("schema_version") != 1:
        raise ValidationError("competency-manifest.json must use schema_version 1")

    fixture = manifest.get("fixture", {})
    fixture_path = resolve_validation_path(str(fixture.get("path", "")))
    if not fixture_path.is_file():
        raise ValidationError(f"Missing fixture: {relative(fixture_path)}")
    actual_digest = sha256_lf(fixture_path)
    expected_digest = fixture.get("sha256_lf")
    if actual_digest != expected_digest:
        raise ValidationError(
            f"Fixture digest mismatch for {relative(fixture_path)}: "
            f"expected {expected_digest}, found {actual_digest}"
        )

    queries = manifest.get("queries")
    if not isinstance(queries, list) or len(queries) != 20:
        raise ValidationError("Competency manifest must contain exactly 20 queries")

    ids: set[str] = set()
    query_paths: set[Path] = set()
    categories: set[str] = set()
    for index, case in enumerate(queries, start=1):
        if not isinstance(case, dict):
            raise ValidationError(f"Query entry {index} must be an object")
        expected_id = f"CQ{index:02d}"
        if case.get("id") != expected_id:
            raise ValidationError(f"Query entry {index} must have id {expected_id}")
        if expected_id in ids:
            raise ValidationError(f"Duplicate query id: {expected_id}")
        ids.add(expected_id)

        category = case.get("category")
        if category not in EXPECTED_CATEGORIES:
            raise ValidationError(f"{expected_id} has unknown category: {category}")
        categories.add(category)

        requirements = case.get("requirements")
        if not requirements or any(not re.fullmatch(r"OR-0[1-8]", item) for item in requirements):
            raise ValidationError(f"{expected_id} must link to one or more OR-01 through OR-08 IDs")
        if not case.get("paper_pages"):
            raise ValidationError(f"{expected_id} must record at least one paper page")
        if case.get("assertion") != "exact-rows":
            raise ValidationError(f"{expected_id} must use the supported exact-rows assertion")
        if case.get("required_inference") not in {"none", "OWL2-RL"}:
            raise ValidationError(f"{expected_id} has an unsupported inference regime")
        if not isinstance(case.get("ordering_matters"), bool):
            raise ValidationError(f"{expected_id} must say whether ordering matters")

        columns = case.get("expected_columns")
        if not isinstance(columns, list) or not columns or len(columns) != len(set(columns)):
            raise ValidationError(f"{expected_id} must declare unique expected columns")

        query_path = resolve_validation_path(str(case.get("query", "")))
        case_fixture_path = resolve_validation_path(str(case.get("fixture", "")))
        if case_fixture_path != fixture_path:
            raise ValidationError(
                f"{expected_id} must reference the suite's immutable fixture: {relative(fixture_path)}"
            )
        if query_path.name != f"cq{index:02d}.rq" or not query_path.is_file():
            raise ValidationError(f"{expected_id} query file is missing or incorrectly named")
        query_paths.add(query_path)
        resolve_validation_path(str(case.get("expected", "")))

    actual_query_paths = set((VALIDATION_ROOT / "queries").glob("cq*.rq"))
    if query_paths != actual_query_paths:
        missing = sorted(relative(path) for path in query_paths - actual_query_paths)
        extra = sorted(relative(path) for path in actual_query_paths - query_paths)
        raise ValidationError(f"Query catalog mismatch; missing={missing}, extra={extra}")
    if categories != EXPECTED_CATEGORIES:
        raise ValidationError(
            f"All six paper categories are required; missing={sorted(EXPECTED_CATEGORIES - categories)}"
        )
    return manifest


def semantic_artifact_paths() -> list[Path]:
    ontology_root = ROOT / "ontology"
    paths = [
        path
        for path in ontology_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".ttl", ".rdf", ".owl"}
    ]
    return sorted(paths)


def parse_semantic_artifacts() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in semantic_artifact_paths():
        graph = Graph()
        try:
            graph.parse(path)
        except Exception as exc:  # RDF parser exceptions vary by serialization.
            raise ValidationError(f"Could not parse semantic artifact {relative(path)}: {exc}") from exc
        records.append(
            {
                "path": relative(path),
                "sha256_lf": sha256_lf(path),
                "triple_count": len(graph),
            }
        )
    return records


def build_rdflib_graph(fixture_path: Path) -> Graph:
    graph = Graph()
    for path in (ONTOLOGY_PATH, TAXONOMY_PATH, fixture_path):
        graph.parse(path)
    DeductiveClosure(
        OWLRL_Semantics,
        axiomatic_triples=False,
        datatype_axioms=False,
    ).expand(graph)
    check_inference_probes(graph)
    return graph


def check_inference_probes(graph: Graph) -> None:
    event = URIRef(f"{FIXTURE}event/inference-probe")
    location = URIRef(f"{FIXTURE}location/inference-probe")
    region = URIRef(f"{FIXTURE}location/inference-probe-region")
    checks = {
        "fixture marker": (FIXTURE_MARKER, RDFS.label, FIXTURE_LABEL),
        "subclass inference": (event, RDF.type, URIRef(f"{SAKUNA}DisasterEvent")),
        "hasLocation property chain": (event, URIRef(f"{SAKUNA}hasLocation"), location),
        "transitive isPartOf": (location, URIRef(f"{SAKUNA}isPartOf"), region),
    }
    missing = [name for name, triple in checks.items() if triple not in graph]
    if missing:
        raise ValidationError(f"Inference preflight failed: {', '.join(missing)}")


def normalize_term(term: Identifier | None) -> str:
    if term is None:
        return ""
    if not isinstance(term, Literal):
        return term.n3()

    if term.datatype == XSD.decimal:
        value = Decimal(str(term.toPython()))
        lexical = format(value.normalize(), "f")
        if "." not in lexical:
            lexical += ".0"
        return Literal(lexical, datatype=XSD.decimal, normalize=False).n3()
    if term.datatype == XSD.dateTime:
        value = term.toPython()
        if isinstance(value, datetime):
            return Literal(value.isoformat(), datatype=XSD.dateTime, normalize=False).n3()
    return term.n3()


def normalize_rdflib_result(result: Any) -> tuple[list[str], list[list[str]]]:
    columns = [str(variable) for variable in result.vars]
    rows = [[normalize_term(term) for term in row] for row in result]
    return columns, rows


def sparql_json_term(binding: dict[str, str] | None) -> str:
    if binding is None:
        return ""
    kind = binding.get("type")
    value = binding.get("value", "")
    if kind == "uri":
        return URIRef(value).n3()
    if kind == "bnode":
        return f"_:{value}"
    if kind in {"literal", "typed-literal"}:
        datatype = binding.get("datatype")
        language = binding.get("xml:lang") or binding.get("lang")
        return normalize_term(Literal(
            value,
            lang=language,
            datatype=URIRef(datatype) if datatype else None,
        ))
    raise ValidationError(f"Unsupported SPARQL JSON binding type: {kind}")


def graphdb_request(
    endpoint: str,
    query: str,
    *,
    username: str | None,
    password: str | None,
    timeout: float,
) -> dict[str, Any]:
    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-query; charset=utf-8",
    }
    if username is not None:
        token = base64.b64encode(f"{username}:{password or ''}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"
    request = Request(endpoint, data=query.encode("utf-8"), headers=headers, method="POST")
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise ValidationError(f"GraphDB returned HTTP {exc.code}: {detail}") from exc
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise ValidationError(f"GraphDB request failed: {exc}") from exc


def check_graphdb_probes(
    endpoint: str,
    *,
    username: str | None,
    password: str | None,
    timeout: float,
) -> None:
    probe = """
        PREFIX : <https://sakuna.ph/>
        PREFIX fix: <https://sakuna.ph/fixture/>
        PREFIX evt: <https://sakuna.ph/fixture/event/>
        PREFIX loc: <https://sakuna.ph/fixture/location/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        ASK {
            fix:competency-v1 a prov:Collection ;
                rdfs:label "SakunaGraPH competency fixture v1" .
            evt:inference-probe a :DisasterEvent ;
                :hasLocation loc:inference-probe .
            loc:inference-probe :isPartOf loc:inference-probe-region .
        }
    """
    payload = graphdb_request(
        endpoint,
        probe,
        username=username,
        password=password,
        timeout=timeout,
    )
    if payload.get("boolean") is not True:
        raise ValidationError(
            "GraphDB fixture/inference probe failed. Use a dedicated GraphDB 11.1.3 repository "
            "configured with OWL2-RL and loaded with the pinned semantic artifacts."
        )


def execute_graphdb_select(
    endpoint: str,
    query: str,
    *,
    username: str | None,
    password: str | None,
    timeout: float,
) -> tuple[list[str], list[list[str]]]:
    payload = graphdb_request(
        endpoint,
        query,
        username=username,
        password=password,
        timeout=timeout,
    )
    try:
        columns = list(payload["head"]["vars"])
        bindings = payload["results"]["bindings"]
    except (KeyError, TypeError) as exc:
        raise ValidationError("GraphDB did not return a SPARQL SELECT result") from exc
    rows = [[sparql_json_term(binding.get(column)) for column in columns] for binding in bindings]
    return columns, rows


def read_expected(path: Path) -> tuple[list[str], list[list[str]]]:
    if not path.is_file():
        raise ValidationError(f"Missing expected result: {relative(path)}")
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        try:
            columns = next(reader)
        except StopIteration as exc:
            raise ValidationError(f"Expected result is empty: {relative(path)}") from exc
        return columns, list(reader)


def write_expected(path: Path, columns: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)


def display_rows(rows: Sequence[Sequence[str]]) -> list[str]:
    return [json.dumps(list(row), ensure_ascii=False) + "\n" for row in rows]


def compare_case(
    case: dict[str, Any],
    actual_columns: list[str],
    actual_rows: list[list[str]],
) -> None:
    expected_path = resolve_validation_path(case["expected"])
    expected_columns, expected_rows = read_expected(expected_path)
    declared_columns = case["expected_columns"]
    if actual_columns != declared_columns:
        raise ValidationError(
            f"{case['id']} result columns differ: expected {declared_columns}, found {actual_columns}"
        )
    if expected_columns != declared_columns:
        raise ValidationError(
            f"{case['id']} expected CSV header differs: "
            f"declared {declared_columns}, found {expected_columns}"
        )

    if not case["ordering_matters"]:
        actual_rows = sorted(actual_rows)
        expected_rows = sorted(expected_rows)
    if actual_rows != expected_rows:
        diff = "".join(
            difflib.unified_diff(
                display_rows(expected_rows),
                display_rows(actual_rows),
                fromfile=relative(expected_path),
                tofile=f"actual/{case['id'].lower()}",
            )
        )
        raise ValidationError(f"{case['id']} row mismatch:\n{diff}")


def validate_competency(
    *,
    engine: str,
    graphdb_endpoint: str | None = None,
    graphdb_username: str | None = None,
    graphdb_password: str | None = None,
    timeout: float = 30.0,
    update_expected: bool = False,
) -> tuple[dict[str, Any], list[str]]:
    started_at = datetime.now(UTC)
    manifest = load_manifest()
    artifacts = parse_semantic_artifacts()
    fixture_path = resolve_validation_path(manifest["fixture"]["path"])

    if update_expected and engine != "rdflib":
        raise ValidationError("Expected results can only be regenerated from the frozen RDFLib lane")

    graph: Graph | None = None
    if engine == "rdflib":
        graph = build_rdflib_graph(fixture_path)
        engine_record = {
            "name": "RDFLib preflight",
            "rdflib_version": rdflib.__version__,
            "owlrl_version": owlrl.__version__,
            "inference": "OWL2-RL",
            "production_equivalence": False,
        }
    elif engine == "graphdb":
        if not graphdb_endpoint:
            raise ValidationError(
                "--graphdb-endpoint or GRAPHDB_ENDPOINT is required with --engine graphdb"
            )
        check_graphdb_probes(
            graphdb_endpoint,
            username=graphdb_username,
            password=graphdb_password,
            timeout=timeout,
        )
        engine_record = {
            "name": "GraphDB",
            "declared_version": "11.1.3",
            "ruleset": "OWL2-RL",
            "endpoint": graphdb_endpoint,
            "configuration_verified_by": [
                "fixture marker",
                "rdfs:subClassOf inference",
                "owl:propertyChainAxiom inference",
                "owl:TransitiveProperty inference",
            ],
        }
    else:
        raise ValidationError(f"Unsupported engine: {engine}")

    failures: list[str] = []
    outcomes: list[dict[str, Any]] = []
    for case in manifest["queries"]:
        query_path = resolve_validation_path(case["query"])
        query = query_path.read_text(encoding="utf-8")
        query_started = time.perf_counter()
        try:
            if graph is not None:
                columns, rows = normalize_rdflib_result(graph.query(query))
            else:
                assert graphdb_endpoint is not None
                columns, rows = execute_graphdb_select(
                    graphdb_endpoint,
                    query,
                    username=graphdb_username,
                    password=graphdb_password,
                    timeout=timeout,
                )
            if update_expected:
                if columns != case["expected_columns"]:
                    raise ValidationError(
                        f"{case['id']} result columns differ: "
                        f"expected {case['expected_columns']}, found {columns}"
                    )
                write_expected(resolve_validation_path(case["expected"]), columns, rows)
            else:
                compare_case(case, columns, rows)
        except Exception as exc:  # Continue to report all query failures in one run.
            message = f"{case['id']}: {exc}"
            failures.append(message)
            status = "failed"
            row_count = None
        else:
            status = "passed"
            row_count = len(rows)
        outcomes.append(
            {
                "id": case["id"],
                "category": case["category"],
                "requirements": case["requirements"],
                "status": status,
                "row_count": row_count,
                "duration_ms": round((time.perf_counter() - query_started) * 1000, 3),
            }
        )

    completed_at = datetime.now(UTC)
    report = {
        "schema_version": 1,
        "suite": manifest["suite"],
        "status": "failed" if failures else "passed",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "engine": engine_record,
        "fixture": {
            "path": relative(fixture_path),
            "sha256_lf": manifest["fixture"]["sha256_lf"],
        },
        "semantic_artifacts": artifacts,
        "queries": outcomes,
        "failures": failures,
    }
    return report, failures


def write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    suites = parser.add_mutually_exclusive_group(required=True)
    suites.add_argument(
        "--competency",
        action="store_true",
        help="run the 20 competency-question regression cases",
    )
    suites.add_argument(
        "--shacl",
        action="store_true",
        help="run the post-paper SHACL publication-contract fixtures",
    )
    parser.add_argument(
        "--engine",
        choices=("rdflib", "graphdb"),
        default="rdflib",
        help="execution lane (default: rdflib offline preflight)",
    )
    parser.add_argument(
        "--graphdb-endpoint",
        default=os.environ.get("GRAPHDB_ENDPOINT"),
        help="dedicated GraphDB SPARQL query endpoint (or GRAPHDB_ENDPOINT)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="GraphDB request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="optional path for the machine-readable JSON report",
    )
    parser.add_argument(
        "--update-expected",
        action="store_true",
        help="maintainer-only: rewrite expected CSVs from the RDFLib fixture lane",
    )
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.shacl and args.engine != "rdflib":
        parser.error("--shacl is an offline pySHACL suite and does not accept --engine graphdb")
    if args.shacl and args.update_expected:
        parser.error("--update-expected applies only to --competency")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.shacl:
        try:
            from sakunagraph_etl.quality.shacl_fixtures import (
                ShaclFixtureError,
                run_shacl_fixture_suite,
            )

            suite_result = run_shacl_fixture_suite()
        except (ImportError, ShaclFixtureError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

        report = suite_result.report
        if args.report:
            report_path = args.report if args.report.is_absolute() else ROOT / args.report
            write_report(report_path, report)
            print(
                f"Report: "
                f"{relative(report_path) if report_path.is_relative_to(ROOT) else report_path}"
            )

        for outcome in report["cases"]:
            marker = "PASS" if outcome["status"] == "passed" else "FAIL"
            count = "?" if outcome["result_count"] is None else outcome["result_count"]
            print(f"{marker} {outcome['case_id']} [{count} validation results]")
        if suite_result.failures:
            print("\nSHACL fixture failures:", file=sys.stderr)
            for failure in suite_result.failures:
                print(f"- {failure}", file=sys.stderr)
            return 1

        print(
            f"\nPASS: validated all {len(report['cases'])} post-paper SHACL fixture cases "
            "across the disaster and PSGC publication contracts."
        )
        return 0

    username = os.environ.get("GRAPHDB_READ_ONLY_USERNAME")
    password = os.environ.get("GRAPHDB_READ_ONLY_PASSWORD")
    if (username is None) != (password is None):
        print(
            "ERROR: GRAPHDB_READ_ONLY_USERNAME and GRAPHDB_READ_ONLY_PASSWORD must be set together",
            file=sys.stderr,
        )
        return 2

    try:
        report, failures = validate_competency(
            engine=args.engine,
            graphdb_endpoint=args.graphdb_endpoint,
            graphdb_username=username,
            graphdb_password=password,
            timeout=args.timeout,
            update_expected=args.update_expected,
        )
    except ValidationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    if args.report:
        report_path = args.report if args.report.is_absolute() else ROOT / args.report
        write_report(report_path, report)
        print(f"Report: {relative(report_path) if report_path.is_relative_to(ROOT) else report_path}")

    for outcome in report["queries"]:
        marker = "PASS" if outcome["status"] == "passed" else "FAIL"
        row_count = "?" if outcome["row_count"] is None else outcome["row_count"]
        print(f"{marker} {outcome['id']} [{row_count} rows] {outcome['category']}")
    if failures:
        print("\nCompetency failures:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    action = "generated" if args.update_expected else "validated"
    print(
        f"\nPASS: {action} all 20 competency questions across all six paper categories "
        f"using {report['engine']['name']}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
