"""Load SakunaGraPH Turtle files into GraphDB named graphs.

Run from ``etl/``:

    python -m pipeline.load_graphdb --scope ontology --scope events

The context IRI is derived from the file's path beneath ``data/rdf``.  Every
top-level Turtle file in ``ontology/`` is loaded into the ontology graph;
ontology subdirectories are excluded. Files in a source subdirectory share
that source graph, so
``data/rdf/events/dromic/dromic-2025.ttl`` is loaded into
``https://sakuna.ph/events/dromic``.  A Turtle file directly below
``data/rdf/events`` uses its filename stem as the source graph, e.g.
``events/emdat.ttl`` is loaded into ``https://sakuna.ph/events/emdat``.

``--replace`` groups files by context and replaces each named graph in one
RDF4J transaction.  This keeps a context unchanged if any input fails and lets
GraphDB optimize replacement without a separate, inference-heavy clear.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote, urljoin

import requests


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RDF_ROOT = REPOSITORY_ROOT / "data" / "rdf"
DEFAULT_ONTOLOGY_DIR = REPOSITORY_ROOT / "ontology"
GRAPH_BASE_IRI = "https://sakuna.ph"
SCOPES = ("ontology", "events", "orgs", "prov", "psgc", "resolution")
REPLACE_GRAPH_PREDICATE = "http://www.ontotext.com/replaceGraph"
REPLACE_GRAPH_MARKER = "urn:sakunagraph:loader"


class LoadTarget(NamedTuple):
    """A local Turtle file and the named graph to which it belongs."""

    path: Path
    context: str


class LoaderError(RuntimeError):
    """An expected GraphDB configuration or request error."""


def is_relative_to(path: Path, parent: Path) -> bool:
    """Return whether *path* is inside *parent* (also on Python < 3.9)."""
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def graph_iri_for(path: Path, rdf_root: Path, ontology_dir: Path) -> str:
    """Derive the context IRI for a supported SakunaGraPH Turtle file."""
    if path.parent == ontology_dir:
        return f"{GRAPH_BASE_IRI}/ontology"

    relative = path.relative_to(rdf_root)
    parent_parts = relative.parent.parts

    # Source files directly in data/rdf/events have no source directory.
    # Keep them separate rather than merging unrelated sources into /events.
    if parent_parts == ("events",):
        graph_parts = (*parent_parts, relative.stem)
    elif parent_parts:
        graph_parts = parent_parts
    else:
        # A file placed immediately under data/rdf is its own context graph.
        graph_parts = (relative.stem,)

    encoded_parts = (quote(part, safe="-._~") for part in graph_parts)
    return f"{GRAPH_BASE_IRI}/{'/'.join(encoded_parts)}"


def discover_scope(scope: str, rdf_root: Path, ontology_dir: Path) -> list[Path]:
    """Return Turtle files selected by one logical SakunaGraPH scope."""
    if scope == "ontology":
        return sorted(path for path in ontology_dir.glob("*.ttl") if path.is_file())

    directory = rdf_root / scope
    if not directory.is_dir():
        return []
    return sorted(path for path in directory.rglob("*.ttl") if path.is_file())


def resolve_file_argument(value: str, rdf_root: Path, ontology_dir: Path) -> Path:
    """Resolve an explicit --file value and keep it within loader-owned data."""
    supplied = Path(value).expanduser()
    candidates = (
        supplied,
        rdf_root / supplied,
        REPOSITORY_ROOT / supplied,
    )
    for candidate in candidates:
        path = candidate.resolve()
        if not path.is_file():
            continue
        if path.suffix.lower() != ".ttl":
            raise LoaderError(f"Only Turtle files are supported: {path}")
        if path.parent == ontology_dir or is_relative_to(path, rdf_root):
            return path
        raise LoaderError(
            f"--file must be below {rdf_root} or directly below {ontology_dir}: {path}"
        )
    raise LoaderError(f"Turtle file not found: {value}")


def collect_targets(
    scopes: Iterable[str], files: Iterable[str], rdf_root: Path, ontology_dir: Path
) -> list[LoadTarget]:
    """Discover selected files and return deterministic, de-duplicated targets."""
    paths: set[Path] = set()
    for scope in scopes:
        if scope == "all":
            if rdf_root.is_dir():
                paths.update(path for path in rdf_root.rglob("*.ttl") if path.is_file())
            paths.update(discover_scope("ontology", rdf_root, ontology_dir))
        else:
            paths.update(discover_scope(scope, rdf_root, ontology_dir))

    for value in files:
        paths.add(resolve_file_argument(value, rdf_root, ontology_dir))

    return [
        LoadTarget(path, graph_iri_for(path, rdf_root, ontology_dir))
        for path in sorted(paths)
    ]


def graphdb_url(host: str, repo: str) -> str:
    return f"{repository_url(host, repo)}/statements"


def repository_url(host: str, repo: str) -> str:
    return f"{host.rstrip('/')}/repositories/{quote(repo, safe='-._~')}"


def transactions_url(host: str, repo: str) -> str:
    return f"{repository_url(host, repo)}/transactions"


def display_path(path: Path) -> Path:
    """Show repository-relative paths when possible, otherwise the full path."""
    try:
        return path.relative_to(REPOSITORY_ROOT)
    except ValueError:
        return path


def validate_repository(session: requests.Session, host: str, repo: str) -> None:
    """Confirm the configured repository exists before mutating it."""
    url = f"{host.rstrip('/')}/repositories"
    try:
        response = session.get(url, headers={"Accept": "application/json"}, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise LoaderError(f"Cannot query GraphDB repositories at {url}: {error}") from error

    try:
        payload = response.json()
    except ValueError as error:
        raise LoaderError(f"GraphDB returned invalid repository metadata from {url}") from error

    if isinstance(payload, list):
        repositories = {item.get("id") for item in payload if isinstance(item, dict)}
    else:
        bindings = payload.get("results", {}).get("bindings", [])
        repositories = {
            item.get("id", {}).get("value", item.get("id"))
            for item in bindings
            if isinstance(item, dict)
        }

    if repo not in repositories:
        available = ", ".join(sorted(name for name in repositories if name)) or "none"
        raise LoaderError(f"Repository '{repo}' was not found (available: {available}).")


def clear_repository(session: requests.Session, endpoint: str, timeout: int) -> None:
    """Clear every statement from a repository, including every named graph."""
    try:
        response = session.delete(endpoint, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as error:
        raise LoaderError(f"Could not clear repository: {error}") from error


def clear_context(
    session: requests.Session, endpoint: str, context: str, timeout: int
) -> None:
    """Clear one named graph using the RDF4J/GraphDB statements API."""
    try:
        response = session.delete(
            endpoint,
            params={"context": f"<{context}>"},
            timeout=timeout,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        raise LoaderError(f"Could not clear context <{context}>: {error}") from error


def group_targets_by_context(
    targets: Iterable[LoadTarget],
) -> list[tuple[str, list[LoadTarget]]]:
    """Group targets deterministically for one transaction per named graph."""
    grouped: dict[str, list[LoadTarget]] = {}
    for target in targets:
        grouped.setdefault(target.context, []).append(target)
    return [(context, grouped[context]) for context in sorted(grouped)]


def _request_error_detail(error: requests.RequestException) -> str:
    if error.response is not None:
        return error.response.text[:500]
    return str(error)


def _rollback_transaction(
    session: requests.Session,
    transaction_endpoint: str,
    timeout: int,
) -> None:
    """Best-effort rollback that never masks the original load failure."""
    try:
        session.delete(transaction_endpoint, timeout=timeout)
    except requests.RequestException:
        pass


def replace_context(
    session: requests.Session,
    transaction_collection: str,
    context: str,
    targets: Iterable[LoadTarget],
    timeout: int,
) -> None:
    """Atomically replace one named graph with all of its selected files."""
    context_targets = list(targets)
    if not context_targets:
        raise LoaderError(f"No Turtle files selected for context <{context}>")

    for target in context_targets:
        try:
            if target.path.stat().st_size == 0:
                raise LoaderError(f"Refusing to load empty file: {target.path}")
        except OSError as error:
            raise LoaderError(f"Could not inspect {target.path}: {error}") from error

    transaction_endpoint: str | None = None
    try:
        response = session.post(transaction_collection, timeout=timeout)
        response.raise_for_status()
        location = response.headers.get("Location")
        if not location:
            raise LoaderError(
                "GraphDB started a transaction without returning its endpoint"
            )
        transaction_endpoint = urljoin(
            f"{transaction_collection.rstrip('/')}/",
            location,
        )

        # GraphDB consumes this marker as transaction metadata; it is not
        # inserted as data.  Subsequent ADD actions describe the replacement
        # graph, allowing GraphDB to preserve overlapping inferred statements.
        marker_update = (
            "INSERT DATA { "
            f"<{REPLACE_GRAPH_MARKER}> "
            f"<{REPLACE_GRAPH_PREDICATE}> "
            f"<{context}> "
            "}"
        )
        response = session.put(
            transaction_endpoint,
            params={"action": "UPDATE"},
            data=marker_update.encode("utf-8"),
            headers={"Content-Type": "application/sparql-update"},
            timeout=timeout,
        )
        response.raise_for_status()

        for target in context_targets:
            with target.path.open("rb") as source:
                response = session.put(
                    transaction_endpoint,
                    params={"action": "ADD", "context": f"<{context}>"},
                    data=source,
                    headers={"Content-Type": "text/turtle"},
                    timeout=timeout,
                )
            try:
                response.raise_for_status()
            except requests.RequestException as error:
                detail = _request_error_detail(error)
                raise LoaderError(
                    f"Could not add {target.path} to <{context}>: {detail}"
                ) from error

        response = session.put(
            transaction_endpoint,
            params={"action": "COMMIT"},
            timeout=timeout,
        )
        response.raise_for_status()
        transaction_endpoint = None
    except LoaderError:
        if transaction_endpoint is not None:
            _rollback_transaction(session, transaction_endpoint, timeout)
        raise
    except requests.RequestException as error:
        if transaction_endpoint is not None:
            _rollback_transaction(session, transaction_endpoint, timeout)
        detail = _request_error_detail(error)
        raise LoaderError(f"Could not replace context <{context}>: {detail}") from error
    except OSError as error:
        if transaction_endpoint is not None:
            _rollback_transaction(session, transaction_endpoint, timeout)
        raise LoaderError(f"Could not read input for context <{context}>: {error}") from error


def load_target(session: requests.Session, endpoint: str, target: LoadTarget, timeout: int) -> None:
    """Upload one Turtle document into its derived GraphDB context graph."""
    if target.path.stat().st_size == 0:
        raise LoaderError(f"Refusing to load empty file: {target.path}")

    try:
        with target.path.open("rb") as source:
            response = session.post(
                endpoint,
                params={"context": f"<{target.context}>"},
                data=source,
                headers={"Content-Type": "text/turtle"},
                timeout=timeout,
            )
        response.raise_for_status()
    except requests.RequestException as error:
        detail = _request_error_detail(error)
        raise LoaderError(f"{target.path}: {detail}") from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load SakunaGraPH Turtle files into GraphDB named graphs."
    )
    parser.add_argument("--repo", default="sakunagraph", help="GraphDB repository ID")
    parser.add_argument("--host", default="http://localhost:7200", help="GraphDB server URL")
    parser.add_argument(
        "--rdf-root",
        type=Path,
        default=DEFAULT_RDF_ROOT,
        help="Root containing RDF subgraphs (default: repository data/rdf)",
    )
    parser.add_argument(
        "--scope",
        action="append",
        choices=("all", *SCOPES),
        help="Load a logical subgraph; repeat to combine scopes (default: all)",
    )
    parser.add_argument(
        "--file",
        action="append",
        default=[],
        metavar="TTL",
        help="Load one specific Turtle file; repeat as needed",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Atomically replace each selected named graph",
    )
    parser.add_argument(
        "--clear-repository",
        "--clear",
        dest="clear_repository",
        action="store_true",
        help="Clear the whole repository before loading (not limited to selected graphs)",
    )
    parser.add_argument("--dry-run", action="store_true", help="List files and contexts without calling GraphDB")
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Per-request clear, replacement, and upload timeout in seconds",
    )
    parser.add_argument("--username", default=os.getenv("GRAPHDB_USERNAME"), help="GraphDB username (or GRAPHDB_USERNAME)")
    parser.add_argument("--password", default=os.getenv("GRAPHDB_PASSWORD"), help="GraphDB password (or GRAPHDB_PASSWORD)")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.replace and args.clear_repository:
        parser.error("--replace and --clear-repository cannot be used together")
    return args


def main() -> int:
    args = parse_args()
    rdf_root = args.rdf_root.resolve()
    ontology_dir = DEFAULT_ONTOLOGY_DIR.resolve()
    # An explicit file selection is intentionally narrow.  Scopes and files
    # can still be combined when the caller wants their union.
    scopes = args.scope if args.scope is not None else ([] if args.file else ["all"])

    try:
        targets = collect_targets(scopes, args.file, rdf_root, ontology_dir)
    except (LoaderError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    if not targets:
        print("No Turtle files matched the requested scope or file selection.")
        return 0

    print(f"Selected {len(targets)} Turtle file(s):")
    for target in targets:
        print(f"  {display_path(target.path)} -> <{target.context}>")

    if args.dry_run:
        return 0

    session = requests.Session()
    if args.username:
        session.auth = (args.username, args.password or "")
    endpoint = graphdb_url(args.host, args.repo)
    transaction_collection = transactions_url(args.host, args.repo)

    try:
        validate_repository(session, args.host, args.repo)
        if args.clear_repository:
            print(f"Clearing repository '{args.repo}'...")
            clear_repository(session, endpoint, args.timeout)

        loaded = 0
        failures: list[str] = []
        if args.replace:
            context_groups = group_targets_by_context(targets)
            print(f"Replacing {len(context_groups)} selected named graph(s) atomically...")
            for context, context_targets in context_groups:
                try:
                    replace_context(
                        session,
                        transaction_collection,
                        context,
                        context_targets,
                        args.timeout,
                    )
                except LoaderError as error:
                    failures.append(str(error))
                    print(f"  FAIL  {error}", file=sys.stderr)
                else:
                    loaded += len(context_targets)
                    print(
                        f"  OK  {len(context_targets)} file(s) -> <{context}>"
                    )
        else:
            for target in targets:
                try:
                    load_target(session, endpoint, target, args.timeout)
                except LoaderError as error:
                    failures.append(str(error))
                    print(f"  FAIL  {error}", file=sys.stderr)
                else:
                    loaded += 1
                    print(f"  OK  {target.path.name} -> <{target.context}>")
    except LoaderError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    finally:
        session.close()

    if failures:
        print(
            f"Loaded {loaded} Turtle file(s); {len(failures)} failed. "
            f"Repository: '{args.repo}'.",
            file=sys.stderr,
        )
        return 1

    print(f"Loaded {loaded} Turtle file(s) into repository '{args.repo}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
