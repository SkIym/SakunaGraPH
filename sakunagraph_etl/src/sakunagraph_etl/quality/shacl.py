"""Package-owned SHACL validation implementation."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence
from pyshacl import validate
from rdflib import BNode, Graph, URIRef
from rdflib.namespace import RDF
from sakunagraph_etl.config import SETTINGS


log = logging.getLogger(__name__)

DEFAULT_SHAPES_PATH = SETTINGS.paths.ontology_root / "shapes" / "shapes.ttl"
DEFAULT_ONTOLOGY_PATH = SETTINGS.paths.ontology_root / "sakunagraph.ttl"
DEFAULT_CONTEXT_GRAPH_PATHS = (
    SETTINGS.paths.rdf_root / "psgc" / "psgc.ttl",
    SETTINGS.paths.ontology_root / "disaster_type_scheme.ttl",
    SETTINGS.paths.rdf_root / "orgs" / "orgs.ttl",
    SETTINGS.paths.rdf_root / "prov" / "prov.ttl",
)
DEFAULT_DATA_GRAPH_PATTERNS = (
    SETTINGS.paths.event_rdf_root / "gda" / "*.ttl",
    SETTINGS.paths.event_rdf_root / "emdat" / "*.ttl",
    SETTINGS.paths.event_rdf_root / "dromic" / "*.ttl",
    SETTINGS.paths.event_rdf_root / "ndrrmc" / "*.ttl",
    SETTINGS.paths.rdf_root / "resolution" / "alignments.ttl",
)

GraphInput = Graph | str | Path
FocusNode = URIRef | BNode | str


@dataclass(frozen=True)
class ShaclValidationResult:
    conforms: bool
    results_graph: Graph
    results_text: str
    label: str
    data_triples: int
    validation_triples: int
    focus_nodes: tuple[FocusNode, ...]


class ShaclValidationError(RuntimeError):
    def __init__(self, result: ShaclValidationResult) -> None:
        super().__init__(
            f"SHACL validation failed for {result.label}.\n{result.results_text}"
        )
        self.result = result


class ShaclValidator:
    """
    Reusable SHACL validator with preloaded shapes, ontology, and context data.

    Create one instance per pipeline run and reuse it for every batch. This
    keeps shapes.ttl, sakunagraph.ttl, and reference graphs off the hot path.
    """

    def __init__(
        self,
        *,
        shapes_graph: Graph,
        ontology_graph: Graph | None = None,
        context_graph: Graph | None = None,
    ) -> None:
        self.shapes_graph = shapes_graph
        self.ontology_graph = ontology_graph
        self.context_graph = context_graph if context_graph is not None else Graph()

    @classmethod
    def from_paths(
        cls,
        *,
        shapes_graph: GraphInput = DEFAULT_SHAPES_PATH,
        ontology_graph: GraphInput | None = DEFAULT_ONTOLOGY_PATH,
        context_graphs: Iterable[GraphInput] | None = None,
        include_context_graphs: bool = True,
        include_default_context: bool = True,
    ) -> "ShaclValidator":
        context_graph = Graph()
        if include_context_graphs:
            for context_input in _context_graph_inputs(context_graphs, include_default_context):
                _merge_graph(context_graph, _as_graph(context_input))

        return cls(
            shapes_graph=_as_graph(shapes_graph),
            ontology_graph=_as_graph(ontology_graph) if ontology_graph is not None else None,
            context_graph=context_graph,
        )

    def validate_graph(
        self,
        graph: GraphInput,
        *,
        focus_nodes: Iterable[FocusNode] | None = None,
        label: str | None = None,
        inference: str | None = None,
        abort_on_first: bool = False,
        allow_infos: bool = False,
        allow_warnings: bool = False,
        meta_shacl: bool = False,
        advanced: bool = False,
        js: bool = False,
        debug: bool = False,
        raise_on_error: bool = True,
    ) -> ShaclValidationResult:
        source_graph = _as_graph(graph)
        working_graph = _clone_graph(self.context_graph)
        _merge_graph(working_graph, source_graph)
        focus = tuple(focus_nodes or ())

        kwargs = {
            "shacl_graph": self.shapes_graph,
            "inference": inference,
            "abort_on_first": abort_on_first,
            "allow_infos": allow_infos,
            "allow_warnings": allow_warnings,
            "meta_shacl": meta_shacl,
            "advanced": advanced,
            "js": js,
            "debug": debug,
        }
        if self.ontology_graph is not None:
            kwargs["ont_graph"] = self.ontology_graph

        if focus:
            if not _validate_supports_focus_nodes():
                raise RuntimeError(
                    "The installed pyshacl version does not support focus_nodes. "
                    "Upgrade pyshacl or call validate_graph without focus_nodes."
                )
            kwargs["focus_nodes"] = list(focus)

        validation_label = label or _graph_label(graph)
        log.info(
            "Validating %s with SHACL (%d data triples, %d context triples)",
            validation_label,
            len(source_graph),
            len(self.context_graph),
        )

        conforms, results_graph, results_text = validate(working_graph, **kwargs)
        result = ShaclValidationResult(
            conforms=bool(conforms),
            results_graph=results_graph,
            results_text=str(results_text),
            label=validation_label,
            data_triples=len(source_graph),
            validation_triples=len(working_graph),
            focus_nodes=focus,
        )

        if raise_on_error and not result.conforms:
            raise ShaclValidationError(result)

        return result


def validate_graph(
    graph: GraphInput,
    *,
    focus_nodes: Iterable[FocusNode] | None = None,
    label: str | None = None,
    shapes_graph: GraphInput = DEFAULT_SHAPES_PATH,
    ontology_graph: GraphInput | None = DEFAULT_ONTOLOGY_PATH,
    context_graphs: Iterable[GraphInput] | None = None,
    include_context_graphs: bool = True,
    include_default_context: bool = True,
    inference: str | None = None,
    abort_on_first: bool = False,
    allow_infos: bool = False,
    allow_warnings: bool = False,
    meta_shacl: bool = False,
    advanced: bool = False,
    js: bool = False,
    debug: bool = False,
    raise_on_error: bool = True,
) -> ShaclValidationResult:
    """
    Validate an RDF graph against the SakunaGraPH SHACL shapes.

    Reference graphs are copied into the validation graph so `sh:class`
    constraints can see PSGC, disaster-type, organization, and provenance
    nodes. When callers pass `focus_nodes`, those context nodes are not
    selected as validation targets.
    """
    uses_default_validator = (
        shapes_graph == DEFAULT_SHAPES_PATH
        and ontology_graph == DEFAULT_ONTOLOGY_PATH
        and context_graphs is None
        and include_context_graphs
        and include_default_context
    )
    validator = (
        _default_validator()
        if uses_default_validator
        else ShaclValidator.from_paths(
            shapes_graph=shapes_graph,
            ontology_graph=ontology_graph,
            context_graphs=context_graphs,
            include_context_graphs=include_context_graphs,
            include_default_context=include_default_context,
        )
    )

    return validator.validate_graph(
        graph,
        focus_nodes=focus_nodes,
        label=label,
        inference=inference,
        abort_on_first=abort_on_first,
        allow_infos=allow_infos,
        allow_warnings=allow_warnings,
        meta_shacl=meta_shacl,
        advanced=advanced,
        js=js,
        debug=debug,
        raise_on_error=raise_on_error,
    )


@lru_cache(maxsize=1)
def _default_validator() -> ShaclValidator:
    return ShaclValidator.from_paths()


def validate_file(path: str | Path, **kwargs) -> ShaclValidationResult:
    return validate_graph(Path(path), label=kwargs.pop("label", str(path)), **kwargs)


def validation_focus_nodes(graph: Graph) -> tuple[URIRef, ...]:
    """Return typed URI subjects from the caller's graph for focused validation."""
    return tuple(
        sorted(
            {
                subject
                for subject in graph.subjects(RDF.type, None)
                if isinstance(subject, URIRef)
            },
            key=str,
        )
    )


def existing_default_context_paths() -> tuple[Path, ...]:
    return tuple(path for path in DEFAULT_CONTEXT_GRAPH_PATHS if path.exists())


def default_data_graph_paths() -> tuple[Path, ...]:
    paths: list[Path] = []
    for pattern in DEFAULT_DATA_GRAPH_PATTERNS:
        paths.extend(sorted(pattern.parent.glob(pattern.name)))
    return tuple(paths)


def _context_graph_inputs(
    context_graphs: Iterable[GraphInput] | None,
    include_default_context: bool,
) -> tuple[GraphInput, ...]:
    inputs: list[GraphInput] = []
    if include_default_context:
        inputs.extend(existing_default_context_paths())
    if context_graphs:
        inputs.extend(context_graphs)
    return tuple(inputs)



@lru_cache(maxsize=None)
def _validate_supports_focus_nodes() -> bool:
    import inspect

    signature = inspect.signature(validate)
    return "focus_nodes" in signature.parameters


def _as_graph(graph: GraphInput | None) -> Graph:
    if graph is None:
        return Graph()
    if isinstance(graph, Graph):
        return graph

    return _load_graph(str(Path(graph).resolve()))


@lru_cache(maxsize=None)
def _load_graph(path: str) -> Graph:
    graph = Graph()
    graph.parse(path)
    return graph


def _clone_graph(graph: Graph) -> Graph:
    clone = Graph()
    _merge_graph(clone, graph)
    return clone


def _merge_graph(target: Graph, source: Graph) -> None:
    for prefix, namespace in source.namespaces():
        target.bind(prefix, namespace)
    for triple in source:
        target.add(triple)


def _graph_label(graph: GraphInput) -> str:
    if isinstance(graph, Graph):
        return "in-memory graph"
    return str(graph)


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate SakunaGraPH RDF with SHACL.")
    parser.add_argument("graphs", nargs="*", help="Turtle files to validate.")
    parser.add_argument(
        "--no-default-context",
        action="store_true",
        help="Do not include PSGC, disaster-type, organization, or provenance context graphs.",
    )
    parser.add_argument(
        "--no-context",
        action="store_true",
        help="Do not load or merge any context graphs.",
    )
    parser.add_argument(
        "--no-focus",
        action="store_true",
        help="Validate all target nodes, including nodes from context graphs.",
    )
    args = parser.parse_args(argv)

    paths = tuple(Path(path) for path in args.graphs) or default_data_graph_paths()
    if not paths:
        raise FileNotFoundError("No RDF data graphs found under data/rdf.")

    exit_code = 0
    for path in paths:
        graph = _as_graph(path)
        result = validate_graph(
            graph,
            label=str(path),
            focus_nodes=None if args.no_focus else validation_focus_nodes(graph),
            include_context_graphs=not args.no_context,
            include_default_context=not args.no_default_context,
            raise_on_error=False,
        )
        print(f"{result.label}: {result.conforms}")
        if not result.conforms:
            exit_code = 1
            print(result.results_text)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(_main())
