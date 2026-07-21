"""SHACL validation interface with an adapter for the active validator."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

from rdflib import BNode, Graph, RDF, URIRef


GraphInput = Graph | str | Path
FocusNode = URIRef | BNode | str


@dataclass(frozen=True, slots=True)
class ValidationOutcome:
    conforms: bool
    label: str
    details: str
    data_triples: int
    validation_triples: int
    native_result: Any = None


@runtime_checkable
class ValidationService(Protocol):
    def validate(
        self,
        graph: GraphInput,
        *,
        label: str,
        focus_nodes: Iterable[FocusNode] | None = None,
        raise_on_error: bool = True,
    ) -> ValidationOutcome: ...


class ShaclValidationService:
    """Adapter around the package-owned SHACL validator."""

    def __init__(self, validator: Any | None = None, **validator_options: Any) -> None:
        if validator is None:
            from sakunagraph_etl.quality.shacl import ShaclValidator

            validator = ShaclValidator.from_paths(**validator_options)
        self.validator = validator

    def validate(
        self,
        graph: GraphInput,
        *,
        label: str,
        focus_nodes: Iterable[FocusNode] | None = None,
        raise_on_error: bool = True,
    ) -> ValidationOutcome:
        result = self.validator.validate_graph(
            graph,
            label=label,
            focus_nodes=focus_nodes,
            raise_on_error=raise_on_error,
        )
        return ValidationOutcome(
            conforms=bool(result.conforms),
            label=str(result.label),
            details=str(result.results_text),
            data_triples=int(result.data_triples),
            validation_triples=int(result.validation_triples),
            native_result=result,
        )


def validation_focus_nodes(graph: Graph) -> tuple[URIRef, ...]:
    """Return typed URI subjects suitable for focused SHACL validation."""

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


__all__ = [
    "FocusNode",
    "GraphInput",
    "ShaclValidationService",
    "ValidationOutcome",
    "ValidationService",
    "validation_focus_nodes",
]
