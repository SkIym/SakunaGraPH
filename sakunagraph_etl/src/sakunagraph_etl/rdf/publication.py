"""Graph publication interface and GraphDB adapter."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, Protocol, runtime_checkable

from rdflib import Graph

from sakunagraph_etl.config import DeploymentProfile
from sakunagraph_etl.rdf.validation import (
    ShaclValidationService,
    validation_focus_nodes,
)

if TYPE_CHECKING:
    from sakunagraph_etl.rdf.validation import ValidationService


class PublicationMode(str, Enum):
    APPEND = "append"
    REPLACE = "replace"


@dataclass(frozen=True, slots=True)
class PublicationTarget:
    path: Path
    context: str


@dataclass(frozen=True, slots=True)
class PublicationResult:
    published_files: int
    published_contexts: tuple[str, ...]
    mode: PublicationMode


class PublicationValidationError(RuntimeError):
    """Raised before network access when selected RDF is not publishable."""


def publication_validation_required(
    profile: str | DeploymentProfile,
    requested: bool | None = None,
) -> bool:
    """Resolve the validation policy for one deployment profile.

    Validation is mandatory in on-premise and cloud deployments. Local runs
    may opt in with ``--validate`` while retaining the established workflow.
    """
    selected = (
        profile
        if isinstance(profile, DeploymentProfile)
        else DeploymentProfile(profile)
    )
    production = selected in {DeploymentProfile.ONPREM, DeploymentProfile.CLOUD}
    if production and requested is False:
        raise PublicationValidationError(
            f"SHACL validation cannot be disabled for the {selected.value} profile"
        )
    return production or requested is True


def validate_publication_targets(
    targets: Iterable[PublicationTarget],
    validation_service: ValidationService,
) -> None:
    """Parse and SHACL-validate each complete context before network access."""
    grouped: dict[str, list[PublicationTarget]] = {}
    for target in targets:
        grouped.setdefault(target.context, []).append(target)

    for context in sorted(grouped):
        graph = Graph()
        context_targets = sorted(grouped[context], key=lambda target: target.path)
        for target in context_targets:
            try:
                graph.parse(target.path, format="turtle")
            except Exception as error:
                raise PublicationValidationError(
                    f"Invalid Turtle in {target.path}: {error}"
                ) from error

        try:
            outcome = validation_service.validate(
                graph,
                label=f"GraphDB context <{context}>",
                focus_nodes=validation_focus_nodes(graph),
                raise_on_error=False,
            )
        except Exception as error:
            raise PublicationValidationError(
                f"Could not validate context <{context}>: {error}"
            ) from error
        if not outcome.conforms:
            raise PublicationValidationError(
                f"SHACL validation failed for context <{context}>: {outcome.details}"
            )


@runtime_checkable
class GraphPublisher(Protocol):
    def publish(
        self,
        targets: Iterable[PublicationTarget],
        *,
        mode: PublicationMode = PublicationMode.APPEND,
    ) -> PublicationResult: ...


class GraphDbPublisher:
    """Publish targets through the package-owned GraphDB adapter."""

    def __init__(
        self,
        *,
        host: str,
        repository: str,
        timeout: int = 300,
        username: str | None = None,
        password: str | None = None,
        session: Any | None = None,
        validate_connection: bool = True,
        require_validation: bool = False,
        validation_service: ValidationService | None = None,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.host = host.rstrip("/")
        self.repository = repository
        self.timeout = timeout
        self.username = username
        self.password = password
        self.session = session
        self.validate_connection = validate_connection
        self.require_validation = require_validation
        self.validation_service = validation_service

    def publish(
        self,
        targets: Iterable[PublicationTarget],
        *,
        mode: PublicationMode = PublicationMode.APPEND,
    ) -> PublicationResult:
        publication_targets = tuple(targets)
        if not publication_targets:
            return PublicationResult(0, (), mode)

        if self.require_validation:
            try:
                service = self.validation_service or ShaclValidationService()
            except Exception as error:
                raise PublicationValidationError(
                    f"Could not initialize SHACL validation: {error}"
                ) from error
            validate_publication_targets(publication_targets, service)

        import requests

        from sakunagraph_etl.io.graphdb import (
            LoadTarget,
            graphdb_url,
            group_targets_by_context,
            load_target,
            replace_context,
            transactions_url,
            validate_repository,
        )

        loader_targets = tuple(
            LoadTarget(path=target.path, context=target.context)
            for target in publication_targets
        )
        owns_session = self.session is None
        session = self.session or requests.Session()
        if self.username:
            session.auth = (self.username, self.password or "")

        try:
            if self.validate_connection:
                validate_repository(session, self.host, self.repository)

            if mode is PublicationMode.REPLACE:
                transaction_endpoint = transactions_url(self.host, self.repository)
                for context, context_targets in group_targets_by_context(loader_targets):
                    replace_context(
                        session,
                        transaction_endpoint,
                        context,
                        context_targets,
                        self.timeout,
                    )
            else:
                endpoint = graphdb_url(self.host, self.repository)
                for target in loader_targets:
                    load_target(session, endpoint, target, self.timeout)

            return PublicationResult(
                published_files=len(publication_targets),
                published_contexts=tuple(
                    sorted({target.context for target in publication_targets})
                ),
                mode=mode,
            )
        finally:
            if owns_session:
                session.close()


__all__ = [
    "GraphDbPublisher",
    "GraphPublisher",
    "PublicationMode",
    "PublicationResult",
    "PublicationTarget",
    "PublicationValidationError",
    "publication_validation_required",
    "validate_publication_targets",
]
