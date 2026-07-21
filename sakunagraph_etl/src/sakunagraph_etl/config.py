"""Typed runtime configuration for the SakunaGraPH ETL."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from os import environ as process_environ
from pathlib import Path
from typing import Mapping


class DeploymentProfile(str, Enum):
    LOCAL = "local"
    ONPREM = "onprem"
    CLOUD = "cloud"


PROFILE_CHOICES = tuple(profile.value for profile in DeploymentProfile)


def _source_project_root() -> Path | None:
    """Find the standalone project when running an editable/source install."""

    module_path = Path(__file__).resolve()
    for candidate in module_path.parents:
        if (
            (candidate / "pyproject.toml").is_file()
            and (candidate / "src" / "sakunagraph_etl").is_dir()
        ):
            return candidate

    return None


def _source_repository_root(project_root: Path | None) -> Path:
    """Find shared data/resources without coupling the package to legacy ETL."""

    if project_root is not None:
        parent = project_root.parent
        if (parent / "ontology").is_dir() or (parent / "etl").is_dir():
            return parent
        return project_root

    # A wheel cannot contain the caller's data repository. In that case the
    # current directory is a harmless default and SAKUNA_REPOSITORY_ROOT or
    # explicit CLI paths provide the deployment location.
    return Path.cwd().resolve()


_SOURCE_PROJECT_ROOT = _source_project_root()
PROJECT_ROOT = _SOURCE_PROJECT_ROOT or Path.cwd().resolve()
REPOSITORY_ROOT = _source_repository_root(_SOURCE_PROJECT_ROOT)
ETL_ROOT = REPOSITORY_ROOT / "etl"


@dataclass(frozen=True, slots=True)
class EtlPaths:
    repository_root: Path
    etl_root: Path
    data_root: Path
    logs_root: Path
    ontology_root: Path
    constants_root: Path
    debug_root: Path | None = None

    @property
    def raw_root(self) -> Path:
        return self.data_root / "raw"

    @property
    def parsed_root(self) -> Path:
        return self.data_root / "parsed"

    @property
    def rdf_root(self) -> Path:
        return self.data_root / "rdf"

    @property
    def event_rdf_root(self) -> Path:
        return self.rdf_root / "events"


@dataclass(frozen=True, slots=True)
class EtlSettings:
    profile: DeploymentProfile
    paths: EtlPaths
    graphdb_host: str
    graphdb_repository: str
    artifact_root: Path
    object_bucket: str | None
    object_prefix: str
    object_endpoint: str | None
    object_region: str
    quality_minimum_records: int
    quality_maximum_rejected_records: int
    quality_maximum_rejected_ratio: float
    quality_fail_on_unexpected_columns: bool


def _boolean(value: str, *, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _absolute_path(value: str | Path, *, base: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base / path
    return path.resolve(strict=False)


def load_settings(
    profile: str | DeploymentProfile | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> EtlSettings:
    """Load one deployment profile from environment variables.

    Relative overrides resolve from ``SAKUNA_REPOSITORY_ROOT`` (or the source
    repository for editable installs), never from an accidental working
    directory.
    """
    values = process_environ if environ is None else environ
    repository_root = _absolute_path(
        values.get("SAKUNA_REPOSITORY_ROOT", REPOSITORY_ROOT),
        base=REPOSITORY_ROOT,
    )
    etl_root = repository_root / "etl"
    selected = profile or values.get("SAKUNA_ETL_PROFILE", DeploymentProfile.LOCAL.value)
    try:
        deployment_profile = (
            selected
            if isinstance(selected, DeploymentProfile)
            else DeploymentProfile(selected.strip().lower())
        )
    except ValueError as error:
        choices = ", ".join(PROFILE_CHOICES)
        raise ValueError(f"Unknown ETL profile {selected!r}; expected one of: {choices}") from error

    data_root = _absolute_path(
        values.get("SAKUNA_DATA_ROOT", repository_root / "data"),
        base=repository_root,
    )
    logs_root = _absolute_path(
        values.get("SAKUNA_LOGS_ROOT", repository_root / "logs"),
        base=repository_root,
    )
    ontology_root = _absolute_path(
        values.get("SAKUNA_ONTOLOGY_ROOT", repository_root / "ontology"),
        base=repository_root,
    )
    constants_root = _absolute_path(
        values.get("SAKUNA_CONSTANTS_ROOT", repository_root / "constants"),
        base=repository_root,
    )
    debug_value = values.get("SAKUNA_DEBUG_ROOT")
    debug_root = (
        _absolute_path(debug_value, base=repository_root)
        if debug_value and debug_value.strip()
        else None
    )
    artifact_root = _absolute_path(
        values.get("SAKUNA_ARTIFACT_ROOT", data_root / "artifacts"),
        base=repository_root,
    )

    quality_minimum_records = int(values.get("SAKUNA_QUALITY_MINIMUM_RECORDS", "1"))
    quality_maximum_rejected_records = int(
        values.get("SAKUNA_QUALITY_MAXIMUM_REJECTED_RECORDS", "0")
    )
    quality_maximum_rejected_ratio = float(
        values.get("SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO", "0")
    )
    quality_fail_on_unexpected_columns = _boolean(
        values.get("SAKUNA_QUALITY_FAIL_ON_UNEXPECTED_COLUMNS", "true"),
        name="SAKUNA_QUALITY_FAIL_ON_UNEXPECTED_COLUMNS",
    )
    if quality_minimum_records < 0 or quality_maximum_rejected_records < 0:
        raise ValueError("quality record thresholds cannot be negative")
    if not 0.0 <= quality_maximum_rejected_ratio <= 1.0:
        raise ValueError("SAKUNA_QUALITY_MAXIMUM_REJECTED_RATIO must be between 0 and 1")

    return EtlSettings(
        profile=deployment_profile,
        paths=EtlPaths(
            repository_root=repository_root,
            etl_root=etl_root,
            data_root=data_root,
            logs_root=logs_root,
            ontology_root=ontology_root,
            constants_root=constants_root,
            debug_root=debug_root,
        ),
        graphdb_host=values.get("GRAPHDB_HOST", "http://localhost:7200").rstrip("/"),
        graphdb_repository=values.get("GRAPHDB_REPOSITORY", "sakunagraph"),
        artifact_root=artifact_root,
        object_bucket=values.get("SAKUNA_OBJECT_BUCKET") or None,
        object_prefix=values.get("SAKUNA_OBJECT_PREFIX", "sakunagraph").strip("/"),
        object_endpoint=values.get("SAKUNA_OBJECT_ENDPOINT") or None,
        object_region=values.get("AWS_REGION", values.get("AWS_DEFAULT_REGION", "ap-southeast-1")),
        quality_minimum_records=quality_minimum_records,
        quality_maximum_rejected_records=quality_maximum_rejected_records,
        quality_maximum_rejected_ratio=quality_maximum_rejected_ratio,
        quality_fail_on_unexpected_columns=quality_fail_on_unexpected_columns,
    )


SETTINGS = load_settings()

__all__ = [
    "DeploymentProfile",
    "EtlPaths",
    "EtlSettings",
    "ETL_ROOT",
    "PROFILE_CHOICES",
    "PROJECT_ROOT",
    "REPOSITORY_ROOT",
    "SETTINGS",
    "load_settings",
]
