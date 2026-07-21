"""Serializable run manifests independent of the backing storage."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from typing import Any, Mapping, Protocol, runtime_checkable

from .storage import Storage, StorageKey


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    path: str
    media_type: str
    size_bytes: int | None = None
    sha256: str | None = None
    role: str = "output"
    validation_status: str = "NOT_RUN"
    code_version: str | None = None
    original_path: str | None = None
    storage_uri: str | None = None
    created_at: str | None = None
    logical_path: str | None = None
    graph_context: str | None = None


@dataclass(frozen=True, slots=True)
class RunManifest:
    run_id: str
    pipeline: str
    created_at: str
    profile: str
    artifacts: tuple[ArtifactManifest, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
    code_version: str | None = None
    status: str = "COMPLETED"
    schema_version: int = 2

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RunManifest":
        return cls(
            run_id=str(value["run_id"]),
            pipeline=str(value["pipeline"]),
            created_at=str(value["created_at"]),
            profile=str(value["profile"]),
            artifacts=tuple(
                ArtifactManifest(**artifact)
                for artifact in value.get("artifacts", ())
            ),
            metadata=dict(value.get("metadata", {})),
            code_version=(
                str(value["code_version"])
                if value.get("code_version") is not None
                else None
            ),
            status=str(value.get("status", "COMPLETED")),
            schema_version=int(value.get("schema_version", 1)),
        )


@runtime_checkable
class ManifestStore(Protocol):
    def load(self) -> RunManifest | None: ...

    def save(self, manifest: RunManifest) -> None: ...


class JsonManifestStore:
    def __init__(self, storage: Storage, key: StorageKey) -> None:
        self.storage = storage
        self.key = key

    def load(self) -> RunManifest | None:
        if not self.storage.exists(self.key):
            return None
        return RunManifest.from_dict(json.loads(self.storage.read_text(self.key)))

    def save(self, manifest: RunManifest) -> None:
        payload = json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n"
        self.storage.write_text(self.key, payload, atomic=True)


__all__ = [
    "ArtifactManifest",
    "JsonManifestStore",
    "ManifestStore",
    "RunManifest",
]
