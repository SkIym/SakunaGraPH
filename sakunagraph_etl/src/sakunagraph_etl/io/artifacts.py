"""Content-addressed ETL runs, immutable artifacts, and quarantine."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sakunagraph_etl import __version__
from sakunagraph_etl.config import PROFILE_CHOICES, EtlSettings, load_settings

from .manifests import ArtifactManifest, RunManifest
from .storage import (
    LocalFileStorage,
    NetworkFileStorage,
    Storage,
    StorageConflictError,
    storage_for_profile,
)


class ArtifactIntegrityError(RuntimeError):
    """Raised when a manifest checksum does not match its artifact."""


@dataclass(frozen=True, slots=True)
class ArtifactRunResult:
    run_id: str
    manifest_key: str
    manifest: RunManifest


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _media_type(path: Path) -> str:
    if path.suffix.lower() in {".ttl", ".n3"}:
        return "text/turtle"
    if path.suffix.lower() in {".xlsx", ".xlsm"}:
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def _expand_files(paths: Iterable[str | Path]) -> tuple[tuple[Path, str], ...]:
    files: dict[str, Path] = {}
    for supplied in paths:
        path = Path(supplied).expanduser().resolve(strict=True)
        if path.is_dir():
            for candidate in path.rglob("*"):
                if candidate.is_file():
                    logical = (Path(path.name) / candidate.relative_to(path)).as_posix()
                    if logical in files and files[logical] != candidate:
                        raise ValueError(f"Duplicate logical artifact path: {logical}")
                    files[logical] = candidate
        else:
            if path.name in files and files[path.name] != path:
                raise ValueError(f"Duplicate logical artifact path: {path.name}")
            files[path.name] = path
    return tuple(sorted(((path, logical) for logical, path in files.items()), key=lambda item: item[1]))


def stable_run_id(
    pipeline: str,
    inputs: Iterable[tuple[str, str, int]],
    *,
    parameters: Mapping[str, Any] | None = None,
    code_version: str = __version__,
) -> str:
    """Create a retry-stable run ID from content, code, and output parameters."""
    identity = {
        "pipeline": pipeline,
        "code_version": code_version,
        "inputs": sorted(inputs),
        "parameters": dict(parameters or {}),
    }
    digest = sha256_bytes(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )
    return f"{pipeline}-{digest[:24]}"


def _storage_uri(storage: Storage, key: str) -> str:
    uri_for = getattr(storage, "uri_for", None)
    if callable(uri_for):
        return str(uri_for(key))
    path_for = getattr(storage, "path_for", None)
    if callable(path_for):
        return str(path_for(key))
    return key


def _write_immutable(storage: Storage, key: str, value: bytes) -> None:
    storage.write_once(key, value)
    stored = storage.read_bytes(key)
    if sha256_bytes(stored) != sha256_bytes(value):
        raise ArtifactIntegrityError(f"Stored artifact checksum mismatch: {key}")


def _emit_task_artifact_result(
    result: ArtifactRunResult,
    storage: Storage,
) -> None:
    """Append a small task envelope when a workflow requested one.

    Source jobs remain unaware of orchestration. The workflow process supplies
    a unique result-file path, and this artifact boundary reports only stable
    run metadata after the immutable manifest has been committed.
    """
    requested = os.getenv("SAKUNA_TASK_RESULT_FILE")
    if not requested:
        return
    result_path = Path(requested).expanduser().resolve(strict=False)
    manifest_path: str | None = None
    path_for = getattr(storage, "path_for", None)
    if callable(path_for):
        manifest_path = str(path_for(result.manifest_key))
    item = {
        "run_id": result.run_id,
        "pipeline": result.manifest.pipeline,
        "status": result.manifest.status,
        "manifest_key": result.manifest_key,
        "manifest_uri": _storage_uri(storage, result.manifest_key),
        "manifest_path": manifest_path,
        "validation_statuses": sorted(
            {
                artifact.validation_status
                for artifact in result.manifest.artifacts
                if artifact.role in {"output", "quarantine"}
            }
        ),
    }
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_storage = NetworkFileStorage(result_path.parent)
    with result_storage.locked(result_path.name):
        if result_path.is_file():
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        else:
            payload = {"schema_version": 1, "artifacts": []}
        if item not in payload["artifacts"]:
            payload["artifacts"].append(item)
        value = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        LocalFileStorage.write_bytes(
            result_storage,
            result_path.name,
            value,
            atomic=True,
        )


def _default_storage(
    settings: EtlSettings,
    output_files: tuple[tuple[Path, str], ...],
) -> Storage:
    if settings.profile.value != "local":
        return storage_for_profile(settings)
    if output_files and all(
        output == settings.paths.data_root or settings.paths.data_root in output.parents
        for output, _ in output_files
    ):
        return storage_for_profile(settings)
    root = output_files[0][0].parent / ".artifacts" if output_files else settings.artifact_root
    return LocalFileStorage(root)


def _default_graph_context(pipeline: str, path: Path) -> str | None:
    """Return the stable named graph for known RDF-producing stages."""
    if path.suffix.lower() not in {".ttl", ".n3"}:
        return None
    if pipeline in {"dromic", "emdat", "gda", "ndrrmc"}:
        return f"https://sakuna.ph/events/{pipeline}"
    return {
        "alignment": "https://sakuna.ph/resolution",
        "organization-registry": "https://sakuna.ph/orgs",
        "psgc": "https://sakuna.ph/psgc",
    }.get(pipeline)


def record_artifact_run(
    pipeline: str,
    *,
    input_paths: Iterable[str | Path],
    output_paths: Iterable[str | Path],
    validation_status: str,
    settings: EtlSettings,
    storage: Storage | None = None,
    parameters: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    code_version: str = __version__,
) -> ArtifactRunResult:
    """Snapshot one reproducible run and persist its manifest last."""
    inputs = _expand_files(input_paths)
    outputs = _expand_files(output_paths)
    input_identity = tuple(
        (logical, sha256_file(path), path.stat().st_size)
        for path, logical in inputs
    )
    run_id = stable_run_id(
        pipeline,
        input_identity,
        parameters=parameters,
        code_version=code_version,
    )
    destination = storage or _default_storage(settings, outputs)
    normalized_validation = validation_status.upper()
    quarantined = normalized_validation == "FAILED"
    base_key = (
        f"quarantine/{pipeline}/{run_id}"
        if quarantined
        else f"runs/{pipeline}/{run_id}"
    )
    manifest_key = f"{base_key}/manifest.json"

    if destination.exists(manifest_key):
        existing = RunManifest.from_dict(json.loads(destination.read_text(manifest_key)))
        for artifact in existing.artifacts:
            if not artifact.sha256:
                continue
            if sha256_bytes(destination.read_bytes(artifact.path)) != artifact.sha256:
                raise ArtifactIntegrityError(
                    f"Existing run artifact failed checksum verification: {artifact.path}"
                )
        supplied_output_digests = {logical: sha256_file(path) for path, logical in outputs}
        existing_output_digests = {
            artifact.logical_path or Path(artifact.original_path or artifact.path).name: artifact.sha256
            for artifact in existing.artifacts
            if artifact.role in {"output", "quarantine"}
        }
        if supplied_output_digests != existing_output_digests:
            raise StorageConflictError(
                f"Retry output differs from immutable run {existing.run_id}"
            )
        result = ArtifactRunResult(run_id, manifest_key, existing)
        _emit_task_artifact_result(result, destination)
        return result

    created_at = datetime.now(timezone.utc).isoformat()
    artifacts: list[ArtifactManifest] = []
    for path, logical in inputs:
        value = path.read_bytes()
        key = f"{base_key}/inputs/{logical}"
        _write_immutable(destination, key, value)
        artifacts.append(
            ArtifactManifest(
                path=key,
                media_type=_media_type(path),
                size_bytes=len(value),
                sha256=sha256_bytes(value),
                role="input",
                validation_status="NOT_APPLICABLE",
                code_version=code_version,
                original_path=str(path),
                storage_uri=_storage_uri(destination, key),
                created_at=created_at,
                logical_path=logical,
                graph_context=None,
            )
        )

    output_role = "quarantine" if quarantined else "output"
    for path, logical in outputs:
        value = path.read_bytes()
        key = f"{base_key}/outputs/{logical}"
        _write_immutable(destination, key, value)
        artifacts.append(
            ArtifactManifest(
                path=key,
                media_type=_media_type(path),
                size_bytes=len(value),
                sha256=sha256_bytes(value),
                role=output_role,
                validation_status=normalized_validation,
                code_version=code_version,
                original_path=str(path),
                storage_uri=_storage_uri(destination, key),
                created_at=created_at,
                logical_path=logical,
                graph_context=_default_graph_context(pipeline, path),
            )
        )

    manifest = RunManifest(
        run_id=run_id,
        pipeline=pipeline,
        created_at=created_at,
        profile=settings.profile.value,
        artifacts=tuple(artifacts),
        metadata={"parameters": dict(parameters or {}), **dict(metadata or {})},
        code_version=code_version,
        status="QUARANTINED" if quarantined else "COMPLETED",
        schema_version=2,
    )
    payload = json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _write_immutable(destination, manifest_key, payload)
    result = ArtifactRunResult(run_id, manifest_key, manifest)
    _emit_task_artifact_result(result, destination)
    return result


def verify_run_manifest(manifest: RunManifest, storage: Storage) -> None:
    for artifact in manifest.artifacts:
        if not artifact.sha256:
            raise ArtifactIntegrityError(f"Artifact has no checksum: {artifact.path}")
        actual = sha256_bytes(storage.read_bytes(artifact.path))
        if actual != artifact.sha256:
            raise ArtifactIntegrityError(
                f"Checksum mismatch for {artifact.path}: {actual} != {artifact.sha256}"
            )


def materialize_manifest_artifacts(
    manifest: RunManifest,
    storage: Storage,
    destination: str | Path,
    *,
    roles: Iterable[str],
) -> tuple[Path, ...]:
    target_root = Path(destination).expanduser().resolve(strict=False)
    target_root.mkdir(parents=True, exist_ok=True)
    selected_roles = set(roles)
    outputs: list[Path] = []
    for artifact in manifest.artifacts:
        if artifact.role not in selected_roles:
            continue
        output = target_root / (artifact.logical_path or Path(artifact.path).name)
        output.parent.mkdir(parents=True, exist_ok=True)
        value = storage.read_bytes(artifact.path)
        if artifact.sha256 and sha256_bytes(value) != artifact.sha256:
            raise ArtifactIntegrityError(f"Checksum mismatch for {artifact.path}")
        LocalFileStorage(target_root).write_once(output.relative_to(target_root), value)
        outputs.append(output)
    return tuple(outputs)


def materialize_manifest_inputs(
    manifest: RunManifest,
    storage: Storage,
    destination: str | Path,
) -> tuple[Path, ...]:
    return materialize_manifest_artifacts(
        manifest,
        storage,
        destination,
        roles=("input",),
    )


def local_artifacts_from_manifest(
    path: str | Path,
    *,
    roles: Iterable[str],
) -> tuple[tuple[ArtifactManifest, Path], ...]:
    """Resolve verified local artifacts while retaining manifest metadata."""
    manifest_path = Path(path).expanduser().resolve(strict=True)
    manifest = RunManifest.from_dict(json.loads(manifest_path.read_text(encoding="utf-8")))
    selected_roles = set(roles)
    parts = manifest_path.parts
    root: Path | None = None
    for marker in ("runs", "quarantine"):
        if marker in parts:
            root = Path(*parts[: parts.index(marker)])
            break
    resolved: list[tuple[ArtifactManifest, Path]] = []
    for artifact in manifest.artifacts:
        if artifact.role not in selected_roles:
            continue
        candidates = []
        if artifact.original_path:
            candidates.append(Path(artifact.original_path))
        candidates.append(Path(artifact.path))
        if root is not None:
            candidates.append(root / artifact.path)
        selected = next((candidate for candidate in candidates if candidate.is_file()), None)
        if selected is None:
            raise FileNotFoundError(f"Manifest input is not locally available: {artifact.path}")
        if artifact.sha256 and sha256_file(selected) != artifact.sha256:
            raise ArtifactIntegrityError(f"Manifest input checksum mismatch: {selected}")
        resolved.append((artifact, selected.resolve()))
    if not resolved:
        raise ValueError(f"Manifest contains no artifacts with roles {selected_roles}: {manifest_path}")
    return tuple(resolved)


def local_artifact_paths_from_manifest(
    path: str | Path,
    *,
    roles: Iterable[str],
) -> tuple[Path, ...]:
    """Resolve and checksum-verify local artifacts from a Stage 6 manifest."""
    return tuple(
        artifact_path
        for _, artifact_path in local_artifacts_from_manifest(path, roles=roles)
    )


def local_input_paths_from_manifest(path: str | Path) -> tuple[Path, ...]:
    return local_artifact_paths_from_manifest(path, roles=("input",))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify or materialize immutable ETL runs.")
    parser.add_argument("action", choices=("verify", "materialize"))
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--profile", choices=PROFILE_CHOICES)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings(args.profile)
    storage = storage_for_profile(settings)
    manifest = RunManifest.from_dict(json.loads(storage.read_text(args.manifest_key)))
    verify_run_manifest(manifest, storage)
    if args.action == "materialize":
        if args.destination is None:
            build_parser().error("--destination is required for materialize")
        materialize_manifest_inputs(manifest, storage, args.destination)
    return 0


__all__ = [
    "ArtifactIntegrityError",
    "ArtifactRunResult",
    "local_artifacts_from_manifest",
    "local_artifact_paths_from_manifest",
    "local_input_paths_from_manifest",
    "main",
    "materialize_manifest_artifacts",
    "materialize_manifest_inputs",
    "record_artifact_run",
    "sha256_bytes",
    "sha256_file",
    "stable_run_id",
    "verify_run_manifest",
]


if __name__ == "__main__":
    raise SystemExit(main())
