"""GraphDB recovery API, backup verification, and manifest-driven rebuilds."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tarfile
import tempfile
from typing import Any, Mapping, Sequence

import requests
from rdflib import Graph

from sakunagraph_etl.config import PROFILE_CHOICES, load_settings
from sakunagraph_etl.io.artifacts import local_artifacts_from_manifest
from sakunagraph_etl.io.storage import LocalFileStorage, NetworkFileStorage


class RecoveryError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BackupMetadata:
    backup_path: str
    created_at: str
    graphdb_host: str
    repositories: tuple[str, ...]
    includes_system_data: bool
    size_bytes: int
    sha256: str
    success_marker_verified: bool
    graphdb_version: str | None = None
    schema_version: int = 1


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _success_marker(path: Path) -> bool | None:
    """Return marker presence, or None when GraphDB's compression is opaque."""
    try:
        with tarfile.open(path, mode="r:*") as archive:
            return any(Path(member.name).name == ".success" for member in archive.getmembers())
    except (tarfile.ReadError, OSError):
        return None


def metadata_path(backup_path: Path) -> Path:
    return backup_path.with_suffix(backup_path.suffix + ".json")


def verify_backup(backup_path: str | Path) -> BackupMetadata:
    path = Path(backup_path).expanduser().resolve(strict=True)
    sidecar = metadata_path(path)
    if not sidecar.is_file():
        raise RecoveryError(f"backup metadata sidecar is missing: {sidecar}")
    value = json.loads(sidecar.read_text(encoding="utf-8"))
    metadata = BackupMetadata(
        backup_path=str(value["backup_path"]),
        created_at=str(value["created_at"]),
        graphdb_host=str(value["graphdb_host"]),
        repositories=tuple(str(item) for item in value.get("repositories", ())),
        includes_system_data=bool(value.get("includes_system_data", False)),
        size_bytes=int(value["size_bytes"]),
        sha256=str(value["sha256"]),
        success_marker_verified=bool(value.get("success_marker_verified", False)),
        graphdb_version=(
            str(value["graphdb_version"])
            if value.get("graphdb_version") is not None
            else None
        ),
        schema_version=int(value.get("schema_version", 1)),
    )
    if path.stat().st_size != metadata.size_bytes:
        raise RecoveryError("backup size differs from its metadata")
    actual = sha256_file(path)
    if actual != metadata.sha256:
        raise RecoveryError(f"backup checksum mismatch: {actual} != {metadata.sha256}")
    marker = _success_marker(path)
    if marker is False:
        raise RecoveryError("GraphDB backup archive has no .success marker")
    return metadata


class GraphDbRecoveryClient:
    def __init__(
        self,
        host: str,
        *,
        session: requests.Session | None = None,
        timeout: int = 3600,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self.host = host.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def backup(
        self,
        output_path: str | Path,
        *,
        repositories: Sequence[str],
        include_system_data: bool,
    ) -> BackupMetadata:
        path = Path(output_path).expanduser().resolve(strict=False)
        payload: dict[str, Any] = {
            "repositories": list(repositories),
            "backupSystemData": include_system_data,
        }
        response = self.session.post(
            f"{self.host}/rest/recovery/backup",
            json=payload,
            timeout=self.timeout,
            stream=True,
        )
        response.raise_for_status()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        digest = hashlib.sha256()
        size_bytes = 0
        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".partial",
                delete=False,
            ) as destination:
                temporary_path = Path(destination.name)
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    destination.write(chunk)
                    digest.update(chunk)
                    size_bytes += len(chunk)
                destination.flush()
                os.fsync(destination.fileno())
            if size_bytes == 0:
                raise RecoveryError("GraphDB returned an empty backup")
            marker = _success_marker(temporary_path)
            if marker is False:
                raise RecoveryError("GraphDB backup archive has no .success marker")

            backup_sha256 = digest.hexdigest()
            storage = NetworkFileStorage(path.parent, lock_timeout=60)
            with storage.locked(path.name):
                sidecar = metadata_path(path)
                if sidecar.is_file() and not path.is_file():
                    raise RecoveryError(f"backup data is missing for metadata sidecar: {sidecar}")
                if path.is_file() and sidecar.is_file():
                    existing = verify_backup(path)
                    if existing.sha256 != backup_sha256:
                        raise RecoveryError(
                            f"backup output path already contains different data: {path}"
                        )
                    return existing
                if path.is_file():
                    if path.stat().st_size != size_bytes or sha256_file(path) != backup_sha256:
                        raise RecoveryError(
                            f"backup output path already contains different data: {path}"
                        )
                else:
                    temporary_path.replace(path)
                    temporary_path = None

                metadata = BackupMetadata(
                    backup_path=str(path),
                    created_at=datetime.now(timezone.utc).isoformat(),
                    graphdb_host=self.host,
                    repositories=tuple(repositories),
                    includes_system_data=include_system_data,
                    size_bytes=size_bytes,
                    sha256=backup_sha256,
                    success_marker_verified=marker is True,
                    graphdb_version=response.headers.get("X-GraphDB-Version"),
                )
                LocalFileStorage(sidecar.parent).write_once(
                    sidecar.name,
                    json.dumps(asdict(metadata), indent=2, sort_keys=True).encode("utf-8")
                    + b"\n",
                )
                return metadata
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()

    def restore(
        self,
        backup_path: str | Path,
        *,
        repositories: Sequence[str],
        restore_system_data: bool,
        remove_stale_repositories: bool,
    ) -> Mapping[str, Any] | str:
        path = Path(backup_path).expanduser().resolve(strict=True)
        verify_backup(path)
        params = {
            "repositories": list(repositories),
            "restoreSystemData": restore_system_data,
            "removeStaleRepositories": remove_stale_repositories,
        }
        with path.open("rb") as source:
            response = self.session.post(
                f"{self.host}/rest/recovery/restore",
                files={
                    "params": (None, json.dumps(params), "application/json"),
                    "file": (path.name, source, "application/x-tar"),
                },
                timeout=self.timeout,
            )
        response.raise_for_status()
        try:
            return response.json()
        except (requests.JSONDecodeError, ValueError):
            return response.text

    def status(self) -> Mapping[str, Any]:
        response = self.session.get(
            f"{self.host}/rest/monitor/backup",
            timeout=min(self.timeout, 30),
        )
        response.raise_for_status()
        return response.json()


def _preflight_manifests(paths: Sequence[Path]) -> None:
    for manifest_path in paths:
        artifacts = local_artifacts_from_manifest(
            manifest_path,
            roles=("output", "quarantine"),
        )
        for artifact, path in artifacts:
            if artifact.role == "quarantine" or artifact.validation_status == "FAILED":
                raise RecoveryError(f"failed artifact cannot be rebuilt: {artifact.path}")
            if path.suffix.lower() == ".ttl":
                Graph().parse(path, format="turtle")


def full_rebuild(
    manifests: Sequence[Path],
    *,
    host: str,
    repository: str,
    profile: str,
    timeout: int,
) -> int:
    """Rebuild a maintenance repository after every manifest passes preflight."""
    _preflight_manifests(manifests)
    from sakunagraph_etl.io import graphdb

    for index, manifest in enumerate(manifests):
        arguments = [
            "--input-manifest", str(manifest),
            "--host", host,
            "--repo", repository,
            "--profile", profile,
            "--validate",
            "--timeout", str(timeout),
        ]
        if index == 0:
            arguments.append("--clear-repository")
        else:
            arguments.append("--replace")
        code = graphdb.main(arguments)
        if code:
            return code
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GraphDB backup, restore, and rebuild tools.")
    parser.add_argument("--host")
    parser.add_argument("--repo")
    parser.add_argument("--profile", choices=PROFILE_CHOICES)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--timeout", type=int, default=3600)
    subparsers = parser.add_subparsers(dest="action", required=True)

    backup = subparsers.add_parser("backup")
    backup.add_argument("--out", type=Path, required=True)
    backup.add_argument("--include-system-data", action="store_true")

    verify = subparsers.add_parser("verify")
    verify.add_argument("backup", type=Path)

    restore = subparsers.add_parser("restore")
    restore.add_argument("backup", type=Path)
    restore.add_argument("--restore-system-data", action="store_true")
    restore.add_argument("--remove-stale-repositories", action="store_true")
    restore.add_argument("--confirm-repository", required=True)

    subparsers.add_parser("status")

    rebuild = subparsers.add_parser("full-rebuild")
    rebuild.add_argument("--manifest", type=Path, action="append", required=True)
    rebuild.add_argument("--target-repository", required=True)
    rebuild.add_argument("--confirm-target", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings(args.profile)
    host = (args.host or settings.graphdb_host).rstrip("/")
    repository = args.repo or settings.graphdb_repository
    if args.timeout <= 0:
        print("ERROR: --timeout must be positive")
        return 2
    if args.action == "verify":
        try:
            print(json.dumps(asdict(verify_backup(args.backup)), indent=2, sort_keys=True))
            return 0
        except (OSError, ValueError, RecoveryError) as error:
            print(f"ERROR: {error}")
            return 2
    if args.action == "full-rebuild":
        if args.confirm_target != args.target_repository:
            print("ERROR: --confirm-target must exactly match --target-repository")
            return 2
        try:
            return full_rebuild(
                args.manifest,
                host=host,
                repository=args.target_repository,
                profile=settings.profile.value,
                timeout=args.timeout,
            )
        except (OSError, ValueError, RecoveryError) as error:
            print(f"ERROR: {error}")
            return 2

    session = requests.Session()
    username = args.username or os.getenv("GRAPHDB_USERNAME")
    password = args.password or os.getenv("GRAPHDB_PASSWORD")
    if username:
        session.auth = (username, password or "")
    client = GraphDbRecoveryClient(host, session=session, timeout=args.timeout)
    try:
        if args.action == "backup":
            metadata = client.backup(
                args.out,
                repositories=(repository,),
                include_system_data=args.include_system_data,
            )
            print(json.dumps(asdict(metadata), indent=2, sort_keys=True))
        elif args.action == "restore":
            if args.confirm_repository != repository:
                raise RecoveryError(
                    "--confirm-repository must exactly match the configured repository"
                )
            result = client.restore(
                args.backup,
                repositories=(repository,),
                restore_system_data=args.restore_system_data,
                remove_stale_repositories=args.remove_stale_repositories,
            )
            print(json.dumps(result, indent=2, sort_keys=True) if isinstance(result, dict) else result)
        else:
            print(json.dumps(client.status(), indent=2, sort_keys=True))
        return 0
    except (OSError, requests.RequestException, ValueError, RecoveryError) as error:
        print(f"ERROR: {error}")
        return 1
    finally:
        session.close()


__all__ = [
    "BackupMetadata",
    "GraphDbRecoveryClient",
    "RecoveryError",
    "full_rebuild",
    "main",
    "verify_backup",
]


if __name__ == "__main__":
    raise SystemExit(main())
