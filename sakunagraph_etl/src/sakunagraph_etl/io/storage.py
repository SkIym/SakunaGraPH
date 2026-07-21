"""Artifact storage implementations for local, network, and cloud profiles."""

from __future__ import annotations

import os
from contextlib import contextmanager
import hashlib
from pathlib import Path
from pathlib import PurePosixPath
import tempfile
import time
from typing import Any, Iterator, Protocol, runtime_checkable


StorageKey = str | os.PathLike[str]


@runtime_checkable
class Storage(Protocol):
    """Minimal artifact storage required by ETL stages."""

    def exists(self, key: StorageKey) -> bool: ...

    def iter_files(self, prefix: StorageKey = "", pattern: str = "**/*") -> tuple[Path, ...]: ...

    def read_bytes(self, key: StorageKey) -> bytes: ...

    def write_bytes(self, key: StorageKey, value: bytes, *, atomic: bool = True) -> Path: ...

    def write_once(self, key: StorageKey, value: bytes) -> Path: ...

    def read_text(self, key: StorageKey, *, encoding: str = "utf-8") -> str: ...

    def write_text(
        self,
        key: StorageKey,
        value: str,
        *,
        encoding: str = "utf-8",
        atomic: bool = True,
    ) -> Path: ...


class StorageConflictError(RuntimeError):
    """Raised when immutable storage already contains different bytes."""


class LocalFileStorage:
    """Filesystem storage rooted at one validated directory."""

    def __init__(self, root: StorageKey) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)

    def path_for(self, key: StorageKey = "") -> Path:
        key_path = Path(key)
        candidate = (
            key_path.resolve(strict=False)
            if key_path.is_absolute()
            else (self.root / key_path).resolve(strict=False)
        )
        if candidate != self.root and self.root not in candidate.parents:
            raise ValueError(f"Storage key escapes configured root: {key}")
        return candidate

    def exists(self, key: StorageKey) -> bool:
        return self.path_for(key).exists()

    def iter_files(self, prefix: StorageKey = "", pattern: str = "**/*") -> tuple[Path, ...]:
        base = self.path_for(prefix)
        if not base.exists():
            return ()
        if base.is_file():
            return (base,)
        return tuple(sorted(path for path in base.glob(pattern) if path.is_file()))

    def read_bytes(self, key: StorageKey) -> bytes:
        return self.path_for(key).read_bytes()

    def write_bytes(self, key: StorageKey, value: bytes, *, atomic: bool = True) -> Path:
        destination = self.path_for(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not atomic:
            destination.write_bytes(value)
            return destination

        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(value)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            temporary_path.replace(destination)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()
        return destination

    def write_once(self, key: StorageKey, value: bytes) -> Path:
        """Create an immutable file, accepting an identical retry."""
        destination = self.path_for(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            if destination.read_bytes() != value:
                raise StorageConflictError(f"Immutable artifact already differs: {destination}")
            return destination

        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=destination.parent,
                prefix=f".{destination.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary:
                temporary.write(value)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            try:
                os.link(temporary_path, destination)
            except FileExistsError:
                if destination.read_bytes() != value:
                    raise StorageConflictError(
                        f"Concurrent immutable artifact differs: {destination}"
                    )
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()
        return destination

    def read_text(self, key: StorageKey, *, encoding: str = "utf-8") -> str:
        return self.path_for(key).read_text(encoding=encoding)

    def write_text(
        self,
        key: StorageKey,
        value: str,
        *,
        encoding: str = "utf-8",
        atomic: bool = True,
    ) -> Path:
        return self.write_bytes(key, value.encode(encoding), atomic=atomic)


class NetworkFileStorage(LocalFileStorage):
    """Filesystem adapter with cross-process locks for shared network mounts."""

    def __init__(
        self,
        root: StorageKey,
        *,
        lock_timeout: float = 30.0,
        stale_lock_seconds: float = 300.0,
    ) -> None:
        super().__init__(root)
        if lock_timeout <= 0:
            raise ValueError("lock_timeout must be positive")
        self.lock_timeout = lock_timeout
        self.stale_lock_seconds = stale_lock_seconds

    def _lock_path(self, key: StorageKey) -> Path:
        digest = hashlib.sha256(os.fspath(key).encode("utf-8")).hexdigest()
        return self.root / ".locks" / f"{digest}.lock"

    @contextmanager
    def locked(self, key: StorageKey) -> Iterator[None]:
        lock_path = self._lock_path(key)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.lock_timeout
        descriptor: int | None = None
        while descriptor is None:
            try:
                descriptor = os.open(
                    lock_path,
                    os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                )
                os.write(descriptor, f"pid={os.getpid()}\n".encode("ascii"))
            except FileExistsError:
                try:
                    age = time.time() - lock_path.stat().st_mtime
                    if age > self.stale_lock_seconds:
                        lock_path.unlink()
                        continue
                except FileNotFoundError:
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for storage lock: {lock_path}")
                time.sleep(0.05)
        try:
            yield
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                lock_path.unlink()
            except FileNotFoundError:
                pass

    def write_bytes(self, key: StorageKey, value: bytes, *, atomic: bool = True) -> Path:
        with self.locked(key):
            return LocalFileStorage.write_bytes(self, key, value, atomic=atomic)

    def write_once(self, key: StorageKey, value: bytes) -> Path:
        with self.locked(key):
            destination = self.path_for(key)
            if destination.exists():
                if destination.read_bytes() != value:
                    raise StorageConflictError(
                        f"Immutable network artifact already differs: {destination}"
                    )
                return destination
            return LocalFileStorage.write_bytes(self, key, value, atomic=True)


class S3ObjectStorage:
    """S3-compatible object adapter with lazy SDK initialization."""

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        endpoint_url: str | None = None,
        region_name: str | None = None,
        client: Any | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("bucket is required")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if client is None:
            try:
                import boto3
            except ImportError as error:
                raise RuntimeError(
                    "Cloud storage requires the pinned cloud dependencies: "
                    "pip install -r requirements-cloud.txt -c constraints.txt"
                ) from error
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region_name,
            )
        self.client = client

    def _key(self, key: StorageKey = "") -> str:
        supplied = os.fspath(key).replace("\\", "/").strip("/")
        parts = PurePosixPath(supplied).parts if supplied else ()
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"Invalid object storage key: {key}")
        return "/".join(part for part in (self.prefix, supplied) if part)

    @staticmethod
    def _error_code(error: Exception) -> str | None:
        response = getattr(error, "response", None)
        if not isinstance(response, dict):
            return None
        return str(response.get("Error", {}).get("Code", ""))

    def exists(self, key: StorageKey) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(key))
        except Exception as error:
            if self._error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def iter_files(self, prefix: StorageKey = "", pattern: str = "**/*") -> tuple[Path, ...]:
        del pattern  # object stores enumerate by prefix; callers filter returned keys
        object_prefix = self._key(prefix)
        paginator = self.client.get_paginator("list_objects_v2")
        paths: list[Path] = []
        root_prefix = f"{self.prefix}/" if self.prefix else ""
        for page in paginator.paginate(Bucket=self.bucket, Prefix=object_prefix):
            for item in page.get("Contents", ()):
                key = str(item["Key"])
                relative = key.removeprefix(root_prefix)
                paths.append(Path(relative))
        return tuple(sorted(paths))

    def read_bytes(self, key: StorageKey) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(key))
        return response["Body"].read()

    def write_bytes(self, key: StorageKey, value: bytes, *, atomic: bool = True) -> Path:
        del atomic  # one S3 PUT is atomically visible
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=value)
        return Path(os.fspath(key))

    def write_once(self, key: StorageKey, value: bytes) -> Path:
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=value,
                IfNoneMatch="*",
            )
        except Exception as error:
            if self._error_code(error) not in {"409", "412", "PreconditionFailed"}:
                raise
            if self.read_bytes(key) != value:
                raise StorageConflictError(
                    f"Immutable object already differs: s3://{self.bucket}/{self._key(key)}"
                ) from error
        return Path(os.fspath(key))

    def read_text(self, key: StorageKey, *, encoding: str = "utf-8") -> str:
        return self.read_bytes(key).decode(encoding)

    def write_text(
        self,
        key: StorageKey,
        value: str,
        *,
        encoding: str = "utf-8",
        atomic: bool = True,
    ) -> Path:
        return self.write_bytes(key, value.encode(encoding), atomic=atomic)

    def uri_for(self, key: StorageKey) -> str:
        return f"s3://{self.bucket}/{self._key(key)}"


def storage_for_profile(settings: Any, *, root: StorageKey | None = None) -> Storage:
    """Create the configured artifact adapter without affecting source inputs."""
    from sakunagraph_etl.config import DeploymentProfile

    selected_root = root or settings.artifact_root
    if settings.profile is DeploymentProfile.LOCAL:
        return LocalFileStorage(selected_root)
    if settings.profile is DeploymentProfile.ONPREM:
        return NetworkFileStorage(selected_root)
    if not settings.object_bucket:
        raise ValueError("SAKUNA_OBJECT_BUCKET is required for the cloud profile")
    return S3ObjectStorage(
        settings.object_bucket,
        prefix=settings.object_prefix,
        endpoint_url=settings.object_endpoint,
        region_name=settings.object_region,
    )


__all__ = [
    "LocalFileStorage",
    "NetworkFileStorage",
    "S3ObjectStorage",
    "Storage",
    "StorageConflictError",
    "StorageKey",
    "storage_for_profile",
]
