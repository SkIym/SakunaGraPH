"""Workflow, backfill, and managed-container task command line interface."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import unquote, urlparse

from sakunagraph_etl.config import PROFILE_CHOICES, load_settings
from sakunagraph_etl.io import (
    LocalFileStorage,
    RunManifest,
    S3ObjectStorage,
    materialize_manifest_artifacts,
    verify_run_manifest,
)

from .catalog import WORKFLOWS, get_workflow
from .models import TaskSpec, TaskStatus, WorkflowSpec
from .observability import configure_structured_logging, safe_identifier
from .runner import WorkflowError, WorkflowRunner, backfill_dates


def _parameters(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        key, separator, supplied = value.partition("=")
        if not separator or not key:
            raise ValueError(f"workflow parameter must use KEY=VALUE: {value}")
        parsed[key] = supplied
    return parsed


def _load_parameters(values: Sequence[str], path: Path | None) -> dict[str, str]:
    file_values: dict[str, str] = {}
    if path is not None:
        payload = json.loads(path.expanduser().resolve(strict=True).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("workflow parameter file must contain a JSON object")
        file_values = {str(key): str(value) for key, value in payload.items()}
    return {**file_values, **_parameters(values)}


def _s3_storage(uri: str) -> tuple[S3ObjectStorage, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"invalid S3 URI: {uri}")
    return S3ObjectStorage(parsed.netloc), parsed.path.lstrip("/")


def _read_uri(uri: str) -> bytes:
    if uri.startswith("s3://"):
        storage, key = _s3_storage(uri)
        return storage.read_bytes(key)
    parsed = urlparse(uri)
    path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(uri)
    return path.expanduser().resolve(strict=True).read_bytes()


def _write_uri(uri: str, value: bytes) -> None:
    if uri.startswith("s3://"):
        storage, key = _s3_storage(uri)
        # Result envelopes are small, versioned pointers. They are deliberately
        # replaceable so a managed retry can advance FAILED to COMPLETED while
        # the referenced artifact manifests remain immutable.
        storage.write_bytes(key, value)
        return
    parsed = urlparse(uri)
    path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(uri)
    path = path.expanduser().resolve(strict=False)
    LocalFileStorage(path.parent).write_bytes(path.name, value, atomic=True)


def _completed_result(uri: str) -> bool:
    if uri.startswith("s3://"):
        storage, key = _s3_storage(uri)
        if not storage.exists(key):
            return False
    else:
        parsed = urlparse(uri)
        path = Path(unquote(parsed.path)) if parsed.scheme == "file" else Path(uri)
        if not path.expanduser().resolve(strict=False).is_file():
            return False
    try:
        payload = json.loads(_read_uri(uri))
        if payload.get("status") != TaskStatus.COMPLETED.value:
            return False
        artifact_values: list[Mapping[str, Any]] = list(payload.get("artifacts", ()))
        for task in payload.get("tasks", {}).values():
            if task.get("status") != TaskStatus.COMPLETED.value:
                return False
            artifact_values.extend(task.get("artifacts", ()))
        for artifact_value in artifact_values:
            if artifact_value.get("status") != "COMPLETED":
                return False
            if "FAILED" in artifact_value.get("validation_statuses", ()):
                return False
            manifest, storage = _manifest_storage(
                str(artifact_value["manifest_uri"]),
                (
                    str(artifact_value["manifest_path"])
                    if artifact_value.get("manifest_path")
                    else None
                ),
            )
            verify_run_manifest(manifest, storage)
            if manifest.status != "COMPLETED":
                return False
    except Exception:
        # A missing, corrupt, expired, or unreachable checkpoint must be rerun.
        return False
    return True


def _local_artifact_root(path: Path) -> Path:
    resolved = path.resolve()
    for index, part in enumerate(resolved.parts):
        if part in {"runs", "quarantine"}:
            return Path(*resolved.parts[:index])
    return resolved.parent


def _manifest_storage(uri: str, manifest_path: str | None):
    if manifest_path and Path(manifest_path).is_file():
        path = Path(manifest_path).resolve()
        return (
            RunManifest.from_dict(json.loads(path.read_text(encoding="utf-8"))),
            LocalFileStorage(_local_artifact_root(path)),
        )
    if uri.startswith("s3://"):
        storage, key = _s3_storage(uri)
        return RunManifest.from_dict(json.loads(storage.read_text(key))), storage
    path = Path(uri).expanduser().resolve(strict=True)
    return (
        RunManifest.from_dict(json.loads(path.read_text(encoding="utf-8"))),
        LocalFileStorage(_local_artifact_root(path)),
    )


def materialize_result_dependencies(
    named_uris: Mapping[str, str],
    work_dir: str | Path,
) -> dict[str, Path]:
    """Materialize upstream output manifests from small task result envelopes."""
    root = Path(work_dir).expanduser().resolve(strict=False)
    manifests: dict[str, Path] = {}
    for name, result_uri in named_uris.items():
        payload = json.loads(_read_uri(result_uri))
        if payload.get("status") != TaskStatus.COMPLETED.value:
            raise WorkflowError(f"dependency {name} has non-completed task status")
        artifact_values: list[Mapping[str, Any]] = []
        if "tasks" in payload:
            for task in payload["tasks"].values():
                if task.get("status") != TaskStatus.COMPLETED.value:
                    raise WorkflowError(f"dependency {name} contains a failed task")
                artifact_values.extend(task.get("artifacts", ()))
        artifact_values.extend(payload.get("artifacts", ()))
        if not artifact_values:
            raise WorkflowError(f"dependency {name} has no artifact results")
        for index, artifact_value in enumerate(artifact_values):
            manifest, storage = _manifest_storage(
                str(artifact_value["manifest_uri"]),
                (
                    str(artifact_value["manifest_path"])
                    if artifact_value.get("manifest_path")
                    else None
                ),
            )
            verify_run_manifest(manifest, storage)
            if manifest.status != "COMPLETED":
                raise WorkflowError(
                    f"dependency {name} contains non-publishable run {manifest.run_id}"
                )
            destination = root / safe_identifier(name) / str(index) / "outputs"
            materialized = materialize_manifest_artifacts(
                manifest,
                storage,
                destination,
                roles=("output",),
            )
            outputs = [artifact for artifact in manifest.artifacts if artifact.role == "output"]
            replacements = {
                artifact.path: str(path)
                for artifact, path in zip(outputs, materialized, strict=True)
            }
            adapted = replace(
                manifest,
                artifacts=tuple(
                    replace(
                        artifact,
                        original_path=replacements.get(artifact.path, artifact.original_path),
                    )
                    for artifact in manifest.artifacts
                ),
            )
            manifest_dir = root / safe_identifier(name) / str(index)
            manifest_path = manifest_dir / "manifest.json"
            LocalFileStorage(manifest_dir).write_bytes(
                manifest_path.name,
                json.dumps(adapted.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n",
                atomic=True,
            )
            manifests[f"{name}:{index}"] = manifest_path
    return manifests


def _dependency_values(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        name, separator, uri = value.partition("=")
        if not separator or not name or not uri:
            raise ValueError(f"dependency must use NAME=RESULT_URI: {value}")
        parsed[name] = uri
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run resumable SakunaGraPH workflows.")
    subparsers = parser.add_subparsers(dest="action", required=True)

    subparsers.add_parser("list", help="List package-owned workflow definitions.")

    run_parser = subparsers.add_parser("run", help="Run or resume one workflow.")
    run_parser.add_argument("workflow", choices=sorted(WORKFLOWS))
    run_parser.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    run_parser.add_argument("--params-file", type=Path)
    run_parser.add_argument("--scheduled-for")
    run_parser.add_argument("--run-id")
    run_parser.add_argument("--state-dir", type=Path)
    run_parser.add_argument("--profile", choices=PROFILE_CHOICES)
    run_parser.add_argument("--no-resume", action="store_true")
    run_parser.add_argument("--result-file", type=Path)

    backfill = subparsers.add_parser("backfill", help="Run one daily data interval at a time.")
    backfill.add_argument("workflow", choices=sorted(WORKFLOWS))
    backfill.add_argument("--start", type=date.fromisoformat, required=True)
    backfill.add_argument("--end", type=date.fromisoformat, required=True)
    backfill.add_argument("--param", action="append", default=[], metavar="KEY=VALUE")
    backfill.add_argument("--params-file", type=Path)
    backfill.add_argument("--state-dir", type=Path)
    backfill.add_argument("--profile", choices=PROFILE_CHOICES)
    backfill.add_argument("--continue-on-error", action="store_true")

    task = subparsers.add_parser(
        "task",
        help="Run one CLI task and persist its small result envelope locally or to S3.",
    )
    task.add_argument("--task-id", required=True)
    task.add_argument("--workflow", default="managed-task")
    task.add_argument("--run-id", required=True)
    task.add_argument("--result-uri", required=True)
    task.add_argument("--input-result", action="append", default=[], metavar="NAME=URI")
    task.add_argument("--work-dir", type=Path, default=Path("/tmp/sakuna-task"))
    task.add_argument("--timeout", type=int, default=14400)
    task.add_argument("--retries", type=int, default=2)
    task.add_argument("--profile", choices=PROFILE_CHOICES)
    task.add_argument("--no-artifacts", action="store_true")
    task.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def _write_state(path: Path, value: Mapping[str, Any]) -> None:
    path = path.expanduser().resolve(strict=False)
    LocalFileStorage(path.parent).write_bytes(
        path.name,
        json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n",
        atomic=True,
    )


def main(argv: Sequence[str] | None = None) -> int:
    configure_structured_logging()
    args = build_parser().parse_args(argv)
    if args.action == "list":
        for workflow in WORKFLOWS.values():
            schedule = workflow.schedule.aws_expression if workflow.schedule else "manual"
            print(f"{workflow.name}\t{schedule}\t{workflow.description}")
        return 0

    settings = load_settings(args.profile)
    try:
        if args.action == "run":
            runner = WorkflowRunner(settings, state_root=args.state_dir)
            state = runner.run(
                get_workflow(args.workflow),
                parameters=_load_parameters(args.param, args.params_file),
                scheduled_for=args.scheduled_for,
                run_id=args.run_id,
                resume=not args.no_resume,
            )
            if args.result_file:
                _write_state(args.result_file, state.to_dict())
            print(json.dumps(state.to_dict(), sort_keys=True))
            return 0 if state.status is TaskStatus.COMPLETED else 1

        if args.action == "backfill":
            runner = WorkflowRunner(settings, state_root=args.state_dir)
            failures = 0
            parameters = _load_parameters(args.param, args.params_file)
            for interval in backfill_dates(args.start, args.end):
                scheduled = datetime.combine(
                    interval,
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                ).isoformat()
                state = runner.run(
                    get_workflow(args.workflow),
                    parameters={**parameters, "data_interval": interval.isoformat()},
                    scheduled_for=scheduled,
                )
                if state.status is TaskStatus.FAILED:
                    failures += 1
                    if not args.continue_on_error:
                        break
            return 1 if failures else 0

        if _completed_result(args.result_uri):
            print(_read_uri(args.result_uri).decode("utf-8").rstrip())
            return 0

        dependencies = _dependency_values(args.input_result)
        materialize_result_dependencies(dependencies, args.work_dir / "dependencies")
        command = tuple(args.command[1:] if args.command[:1] == ["--"] else args.command)
        if not command:
            raise ValueError("managed task requires a sakuna-etl command after --")
        spec = WorkflowSpec(
            name=args.workflow,
            description="Managed container task",
            tasks=(
                TaskSpec(
                    task_id=args.task_id,
                    command=command,
                    retries=args.retries,
                    retry_delay_seconds=30,
                    timeout_seconds=args.timeout,
                    produces_artifacts=not args.no_artifacts,
                ),
            ),
        )
        runner = WorkflowRunner(settings, state_root=args.work_dir / "state")
        state = runner.run(spec, parameters={}, run_id=args.run_id)
        _write_uri(
            args.result_uri,
            json.dumps(state.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n",
        )
        return 0 if state.status is TaskStatus.COMPLETED else 1
    except (OSError, ValueError, WorkflowError) as error:
        print(f"ERROR: {error}")
        return 2


__all__ = ["build_parser", "main", "materialize_result_dependencies"]


if __name__ == "__main__":
    raise SystemExit(main())
