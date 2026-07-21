"""Resumable local/on-premise workflow runner over CLI subprocesses."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlparse

from sakunagraph_etl.config import EtlSettings
from sakunagraph_etl.io import (
    LocalFileStorage,
    NetworkFileStorage,
    RunManifest,
    storage_for_profile,
    verify_run_manifest,
)

from .models import (
    ArtifactResult,
    TaskResult,
    TaskSpec,
    TaskStatus,
    WorkflowRunState,
    WorkflowSpec,
)
from .observability import AlertSink, LineageSink, MetricsStore, safe_identifier, utc_now


log = logging.getLogger(__name__)
CommandExecutor = Callable[[Sequence[str], float, Mapping[str, str]], subprocess.CompletedProcess[str]]


class WorkflowError(RuntimeError):
    pass


class WorkflowBusyError(WorkflowError):
    pass


class ValidationGateError(WorkflowError):
    pass


def default_command_executor(
    command: Sequence[str],
    timeout: float,
    environment: Mapping[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=dict(environment),
    )


def _parse_artifact_results(path: Path) -> tuple[ArtifactResult, ...]:
    if not path.is_file():
        return ()
    payload = json.loads(path.read_text(encoding="utf-8"))
    return tuple(ArtifactResult.from_dict(item) for item in payload.get("artifacts", ()))


def _manifest_values(value: str) -> tuple[str, ...]:
    supplied = value.strip()
    if supplied.startswith("["):
        parsed = json.loads(supplied)
        if not isinstance(parsed, list):
            raise ValueError("manifest parameter JSON must be a list")
        return tuple(str(item) for item in parsed)
    return tuple(item.strip() for item in supplied.split(",") if item.strip())


class WorkflowRunner:
    def __init__(
        self,
        settings: EtlSettings,
        *,
        state_root: str | Path | None = None,
        command_executor: CommandExecutor = default_command_executor,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.state_root = Path(
            state_root or settings.paths.logs_root / "workflows"
        ).expanduser().resolve(strict=False)
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.storage = NetworkFileStorage(
            self.state_root,
            lock_timeout=0.25,
            stale_lock_seconds=48 * 60 * 60,
        )
        self.command_executor = command_executor
        self.sleeper = sleeper
        self.metrics = MetricsStore(settings.paths.logs_root / "metrics")
        self.alerts = AlertSink(settings.paths.logs_root / "alerts" / "workflow-alerts.jsonl")
        self.lineage = LineageSink(settings.paths.logs_root / "lineage" / "openlineage.jsonl")

    @staticmethod
    def _run_id(spec: WorkflowSpec, scheduled_for: str, parameters: Mapping[str, str]) -> str:
        identity = json.dumps(
            {"workflow": spec.name, "scheduled_for": scheduled_for, "parameters": parameters},
            sort_keys=True,
            separators=(",", ":"),
        )
        suffix = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
        instant = re.sub(r"[^0-9]", "", scheduled_for)[:14]
        return f"{safe_identifier(spec.name)}-{instant}-{suffix}"

    def _state_key(self, workflow: str, run_id: str) -> str:
        return f"{safe_identifier(workflow)}/{safe_identifier(run_id)}.json"

    def _save_state(self, state: WorkflowRunState) -> None:
        value = json.dumps(state.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n"
        self.storage.write_bytes(self._state_key(state.workflow, state.run_id), value, atomic=True)

    def _load_state(self, workflow: str, run_id: str) -> WorkflowRunState | None:
        key = self._state_key(workflow, run_id)
        if not self.storage.exists(key):
            return None
        return WorkflowRunState.from_dict(json.loads(self.storage.read_text(key)))

    def _storage_manifest(self, supplied: str) -> tuple[RunManifest, Any]:
        path = Path(supplied).expanduser()
        if path.is_file():
            manifest = RunManifest.from_dict(json.loads(path.read_text(encoding="utf-8")))
            parts = path.resolve().parts
            root = path.parent
            for marker in ("runs", "quarantine"):
                if marker in parts:
                    root = Path(*parts[: parts.index(marker)])
                    break
            return manifest, LocalFileStorage(root)

        storage = storage_for_profile(self.settings)
        key = supplied
        if supplied.startswith("s3://"):
            parsed = urlparse(supplied)
            if getattr(storage, "bucket", None) != parsed.netloc:
                raise ValidationGateError(f"manifest bucket is not configured: {supplied}")
            key = parsed.path.lstrip("/")
            prefix = str(getattr(storage, "prefix", "")).strip("/")
            if prefix and key.startswith(prefix + "/"):
                key = key[len(prefix) + 1 :]
        manifest = RunManifest.from_dict(json.loads(storage.read_text(key)))
        return manifest, storage

    def _validate_manifest(self, supplied: str) -> RunManifest:
        manifest, storage = self._storage_manifest(supplied)
        verify_run_manifest(manifest, storage)
        if manifest.status != "COMPLETED":
            raise ValidationGateError(
                f"manifest {supplied} has non-publishable status {manifest.status}"
            )
        outputs = [artifact for artifact in manifest.artifacts if artifact.role == "output"]
        if not outputs:
            raise ValidationGateError(f"manifest {supplied} has no publishable outputs")
        failed = [artifact.path for artifact in outputs if artifact.validation_status == "FAILED"]
        if failed:
            raise ValidationGateError(f"manifest {supplied} contains failed validation: {failed}")
        return manifest

    def _verify_checkpoint(self, result: TaskResult) -> bool:
        if result.status is not TaskStatus.COMPLETED:
            return False
        try:
            for artifact in result.artifacts:
                self._validate_manifest(artifact.manifest_path or artifact.manifest_uri)
        except (OSError, ValueError, WorkflowError):
            return False
        return True

    def _materialize_manifest_for_command(
        self,
        task_id: str,
        artifact: ArtifactResult,
    ) -> str:
        if artifact.manifest_path and Path(artifact.manifest_path).is_file():
            return artifact.manifest_path
        manifest, storage = self._storage_manifest(artifact.manifest_uri)
        directory = self.state_root / "materialized-manifests" / safe_identifier(task_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{safe_identifier(artifact.run_id)}.json"
        LocalFileStorage(directory).write_bytes(
            path.name,
            json.dumps(manifest.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n",
            atomic=True,
        )
        del storage
        return str(path)

    def _resolve_command(
        self,
        task: TaskSpec,
        parameters: Mapping[str, str],
        completed: Mapping[str, TaskResult],
    ) -> tuple[str, ...]:
        values: list[str] = []
        parameter_pattern = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
        for token in task.command:
            manifest_match = re.fullmatch(r"\{manifest:([A-Za-z0-9_.-]+)\}", token)
            if manifest_match:
                dependency = completed.get(manifest_match.group(1))
                if dependency is None or not dependency.artifacts:
                    raise WorkflowError(f"task {task.task_id} has no dependency manifest")
                values.append(
                    self._materialize_manifest_for_command(task.task_id, dependency.artifacts[0])
                )
                continue

            def substitute(match: re.Match[str]) -> str:
                key = match.group(1)
                if key not in parameters:
                    raise WorkflowError(f"missing workflow parameter: {key}")
                return parameters[key]

            values.append(parameter_pattern.sub(substitute, token))
        return tuple(values)

    def _validate_task_inputs(self, task: TaskSpec, parameters: Mapping[str, str]) -> tuple[str, ...]:
        uris: list[str] = []
        for parameter in task.required_manifest_parameters:
            if parameter not in parameters:
                raise ValidationGateError(f"missing manifest parameter: {parameter}")
            for supplied in _manifest_values(parameters[parameter]):
                manifest = self._validate_manifest(supplied)
                uris.extend(
                    artifact.storage_uri or artifact.path
                    for artifact in manifest.artifacts
                    if artifact.role == "output"
                )
        return tuple(uris)

    @staticmethod
    def _artifact_gate(artifacts: Iterable[ArtifactResult]) -> None:
        for artifact in artifacts:
            if artifact.status != "COMPLETED" or "FAILED" in artifact.validation_statuses:
                raise ValidationGateError(
                    f"artifact run {artifact.run_id} is not publishable: "
                    f"{artifact.status} {artifact.validation_statuses}"
                )

    def _execute_task(
        self,
        spec: WorkflowSpec,
        task: TaskSpec,
        state: WorkflowRunState,
        command: tuple[str, ...],
        input_uris: tuple[str, ...],
    ) -> TaskResult:
        started_at = utc_now()
        started_clock = time.monotonic()
        last_error: str | None = None
        last_exit: int | None = None
        artifacts: tuple[ArtifactResult, ...] = ()
        attempts_used = 0
        validation_failure = False

        for attempt in range(1, task.retries + 2):
            attempts_used = attempt
            result_path = (
                self.state_root / "task-results" / safe_identifier(state.run_id)
                / (
                    f"{safe_identifier(task.task_id)}-{attempt}-"
                    f"{time.time_ns()}.json"
                )
            )
            environment = dict(os.environ)
            environment.update(
                {
                    "SAKUNA_TASK_RESULT_FILE": str(result_path),
                    "SAKUNA_WORKFLOW": spec.name,
                    "SAKUNA_WORKFLOW_RUN_ID": state.run_id,
                    "SAKUNA_TASK_ID": task.task_id,
                    "SAKUNA_ATTEMPT": str(attempt),
                    "SAKUNA_LOG_FORMAT": "json",
                }
            )
            executable = (sys.executable, "-m", "sakunagraph_etl", *command)
            log.info(
                "starting workflow task",
                extra={
                    "workflow": spec.name,
                    "workflow_run_id": state.run_id,
                    "task_id": task.task_id,
                    "attempt": attempt,
                },
            )
            try:
                completed = self.command_executor(executable, task.timeout_seconds, environment)
                last_exit = completed.returncode
                if completed.stdout:
                    log.info("task stdout: %s", completed.stdout.rstrip())
                if completed.stderr:
                    log.warning("task stderr: %s", completed.stderr.rstrip())
                artifacts = _parse_artifact_results(result_path)
                self._artifact_gate(artifacts)
                if completed.returncode != 0:
                    raise WorkflowError(f"command exited with status {completed.returncode}")
                if task.produces_artifacts and not artifacts:
                    raise WorkflowError("task completed without an artifact result envelope")
                last_error = None
                break
            except ValidationGateError as error:
                last_error = str(error)
                validation_failure = True
                break
            except subprocess.TimeoutExpired:
                last_error = f"task exceeded timeout of {task.timeout_seconds} seconds"
            except (OSError, ValueError, WorkflowError) as error:
                last_error = str(error)
            if attempt <= task.retries:
                self.sleeper(task.retry_delay_seconds)

        duration = time.monotonic() - started_clock
        status = TaskStatus.FAILED if last_error else TaskStatus.COMPLETED
        result = TaskResult(
            task_id=task.task_id,
            status=status,
            attempts=attempts_used,
            command=command,
            started_at=started_at,
            finished_at=utc_now(),
            duration_seconds=duration,
            exit_code=last_exit,
            artifacts=artifacts,
            error=last_error,
        )
        self.metrics.record_task(
            workflow=spec.name,
            task=task.task_id,
            status=status.value,
            duration_seconds=duration,
            attempts=attempts_used,
        )
        output_uris = tuple(artifact.manifest_uri for artifact in artifacts)
        self.lineage.emit(
            event_type="FAIL" if status is TaskStatus.FAILED else "COMPLETE",
            workflow=spec.name,
            workflow_run_id=state.run_id,
            task_id=task.task_id,
            input_uris=input_uris,
            output_uris=output_uris,
        )
        if status is TaskStatus.FAILED:
            log.error(
                "workflow task failed: %s",
                last_error,
                extra={
                    "workflow": spec.name,
                    "workflow_run_id": state.run_id,
                    "task_id": task.task_id,
                    "input_uris": input_uris,
                },
            )
            self.alerts.send(
                {
                    "severity": "critical" if validation_failure else "warning",
                    "workflow": spec.name,
                    "workflow_run_id": state.run_id,
                    "task_id": task.task_id,
                    "attempts": attempts_used,
                    "error": last_error,
                }
            )
        return result

    def run(
        self,
        spec: WorkflowSpec,
        *,
        parameters: Mapping[str, str],
        scheduled_for: str | None = None,
        run_id: str | None = None,
        resume: bool = True,
    ) -> WorkflowRunState:
        values = {**spec.parameter_defaults, **{str(k): str(v) for k, v in parameters.items()}}
        missing = [key for key in spec.required_parameters if not values.get(key)]
        if missing:
            raise WorkflowError(f"missing required parameters: {', '.join(missing)}")
        scheduled = scheduled_for or utc_now()
        selected_run_id = run_id or self._run_id(spec, scheduled, values)
        lock_key = f"workflow-{safe_identifier(spec.name)}"
        try:
            lock = self.storage.locked(lock_key)
            lock.__enter__()
        except TimeoutError as error:
            raise WorkflowBusyError(
                f"workflow {spec.name} already has an active run"
            ) from error
        try:
            existing = self._load_state(spec.name, selected_run_id)
            if existing is not None and not resume:
                raise WorkflowError(f"workflow state already exists: {selected_run_id}")
            now = utc_now()
            state = existing or WorkflowRunState(
                workflow=spec.name,
                run_id=selected_run_id,
                scheduled_for=scheduled,
                status=TaskStatus.RUNNING,
                created_at=now,
                updated_at=now,
                parameters=values,
            )
            tasks = dict(state.tasks)
            state = replace(state, status=TaskStatus.RUNNING, updated_at=now, tasks=tasks)
            self._save_state(state)

            for task in spec.tasks:
                previous = tasks.get(task.task_id)
                if previous is not None and self._verify_checkpoint(previous):
                    log.info("resuming after completed immutable task %s", task.task_id)
                    continue
                if any(
                    tasks.get(dependency) is None
                    or tasks[dependency].status is not TaskStatus.COMPLETED
                    for dependency in task.depends_on
                ):
                    raise WorkflowError(f"dependencies are incomplete for task {task.task_id}")
                try:
                    input_uris = self._validate_task_inputs(task, values)
                    command = self._resolve_command(task, values, tasks)
                    task_result = self._execute_task(
                        spec, task, state, command, input_uris
                    )
                except (OSError, ValueError, WorkflowError) as error:
                    log.error(
                        "workflow task input or command gate failed: %s",
                        error,
                        extra={
                            "workflow": spec.name,
                            "workflow_run_id": state.run_id,
                            "task_id": task.task_id,
                        },
                    )
                    task_result = TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        attempts=0,
                        command=task.command,
                        started_at=utc_now(),
                        finished_at=utc_now(),
                        error=str(error),
                    )
                    self.metrics.record_task(
                        workflow=spec.name,
                        task=task.task_id,
                        status=TaskStatus.FAILED.value,
                        duration_seconds=0.0,
                        attempts=0,
                    )
                    self.lineage.emit(
                        event_type="FAIL",
                        workflow=spec.name,
                        workflow_run_id=state.run_id,
                        task_id=task.task_id,
                    )
                    self.alerts.send(
                        {
                            "severity": (
                                "critical"
                                if isinstance(error, ValidationGateError)
                                else "warning"
                            ),
                            "workflow": spec.name,
                            "workflow_run_id": state.run_id,
                            "task_id": task.task_id,
                            "attempts": 0,
                            "error": str(error),
                        }
                    )
                tasks[task.task_id] = task_result
                state = replace(state, tasks=dict(tasks), updated_at=utc_now())
                self._save_state(state)
                if task_result.status is TaskStatus.FAILED:
                    state = replace(state, status=TaskStatus.FAILED, updated_at=utc_now())
                    self._save_state(state)
                    return state

            state = replace(state, status=TaskStatus.COMPLETED, updated_at=utc_now())
            self._save_state(state)
            return state
        finally:
            lock.__exit__(None, None, None)


def backfill_dates(start: date, end: date) -> tuple[date, ...]:
    if end < start:
        raise ValueError("backfill end date cannot be before start date")
    values: list[date] = []
    current = start
    while current <= end:
        values.append(current)
        current += timedelta(days=1)
    return tuple(values)


__all__ = [
    "ValidationGateError",
    "WorkflowBusyError",
    "WorkflowError",
    "WorkflowRunner",
    "backfill_dates",
    "default_command_executor",
]
