"""Serializable workflow contracts whose task boundary is the CLI."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Mapping

from sakunagraph_etl import __version__


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True, slots=True)
class ScheduleSpec:
    aws_expression: str
    timezone: str = "Asia/Manila"
    systemd_on_calendar: str | None = None


@dataclass(frozen=True, slots=True)
class TaskSpec:
    task_id: str
    command: tuple[str, ...]
    depends_on: tuple[str, ...] = ()
    retries: int = 2
    retry_delay_seconds: float = 5.0
    timeout_seconds: int = 3600
    produces_artifacts: bool = True
    required_manifest_parameters: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.task_id or not self.command:
            raise ValueError("task_id and command are required")
        if self.retries < 0 or self.retry_delay_seconds < 0:
            raise ValueError("task retries and retry delay cannot be negative")
        if self.timeout_seconds <= 0:
            raise ValueError("task timeout must be positive")


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    name: str
    description: str
    tasks: tuple[TaskSpec, ...]
    required_parameters: tuple[str, ...] = ()
    parameter_defaults: Mapping[str, str] = field(default_factory=dict)
    schedule: ScheduleSpec | None = None
    max_active_runs: int = 1

    def __post_init__(self) -> None:
        if self.max_active_runs != 1:
            raise ValueError("the filesystem runner currently enforces max_active_runs=1")
        task_ids = [task.task_id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError(f"duplicate task ID in workflow {self.name}")
        known: set[str] = set()
        for task in self.tasks:
            missing = set(task.depends_on) - known
            if missing:
                raise ValueError(
                    f"task {task.task_id} has unordered or unknown dependencies: {sorted(missing)}"
                )
            known.add(task.task_id)


@dataclass(frozen=True, slots=True)
class ArtifactResult:
    run_id: str
    pipeline: str
    status: str
    manifest_key: str
    manifest_uri: str
    manifest_path: str | None = None
    validation_statuses: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ArtifactResult":
        return cls(
            run_id=str(value["run_id"]),
            pipeline=str(value["pipeline"]),
            status=str(value["status"]),
            manifest_key=str(value["manifest_key"]),
            manifest_uri=str(value["manifest_uri"]),
            manifest_path=(
                str(value["manifest_path"])
                if value.get("manifest_path") is not None
                else None
            ),
            validation_statuses=tuple(
                str(status) for status in value.get("validation_statuses", ())
            ),
        )


@dataclass(frozen=True, slots=True)
class TaskResult:
    task_id: str
    status: TaskStatus
    attempts: int
    command: tuple[str, ...]
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    exit_code: int | None = None
    artifacts: tuple[ArtifactResult, ...] = ()
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TaskResult":
        return cls(
            task_id=str(value["task_id"]),
            status=TaskStatus(str(value["status"])),
            attempts=int(value.get("attempts", 0)),
            command=tuple(str(item) for item in value.get("command", ())),
            started_at=str(value.get("started_at", "")),
            finished_at=(
                str(value["finished_at"])
                if value.get("finished_at") is not None
                else None
            ),
            duration_seconds=(
                float(value["duration_seconds"])
                if value.get("duration_seconds") is not None
                else None
            ),
            exit_code=(int(value["exit_code"]) if value.get("exit_code") is not None else None),
            artifacts=tuple(
                ArtifactResult.from_dict(item) for item in value.get("artifacts", ())
            ),
            error=str(value["error"]) if value.get("error") is not None else None,
        )


@dataclass(frozen=True, slots=True)
class WorkflowRunState:
    workflow: str
    run_id: str
    scheduled_for: str
    status: TaskStatus
    created_at: str
    updated_at: str
    parameters: Mapping[str, str]
    tasks: Mapping[str, TaskResult] = field(default_factory=dict)
    code_version: str = __version__
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "status": self.status.value,
            "tasks": {key: result.to_dict() for key, result in self.tasks.items()},
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "WorkflowRunState":
        return cls(
            workflow=str(value["workflow"]),
            run_id=str(value["run_id"]),
            scheduled_for=str(value["scheduled_for"]),
            status=TaskStatus(str(value["status"])),
            created_at=str(value["created_at"]),
            updated_at=str(value["updated_at"]),
            parameters={str(k): str(v) for k, v in value.get("parameters", {}).items()},
            tasks={
                str(key): TaskResult.from_dict(result)
                for key, result in value.get("tasks", {}).items()
            },
            code_version=str(value.get("code_version", __version__)),
            schema_version=int(value.get("schema_version", 1)),
        )


__all__ = [
    "ArtifactResult",
    "ScheduleSpec",
    "TaskResult",
    "TaskSpec",
    "TaskStatus",
    "WorkflowRunState",
    "WorkflowSpec",
]
