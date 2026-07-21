"""Production workflow catalog composed exclusively from ``sakuna-etl`` commands."""

from __future__ import annotations

from .models import ScheduleSpec, TaskSpec, WorkflowSpec


def _source_workflow(
    source: str,
    command: tuple[str, ...],
    *,
    schedule: ScheduleSpec | None,
    required: tuple[str, ...],
) -> WorkflowSpec:
    return WorkflowSpec(
        name=f"source-{source}",
        description=f"Validate and create immutable {source.upper()} RDF artifacts.",
        tasks=(
            TaskSpec(
                task_id=source,
                command=command,
                retries=2,
                retry_delay_seconds=30,
                timeout_seconds=4 * 60 * 60,
            ),
        ),
        required_parameters=required,
        parameter_defaults={"profile": "local"},
        schedule=schedule,
    )


WORKFLOWS: dict[str, WorkflowSpec] = {
    "source-emdat": _source_workflow(
        "emdat",
        (
            "emdat", "--input", "{input}", "--out-dir", "{output_dir}",
            "--validate", "--profile", "{profile}",
        ),
        schedule=ScheduleSpec("cron(0 2 1 * ? *)", systemd_on_calendar="*-*-01 02:00:00"),
        required=("input", "output_dir"),
    ),
    "source-gda": _source_workflow(
        "gda",
        (
            "gda", "--input", "{input}", "--out-dir", "{output_dir}",
            "--validate", "--profile", "{profile}",
        ),
        schedule=None,
        required=("input", "output_dir"),
    ),
    "source-psgc": _source_workflow(
        "psgc",
        (
            "psgc", "--input", "{input}", "--out-dir", "{output_dir}",
            "--profile", "{profile}",
        ),
        schedule=ScheduleSpec("cron(0 1 1 1,4,7,10 ? *)", systemd_on_calendar="*-01,04,07,10-01 01:00:00"),
        required=("input", "output_dir"),
    ),
    "source-ndrrmc": _source_workflow(
        "ndrrmc",
        (
            "ndrrmc", "--data-dir", "{input}", "--out-dir", "{output_dir}",
            "--validate", "--profile", "{profile}",
        ),
        schedule=ScheduleSpec("cron(0 2 * * ? *)", systemd_on_calendar="*-*-* 02:00:00"),
        required=("input", "output_dir"),
    ),
    "source-dromic": _source_workflow(
        "dromic",
        (
            "dromic", "--data-dir", "{input}", "--out", "{output}",
            "--validate", "--profile", "{profile}",
        ),
        schedule=ScheduleSpec("cron(30 2 * * ? *)", systemd_on_calendar="*-*-* 02:30:00"),
        required=("input", "output"),
    ),
    "integration": WorkflowSpec(
        name="integration",
        description="Resolve validated source artifacts and atomically publish alignment RDF.",
        tasks=(
            TaskSpec(
                task_id="align",
                command=(
                    "align", "--sources", "{sources_dir}",
                    "--resolution-dir", "{resolution_dir}", "--stats",
                    "--profile", "{profile}",
                ),
                retries=1,
                retry_delay_seconds=60,
                timeout_seconds=2 * 60 * 60,
                required_manifest_parameters=("source_manifests",),
            ),
            TaskSpec(
                task_id="publish",
                command=(
                    "load-graphdb", "--input-manifest", "{manifest:align}",
                    "--replace", "--validate", "--profile", "{profile}",
                ),
                depends_on=("align",),
                retries=1,
                retry_delay_seconds=60,
                timeout_seconds=30 * 60,
                produces_artifacts=False,
            ),
        ),
        required_parameters=("sources_dir", "resolution_dir", "source_manifests"),
        parameter_defaults={"profile": "local"},
        schedule=ScheduleSpec("cron(0 4 * * ? *)", systemd_on_calendar="*-*-* 04:00:00"),
    ),
}


def get_workflow(name: str) -> WorkflowSpec:
    try:
        return WORKFLOWS[name]
    except KeyError as error:
        raise ValueError(
            f"Unknown workflow {name!r}; expected one of: {', '.join(sorted(WORKFLOWS))}"
        ) from error


__all__ = ["WORKFLOWS", "get_workflow"]
