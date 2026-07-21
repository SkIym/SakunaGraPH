"""Structured logs, Prometheus text metrics, alerts, and lineage events."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import requests

from sakunagraph_etl.io.storage import LocalFileStorage, NetworkFileStorage


log = logging.getLogger(__name__)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in (
            "workflow",
            "workflow_run_id",
            "task_id",
            "attempt",
            "run_id",
            "input_uris",
        ):
            value = getattr(record, key, None) or os.getenv(f"SAKUNA_{key.upper()}")
            if value is not None:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def configure_structured_logging(level: str | None = None) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel((level or os.getenv("SAKUNA_LOG_LEVEL", "INFO")).upper())


def _append_json_line(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    storage = NetworkFileStorage(path.parent)
    with storage.locked(path.name):
        previous = path.read_bytes() if path.is_file() else b""
        line = json.dumps(value, sort_keys=True).encode("utf-8") + b"\n"
        LocalFileStorage.write_bytes(storage, path.name, previous + line, atomic=True)


class MetricsStore:
    """Small Prometheus textfile collector suitable for batch workers."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.state_path = self.root / "sakunagraph-metrics.json"
        self.text_path = self.root / "sakunagraph.prom"
        self.storage = NetworkFileStorage(self.root)

    @staticmethod
    def _label_key(labels: Mapping[str, str]) -> str:
        return ",".join(f"{key}={labels[key]}" for key in sorted(labels))

    @staticmethod
    def _render_labels(label_key: str) -> str:
        if not label_key:
            return ""
        pairs = []
        for item in label_key.split(","):
            key, value = item.split("=", 1)
            escaped = value.replace("\\", "\\\\").replace('"', '\\"')
            pairs.append(f'{key}="{escaped}"')
        return "{" + ",".join(pairs) + "}"

    def record_task(
        self,
        *,
        workflow: str,
        task: str,
        status: str,
        duration_seconds: float,
        attempts: int,
    ) -> None:
        labels = self._label_key({"workflow": workflow, "task": task, "status": status})
        duration_labels = self._label_key({"workflow": workflow, "task": task})
        with self.storage.locked(self.state_path.name):
            state = (
                json.loads(self.state_path.read_text(encoding="utf-8"))
                if self.state_path.is_file()
                else {"counters": {}, "gauges": {}}
            )
            counter_key = f"sakunagraph_task_runs_total|{labels}"
            state["counters"][counter_key] = state["counters"].get(counter_key, 0) + 1
            state["gauges"][
                f"sakunagraph_task_duration_seconds|{duration_labels}"
            ] = duration_seconds
            state["gauges"][
                f"sakunagraph_task_attempts|{duration_labels}"
            ] = attempts
            state_bytes = json.dumps(state, indent=2, sort_keys=True).encode("utf-8") + b"\n"
            LocalFileStorage.write_bytes(
                self.storage,
                self.state_path.name,
                state_bytes,
                atomic=True,
            )
            lines = [
                "# HELP sakunagraph_task_runs_total Completed ETL task executions.",
                "# TYPE sakunagraph_task_runs_total counter",
            ]
            for key, value in sorted(state["counters"].items()):
                metric, metric_labels = key.split("|", 1)
                lines.append(f"{metric}{self._render_labels(metric_labels)} {value}")
            lines.extend(
                [
                    "# HELP sakunagraph_task_duration_seconds Last ETL task duration.",
                    "# TYPE sakunagraph_task_duration_seconds gauge",
                    "# HELP sakunagraph_task_attempts Attempts used by the last ETL task run.",
                    "# TYPE sakunagraph_task_attempts gauge",
                ]
            )
            for key, value in sorted(state["gauges"].items()):
                metric, metric_labels = key.split("|", 1)
                lines.append(f"{metric}{self._render_labels(metric_labels)} {value}")
            LocalFileStorage.write_bytes(
                self.storage,
                self.text_path.name,
                ("\n".join(lines) + "\n").encode("utf-8"),
                atomic=True,
            )


class AlertSink:
    def __init__(self, path: str | Path, webhook_url: str | None = None) -> None:
        self.path = Path(path).expanduser().resolve(strict=False)
        self.webhook_url = webhook_url or os.getenv("SAKUNA_ALERT_WEBHOOK")

    def send(self, payload: Mapping[str, Any]) -> None:
        value = {"timestamp": utc_now(), **dict(payload)}
        _append_json_line(self.path, value)
        if self.webhook_url:
            try:
                response = requests.post(self.webhook_url, json=value, timeout=10)
                response.raise_for_status()
            except requests.RequestException as error:
                # The durable local alert remains the system of record. A broken
                # notification channel must not hide the original ETL failure.
                log.error("alert webhook delivery failed: %s", error)


class LineageSink:
    """Emit OpenLineage-compatible run events without requiring an SDK."""

    PRODUCER = "https://sakuna.ph/etl/0.9.0"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve(strict=False)

    def emit(
        self,
        *,
        event_type: str,
        workflow: str,
        workflow_run_id: str,
        task_id: str,
        input_uris: Iterable[str] = (),
        output_uris: Iterable[str] = (),
    ) -> None:
        def dataset(uri: str) -> dict[str, str]:
            namespace, _, name = uri.rpartition("/")
            return {"namespace": namespace or "file", "name": name or uri}

        _append_json_line(
            self.path,
            {
                "eventType": event_type,
                "eventTime": utc_now(),
                "producer": self.PRODUCER,
                "run": {"runId": workflow_run_id},
                "job": {"namespace": f"sakunagraph/{workflow}", "name": task_id},
                "inputs": [dataset(uri) for uri in input_uris],
                "outputs": [dataset(uri) for uri in output_uris],
            },
        )


def safe_identifier(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")


__all__ = [
    "AlertSink",
    "JsonLogFormatter",
    "LineageSink",
    "MetricsStore",
    "configure_structured_logging",
    "safe_identifier",
    "utc_now",
]
