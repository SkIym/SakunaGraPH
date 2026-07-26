"""Authoritative, concurrency-safe DROMIC event state and compatibility lists."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from pathlib import Path
import re
from typing import Iterable, Mapping

from sakunagraph_etl.io.storage import LocalFileStorage, NetworkFileStorage


STATE_FILENAME = "_event_manifest.json"


class EventStatus(str, Enum):
    PARSED = "PARSED"
    MAPPED = "MAPPED"
    DUPLICATE_CSV = "DUPLICATE_CSV"
    PARSE_ERROR = "PARSE_ERROR"
    MAPPING_ERROR = "MAPPING_ERROR"

    @property
    def requires_rerun(self) -> bool:
        return self in {
            EventStatus.DUPLICATE_CSV,
            EventStatus.PARSE_ERROR,
            EventStatus.MAPPING_ERROR,
        }


@dataclass(frozen=True, slots=True)
class EventStatusRecord:
    event_id: str
    status: EventStatus
    producer: str
    updated_at: str
    reason: str | None = None
    source_filename: str | None = None
    source_version: str | None = None

    @classmethod
    def create(
        cls,
        event_id: str,
        status: EventStatus,
        producer: str,
        *,
        reason: str | None = None,
        source_filename: str | None = None,
        source_version: str | None = None,
    ) -> "EventStatusRecord":
        return cls(
            event_id=event_id,
            status=status,
            producer=producer,
            updated_at=datetime.now(timezone.utc).isoformat(),
            reason=reason,
            source_filename=source_filename,
            source_version=source_version,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "EventStatusRecord":
        return cls(
            event_id=str(value["event_id"]),
            status=EventStatus(str(value["status"])),
            producer=str(value["producer"]),
            updated_at=str(value["updated_at"]),
            reason=str(value["reason"]) if value.get("reason") is not None else None,
            source_filename=(
                str(value["source_filename"])
                if value.get("source_filename") is not None
                else None
            ),
            source_version=(
                str(value["source_version"])
                if value.get("source_version") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class DromicStateManifest:
    year: str
    updated_at: str
    events: Mapping[str, Mapping[str, EventStatusRecord]] = field(default_factory=dict)
    schema_version: int = 2

    def to_dict(self) -> dict[str, object]:
        return {
            "year": self.year,
            "updated_at": self.updated_at,
            "schema_version": self.schema_version,
            "events": {
                event_id: {
                    producer: {
                        **asdict(record),
                        "status": record.status.value,
                    }
                    for producer, record in sorted(producers.items())
                }
                for event_id, producers in sorted(self.events.items())
            },
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "DromicStateManifest":
        raw_events = value.get("events", {})
        events: dict[str, dict[str, EventStatusRecord]] = {}
        if isinstance(raw_events, Mapping):
            for event_id, raw_producers in raw_events.items():
                if not isinstance(raw_producers, Mapping):
                    continue
                events[str(event_id)] = {
                    str(producer): EventStatusRecord.from_dict(record)
                    for producer, record in raw_producers.items()
                    if isinstance(record, Mapping)
                }
        return cls(
            year=str(value.get("year", "unknown")),
            updated_at=str(value.get("updated_at", "")),
            events=events,
            schema_version=int(value.get("schema_version", 1)),
        )


class DromicStateStore:
    """Merge producer-specific state under one network-safe manifest lock."""

    def __init__(self, year_dir: str | Path) -> None:
        self.year_dir = Path(year_dir).expanduser().resolve(strict=False)
        self.storage = NetworkFileStorage(self.year_dir)
        self.key = STATE_FILENAME

    def load(self) -> DromicStateManifest:
        if not self.storage.exists(self.key):
            return DromicStateManifest(
                year=self.year_dir.name,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
        return DromicStateManifest.from_dict(
            json.loads(self.storage.read_text(self.key))
        )

    @staticmethod
    def _failure_events(manifest: DromicStateManifest) -> list[str]:
        return sorted(
            event_id
            for event_id, records in manifest.events.items()
            if any(record.status.requires_rerun for record in records.values())
        )

    @staticmethod
    def _parsed_sources(manifest: DromicStateManifest) -> list[str]:
        return sorted(
            {
                record.source_filename
                for records in manifest.events.values()
                for record in records.values()
                if record.source_filename
                and record.status
                in {EventStatus.PARSED, EventStatus.MAPPED, EventStatus.DUPLICATE_CSV}
            }
        )

    @staticmethod
    def _line_payload(values: Iterable[str]) -> bytes:
        text = "\n".join(values)
        return ((text + "\n") if text else "").encode("utf-8")

    def update(self, records: Iterable[EventStatusRecord]) -> DromicStateManifest:
        incoming = tuple(records)
        if not incoming:
            return self.load()
        with self.storage.locked(self.key):
            current = self.load()
            events = {
                event_id: dict(producers)
                for event_id, producers in current.events.items()
            }
            for record in incoming:
                events.setdefault(record.event_id, {})[record.producer] = record
            updated = DromicStateManifest(
                year=self.year_dir.name,
                updated_at=datetime.now(timezone.utc).isoformat(),
                events=events,
            )
            payload = json.dumps(updated.to_dict(), indent=2, sort_keys=True).encode("utf-8") + b"\n"
            LocalFileStorage.write_bytes(self.storage, self.key, payload, atomic=True)
            LocalFileStorage.write_bytes(
                self.storage,
                "_needs_rerun.txt",
                self._line_payload(self._failure_events(updated)),
                atomic=True,
            )
            LocalFileStorage.write_bytes(
                self.storage,
                "_parsed.txt",
                self._line_payload(self._parsed_sources(updated)),
                atomic=True,
            )
            return updated

    def events_requiring_rerun(
        self,
        manifest: DromicStateManifest | None = None,
    ) -> set[str]:
        return set(self._failure_events(manifest or self.load()))

    def failed_sources(
        self,
        manifest: DromicStateManifest | None = None,
    ) -> set[str]:
        """Return source filenames whose current producer state is a failure."""

        current = manifest or self.load()
        return {
            record.source_filename
            for records in current.events.values()
            for record in records.values()
            if record.source_filename and record.status.requires_rerun
        }

    def parsed_sources(
        self,
        manifest: DromicStateManifest | None = None,
    ) -> set[str]:
        parsed = set(self._parsed_sources(manifest or self.load()))
        if self.storage.exists("_parsed.txt"):
            parsed.update(
                value.strip()
                for value in self.storage.read_text("_parsed.txt").splitlines()
                if value.strip()
            )
        for folder in self.year_dir.iterdir() if self.year_dir.is_dir() else ():
            if not folder.is_dir():
                continue
            source_path = folder / "source.json"
            if not source_path.is_file():
                continue
            try:
                source = json.loads(source_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            filename = source.get("reportName") if isinstance(source, Mapping) else None
            if not isinstance(filename, str) or not filename:
                continue
            if "pdf" in filename.casefold():
                filename = re.sub(
                    r"\.(?:docx|doc)$",
                    "",
                    filename,
                    flags=re.IGNORECASE,
                )
            else:
                filename = re.sub(
                    r"\.(?:docx|doc)$",
                    ".pdf",
                    filename,
                    flags=re.IGNORECASE,
                )
            parsed.add(filename)
        return parsed

    def source_is_current(
        self,
        event_id: str,
        *,
        producer: str,
        source_filename: str,
        source_version: str | None,
        manifest: DromicStateManifest | None = None,
    ) -> bool:
        """Return whether producer state already covers this acquired version."""

        current = manifest or self.load()
        record = current.events.get(event_id, {}).get(producer)
        if record is None or record.status not in {
            EventStatus.PARSED,
            EventStatus.MAPPED,
            EventStatus.DUPLICATE_CSV,
        }:
            return False
        if record.source_filename != source_filename:
            return False
        if not source_version or not record.source_version:
            return True
        return record.source_version == source_version


__all__ = [
    "DromicStateManifest",
    "DromicStateStore",
    "EventStatus",
    "EventStatusRecord",
    "STATE_FILENAME",
]
