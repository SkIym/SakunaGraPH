"""Versioned parsed-input contracts for SakunaGraPH sources."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .schemas import FieldSchema, QualityPolicy, QualityReport, TableSchema, ValueKind, validate_table


EMDAT_SCHEMA = TableSchema(
    source="emdat",
    table="workbook",
    fields=(
        FieldSchema("DisNo.", ValueKind.STRING, nullable=False),
        FieldSchema("Event Name", ValueKind.STRING, nullable=True),
        FieldSchema("Disaster Subtype", ValueKind.STRING, nullable=False),
        FieldSchema("Associated Types", ValueKind.STRING, nullable=True),
        FieldSchema("Location", ValueKind.STRING, nullable=True),
        FieldSchema("Start Year", ValueKind.INTEGER, nullable=False),
        FieldSchema("Start Month", ValueKind.INTEGER, nullable=True),
        FieldSchema("Start Day", ValueKind.INTEGER, nullable=True),
        FieldSchema("End Year", ValueKind.INTEGER, nullable=True),
        FieldSchema("End Month", ValueKind.INTEGER, nullable=True),
        FieldSchema("End Day", ValueKind.INTEGER, nullable=True),
        FieldSchema("Entry Date", ValueKind.DATE, nullable=True),
        FieldSchema("Last Update", ValueKind.DATE, nullable=True),
    ),
    # EM-DAT exports include optional measure columns selected by the operator.
    # They are still listed in QualityReport.unexpected_columns for observability.
    allow_unexpected_columns=True,
)

GDA_SCHEMA = TableSchema(
    source="gda",
    table="workbook",
    fields=(
        FieldSchema("M or I", ValueKind.STRING, nullable=False),
        FieldSchema("Main Event Disaster Type", ValueKind.STRING, nullable=False),
        FieldSchema("Disaster Name", ValueKind.STRING, nullable=True),
        FieldSchema("Date/Period", ValueKind.ANY, nullable=False),
        FieldSchema("Main Area/s Affected / Location", ValueKind.STRING, nullable=False),
    ),
    allow_unexpected_columns=True,
)

PSGC_SCHEMA = TableSchema(
    source="psgc",
    table="workbook",
    fields=(
        FieldSchema("psgc_code", ValueKind.STRING, nullable=False),
        FieldSchema("name", ValueKind.STRING, nullable=False),
        FieldSchema("geo_level", ValueKind.STRING, nullable=False),
        FieldSchema("status", ValueKind.STRING, required=False, nullable=True),
        FieldSchema("population_2024", ValueKind.INTEGER, required=False, nullable=True),
    ),
    allow_unexpected_columns=True,
)

NDRRMC_EVENT_SCHEMA = TableSchema(
    source="ndrrmc",
    table="event_metadata",
    fields=(
        FieldSchema("eventName", ValueKind.STRING, nullable=False),
        FieldSchema("startDate", ValueKind.DATE, nullable=False),
        FieldSchema("id", ValueKind.STRING, nullable=False),
        FieldSchema("endDate", ValueKind.DATE, required=False, nullable=True),
    ),
    allow_unexpected_columns=True,
)

DROMIC_EVENT_SCHEMA = TableSchema(
    source="dromic",
    table="event_metadata",
    fields=(
        FieldSchema("eventName", ValueKind.STRING, nullable=False),
        FieldSchema("eventType", ValueKind.STRING, nullable=True),
        FieldSchema("startDate", ValueKind.DATE, nullable=False),
        FieldSchema("endDate", ValueKind.DATE, required=False, nullable=True),
        FieldSchema("reportDate", ValueKind.DATE, required=False, nullable=True),
    ),
    allow_unexpected_columns=True,
)

SOURCE_SCHEMAS = {
    "emdat": EMDAT_SCHEMA,
    "gda": GDA_SCHEMA,
    "psgc": PSGC_SCHEMA,
    "ndrrmc": NDRRMC_EVENT_SCHEMA,
    "dromic": DROMIC_EVENT_SCHEMA,
}


def event_metadata_rows(
    root: str | Path,
    *,
    folders: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Load one metadata row per selected event, retaining malformed rows for rejection."""

    directory = Path(root).expanduser().resolve(strict=True)
    selected = (
        [directory / folder for folder in folders]
        if folders is not None
        else sorted(path for path in directory.iterdir() if path.is_dir())
    )
    rows: list[dict[str, Any]] = []
    for event_directory in selected:
        metadata_path = event_directory / "metadata.json"
        row: dict[str, Any] = {"__event_folder": event_directory.name}
        try:
            value = json.loads(metadata_path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise TypeError("metadata root must be an object")
            row.update(value)
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError) as error:
            row["__metadata_error"] = f"{type(error).__name__}: {error}"
        rows.append(row)
    return rows


def validate_source_input(
    source: str,
    input_path: str | Path,
    *,
    policy: QualityPolicy | None = None,
    folders: list[str] | tuple[str, ...] | None = None,
) -> QualityReport:
    """Parse and validate one source input using its package-owned boundary."""

    selected = source.strip().lower()
    if selected == "emdat":
        from sakunagraph_etl.sources.emdat.parse import parse_workbook

        data = parse_workbook(input_path).rows
    elif selected == "gda":
        from sakunagraph_etl.sources.gda.parse import parse_workbook

        data = parse_workbook(input_path).rows
    elif selected == "psgc":
        from sakunagraph_etl.sources.psgc.parse import parse_workbook

        data = parse_workbook(input_path).rows
    elif selected in {"ndrrmc", "dromic"}:
        data = event_metadata_rows(input_path, folders=folders)
    else:
        raise ValueError(
            f"unknown quality contract {source!r}; expected one of: "
            f"{', '.join(sorted(SOURCE_SCHEMAS))}"
        )
    return validate_table(data, SOURCE_SCHEMAS[selected], policy=policy)


__all__ = [
    "DROMIC_EVENT_SCHEMA",
    "EMDAT_SCHEMA",
    "GDA_SCHEMA",
    "NDRRMC_EVENT_SCHEMA",
    "PSGC_SCHEMA",
    "SOURCE_SCHEMAS",
    "event_metadata_rows",
    "validate_source_input",
]
