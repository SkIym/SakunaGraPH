"""Dependency-light parsed-data contracts and production quality gates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from enum import Enum
import math
from typing import Any, Iterable, Mapping, Sequence

from sakunagraph_etl.config import DeploymentProfile, EtlSettings


class ValueKind(str, Enum):
    ANY = "any"
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    DATE = "date"


@dataclass(frozen=True, slots=True)
class FieldSchema:
    name: str
    kind: ValueKind = ValueKind.ANY
    required: bool = True
    nullable: bool = True

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "FieldSchema":
        return cls(
            name=str(value["name"]),
            kind=ValueKind(str(value.get("kind", ValueKind.ANY.value))),
            required=bool(value.get("required", True)),
            nullable=bool(value.get("nullable", True)),
        )


@dataclass(frozen=True, slots=True)
class TableSchema:
    source: str
    table: str
    fields: tuple[FieldSchema, ...]
    allow_unexpected_columns: bool = False
    schema_version: int = 1

    def __post_init__(self) -> None:
        names = [field.name for field in self.fields]
        if not self.source or not self.table:
            raise ValueError("schema source and table are required")
        if len(names) != len(set(names)):
            raise ValueError(f"schema {self.source}/{self.table} contains duplicate fields")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "TableSchema":
        return cls(
            source=str(value["source"]),
            table=str(value["table"]),
            fields=tuple(FieldSchema.from_dict(item) for item in value.get("fields", ())),
            allow_unexpected_columns=bool(value.get("allow_unexpected_columns", False)),
            schema_version=int(value.get("schema_version", 1)),
        )

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for field in value["fields"]:
            field["kind"] = field["kind"].value
        return value


@dataclass(frozen=True, slots=True)
class QualityPolicy:
    minimum_records: int = 1
    maximum_rejected_records: int = 0
    maximum_rejected_ratio: float = 0.0
    fail_on_unexpected_columns: bool = True

    def __post_init__(self) -> None:
        if self.minimum_records < 0 or self.maximum_rejected_records < 0:
            raise ValueError("record thresholds cannot be negative")
        if not 0.0 <= self.maximum_rejected_ratio <= 1.0:
            raise ValueError("maximum_rejected_ratio must be between zero and one")

    @classmethod
    def production(cls) -> "QualityPolicy":
        return cls()

    @classmethod
    def from_settings(cls, settings: EtlSettings) -> "QualityPolicy":
        return cls(
            minimum_records=settings.quality_minimum_records,
            maximum_rejected_records=settings.quality_maximum_rejected_records,
            maximum_rejected_ratio=settings.quality_maximum_rejected_ratio,
            fail_on_unexpected_columns=settings.quality_fail_on_unexpected_columns,
        )


@dataclass(frozen=True, slots=True)
class QualityIssue:
    code: str
    message: str
    count: int = 1
    fields: tuple[str, ...] = ()
    row_numbers: tuple[int, ...] = ()


@dataclass(frozen=True, slots=True)
class QualityReport:
    source: str
    table: str
    status: str
    record_count: int
    accepted_count: int
    rejected_count: int
    warning_count: int
    missing_columns: tuple[str, ...]
    unexpected_columns: tuple[str, ...]
    rejection_reasons: tuple[QualityIssue, ...]
    warnings: tuple[QualityIssue, ...]
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class QualityGateError(ValueError):
    def __init__(self, report: QualityReport):
        self.report = report
        reasons = ", ".join(issue.code for issue in report.rejection_reasons)
        super().__init__(
            f"parsed-data quality gate failed for {report.source}/{report.table}: "
            f"{reasons or 'policy threshold exceeded'}"
        )


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip() or value.strip().lower() in {"nan", "none", "null"}
    if isinstance(value, float):
        return math.isnan(value)
    if type(value).__name__ in {"NAType", "NaTType"}:
        return True
    try:
        result = value != value
        return bool(result)
    except (TypeError, ValueError, OverflowError):
        return False


def _matches_kind(value: Any, kind: ValueKind) -> bool:
    if kind is ValueKind.ANY:
        return True
    if kind is ValueKind.STRING:
        return isinstance(value, str)
    if kind is ValueKind.INTEGER:
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        if isinstance(value, float):
            return value.is_integer()
        try:
            int(str(value).replace(",", "").strip())
            return True
        except (TypeError, ValueError):
            return False
    if kind is ValueKind.NUMBER:
        if isinstance(value, bool):
            return False
        try:
            float(str(value).replace(",", "").strip())
            return True
        except (TypeError, ValueError):
            return False
    if kind is ValueKind.DATE:
        if isinstance(value, (date, datetime)):
            return True
        try:
            date.fromisoformat(str(value).strip()[:10])
            return True
        except (TypeError, ValueError):
            return False
    raise AssertionError(f"unsupported value kind: {kind}")


def _tabular_records(data: Any) -> tuple[tuple[str, ...], list[Mapping[str, Any]]]:
    columns = tuple(str(column) for column in getattr(data, "columns", ()))
    if hasattr(data, "iter_rows"):
        return columns, list(data.iter_rows(named=True))
    if hasattr(data, "to_dict"):
        try:
            records = data.to_dict(orient="records")
        except TypeError:
            records = data.to_dicts()
        return columns, list(records)
    records = list(data)
    if not all(isinstance(row, Mapping) for row in records):
        raise TypeError("quality validation requires mappings or a pandas/Polars DataFrame")
    if not columns:
        columns = tuple(dict.fromkeys(str(key) for row in records for key in row))
    return columns, records


def validate_table(
    data: Any,
    schema: TableSchema,
    *,
    policy: QualityPolicy | None = None,
) -> QualityReport:
    """Validate a dataframe or iterable of mappings without mutating its rows."""

    selected_policy = policy or QualityPolicy.production()
    columns, records = _tabular_records(data)
    expected = {field.name for field in schema.fields}
    observed = set(columns)
    missing = tuple(sorted(field.name for field in schema.fields if field.required and field.name not in observed))
    unexpected = tuple(sorted(observed - expected))
    issues: list[QualityIssue] = []
    warnings: list[QualityIssue] = []
    rejected_rows: set[int] = set()
    gate_failed = False

    if missing:
        gate_failed = True
        issues.append(
            QualityIssue(
                "MISSING_COLUMN",
                f"required columns are missing: {', '.join(missing)}",
                count=len(missing),
                fields=missing,
            )
        )
        rejected_rows.update(range(1, len(records) + 1))

    if unexpected and not schema.allow_unexpected_columns:
        unexpected_issue = QualityIssue(
            "UNEXPECTED_COLUMN",
            f"unexpected columns detected: {', '.join(unexpected)}",
            count=len(unexpected),
            fields=unexpected,
        )
        if selected_policy.fail_on_unexpected_columns:
            gate_failed = True
            issues.append(unexpected_issue)
        else:
            warnings.append(unexpected_issue)

    for field in schema.fields:
        if field.name not in observed:
            continue
        null_rows: list[int] = []
        invalid_rows: list[int] = []
        for row_number, row in enumerate(records, start=1):
            value = row.get(field.name)
            if _is_null(value):
                if not field.nullable:
                    null_rows.append(row_number)
                continue
            if not _matches_kind(value, field.kind):
                invalid_rows.append(row_number)
        if null_rows:
            rejected_rows.update(null_rows)
            issues.append(
                QualityIssue(
                    "NULL_REQUIRED_VALUE",
                    f"{field.name} contains null required values",
                    count=len(null_rows),
                    fields=(field.name,),
                    row_numbers=tuple(null_rows[:20]),
                )
            )
        if invalid_rows:
            rejected_rows.update(invalid_rows)
            issues.append(
                QualityIssue(
                    "INVALID_DATATYPE",
                    f"{field.name} contains values outside {field.kind.value}",
                    count=len(invalid_rows),
                    fields=(field.name,),
                    row_numbers=tuple(invalid_rows[:20]),
                )
            )

    record_count = len(records)
    rejected_count = len(rejected_rows)
    rejected_ratio = rejected_count / record_count if record_count else 0.0
    if record_count < selected_policy.minimum_records:
        gate_failed = True
        issues.append(
            QualityIssue(
                "MINIMUM_RECORDS",
                f"found {record_count} records; minimum is {selected_policy.minimum_records}",
            )
        )
    if rejected_count > selected_policy.maximum_rejected_records:
        gate_failed = True
        issues.append(
            QualityIssue(
                "REJECTED_COUNT_THRESHOLD",
                f"rejected {rejected_count} records; maximum is "
                f"{selected_policy.maximum_rejected_records}",
                count=rejected_count,
            )
        )
    if rejected_ratio > selected_policy.maximum_rejected_ratio:
        gate_failed = True
        issues.append(
            QualityIssue(
                "REJECTED_RATIO_THRESHOLD",
                f"rejected ratio {rejected_ratio:.6f}; maximum is "
                f"{selected_policy.maximum_rejected_ratio:.6f}",
                count=rejected_count,
            )
        )

    return QualityReport(
        source=schema.source,
        table=schema.table,
        status="FAILED" if gate_failed else "PASSED",
        record_count=record_count,
        accepted_count=record_count - rejected_count,
        rejected_count=rejected_count,
        warning_count=sum(issue.count for issue in warnings),
        missing_columns=missing,
        unexpected_columns=unexpected,
        rejection_reasons=tuple(issues),
        warnings=tuple(warnings),
        schema_version=schema.schema_version,
    )


def enforce_quality(report: QualityReport) -> QualityReport:
    if report.status != "PASSED":
        raise QualityGateError(report)
    return report


def enforce_production_quality(
    report: QualityReport,
    profile: str | DeploymentProfile,
) -> QualityReport:
    selected = profile if isinstance(profile, DeploymentProfile) else DeploymentProfile(profile)
    if selected in {DeploymentProfile.ONPREM, DeploymentProfile.CLOUD}:
        enforce_quality(report)
    return report


def merge_quality_reports(
    source: str,
    table: str,
    reports: Sequence[QualityReport],
) -> QualityReport:
    failures = tuple(
        issue for report in reports for issue in report.rejection_reasons
    )
    warnings = tuple(issue for report in reports for issue in report.warnings)
    return QualityReport(
        source=source,
        table=table,
        status="FAILED" if any(report.status == "FAILED" for report in reports) else "PASSED",
        record_count=sum(report.record_count for report in reports),
        accepted_count=sum(report.accepted_count for report in reports),
        rejected_count=sum(report.rejected_count for report in reports),
        warning_count=sum(report.warning_count for report in reports),
        missing_columns=tuple(sorted({item for report in reports for item in report.missing_columns})),
        unexpected_columns=tuple(
            sorted({item for report in reports for item in report.unexpected_columns})
        ),
        rejection_reasons=failures,
        warnings=warnings,
    )


__all__ = [
    "FieldSchema",
    "QualityGateError",
    "QualityIssue",
    "QualityPolicy",
    "QualityReport",
    "TableSchema",
    "ValueKind",
    "enforce_production_quality",
    "enforce_quality",
    "merge_quality_reports",
    "validate_table",
]
