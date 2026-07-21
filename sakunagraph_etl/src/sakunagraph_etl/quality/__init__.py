"""Parsed-data and source-specific quality services."""

from .parsed import check_dromic_year, discover_dromic_years, duplicate_dromic_csvs
from .schemas import (
    FieldSchema,
    QualityGateError,
    QualityIssue,
    QualityPolicy,
    QualityReport,
    TableSchema,
    ValueKind,
    enforce_production_quality,
    enforce_quality,
    merge_quality_reports,
    validate_table,
)
from .shacl import ShaclValidationError, ShaclValidationResult, ShaclValidator

__all__ = [
    "FieldSchema",
    "QualityGateError",
    "QualityIssue",
    "QualityPolicy",
    "QualityReport",
    "ShaclValidationError",
    "ShaclValidationResult",
    "ShaclValidator",
    "TableSchema",
    "ValueKind",
    "check_dromic_year",
    "discover_dromic_years",
    "duplicate_dromic_csvs",
    "enforce_production_quality",
    "enforce_quality",
    "merge_quality_reports",
    "validate_table",
]
