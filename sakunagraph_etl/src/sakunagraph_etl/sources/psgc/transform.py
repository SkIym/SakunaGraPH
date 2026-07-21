"""Typed PSGC transform boundary shared by the job and fixture tests."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .parse import ParsedPsgcWorkbook


@dataclass(frozen=True, slots=True)
class PsgcTransformResult:
    workbook: ParsedPsgcWorkbook
    normalized_rows: pl.DataFrame

    @property
    def row_count(self) -> int:
        return self.normalized_rows.height


def transform_psgc(workbook: ParsedPsgcWorkbook) -> PsgcTransformResult:
    """Return the normalized rows produced at the parser boundary.

    PSGC normalization is intentionally performed while reading the workbook
    because hierarchy construction requires normalized codes and levels.
    Keeping this typed hand-off makes that established behavior explicit.
    """

    return PsgcTransformResult(workbook=workbook, normalized_rows=workbook.rows)


__all__ = ["PsgcTransformResult", "transform_psgc"]
