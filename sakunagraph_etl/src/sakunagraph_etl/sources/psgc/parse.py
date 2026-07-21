"""Typed input contract for PSGC workbooks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from .rdf import load_dataframe


@dataclass(frozen=True, slots=True)
class ParsedPsgcWorkbook:
    source_path: Path
    rows: pl.DataFrame

    @property
    def row_count(self) -> int:
        return self.rows.height


def parse_workbook(input_path: str | Path) -> ParsedPsgcWorkbook:
    source_path = Path(input_path).expanduser().resolve(strict=True)
    return ParsedPsgcWorkbook(source_path=source_path, rows=load_dataframe(source_path))


__all__ = ["ParsedPsgcWorkbook", "parse_workbook"]
