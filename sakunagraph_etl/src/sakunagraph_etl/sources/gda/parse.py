"""Typed workbook boundary for the Global Disaster Alert (GDA) source."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .transform import load_with_tiered_headers


@dataclass(frozen=True, slots=True)
class ParsedGdaWorkbook:
    source_path: Path
    rows: pd.DataFrame

    @property
    def row_count(self) -> int:
        return len(self.rows)


def parse_workbook(input_path: str | Path) -> ParsedGdaWorkbook:
    source_path = Path(input_path).expanduser().resolve(strict=True)
    return ParsedGdaWorkbook(
        source_path=source_path,
        rows=load_with_tiered_headers(source_path),
    )


__all__ = ["ParsedGdaWorkbook", "parse_workbook"]
