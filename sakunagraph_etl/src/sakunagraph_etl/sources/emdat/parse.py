"""Typed input boundary for EM-DAT workbooks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from sakunagraph_etl.io import LocalFileStorage, Storage, local_input_paths_from_manifest


EMDAT_SHEET = "EM-DAT Data"


@dataclass(frozen=True, slots=True)
class ParsedEmdatWorkbook:
    """Workbook rows plus the source path needed for provenance."""

    source_path: Path
    rows: pl.DataFrame

    @property
    def row_count(self) -> int:
        return self.rows.height


def parse_workbook(input_path: str | Path) -> ParsedEmdatWorkbook:
    """Read the EM-DAT data sheet as strings, before semantic normalization."""

    source_path = Path(input_path).expanduser().resolve(strict=True)
    rows = pl.read_excel(
        source=str(source_path),
        sheet_name=EMDAT_SHEET,
        infer_schema_length=0,
    )
    return ParsedEmdatWorkbook(source_path=source_path, rows=rows)


def latest_workbook(
    data_dir: str | Path,
    *,
    storage: Storage | None = None,
    input_manifest: str | Path | None = None,
) -> Path:
    """Resolve one workbook without modification-time selection."""

    if input_manifest is not None:
        candidates = tuple(
            path
            for path in local_input_paths_from_manifest(input_manifest)
            if path.suffix.lower() in {".xlsx", ".xls"}
        )
        if len(candidates) != 1:
            raise ValueError(
                f"EM-DAT manifest must select exactly one workbook; found {len(candidates)}"
            )
        return candidates[0]

    root = Path(data_dir).expanduser().resolve(strict=False)
    source_storage = storage or LocalFileStorage(root)
    files = tuple(
        path
        for path in source_storage.iter_files(pattern="*")
        if path.suffix.lower() in {".xlsx", ".xls"}
    )
    if not files:
        raise FileNotFoundError(f"No EM-DAT workbooks found in directory: {root}")
    if len(files) != 1:
        raise ValueError(
            f"Multiple EM-DAT workbooks found in {root}; pass --input or --input-manifest"
        )
    return files[0]


__all__ = ["EMDAT_SHEET", "ParsedEmdatWorkbook", "latest_workbook", "parse_workbook"]
