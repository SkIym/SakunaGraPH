"""Focused ETL regression tests."""
"""Repository tests, including the preserved sibling compatibility layer."""

from pathlib import Path
import sys


LEGACY_ETL_ROOT = Path(__file__).resolve().parents[2] / "etl"
if LEGACY_ETL_ROOT.is_dir() and str(LEGACY_ETL_ROOT) not in sys.path:
    sys.path.insert(0, str(LEGACY_ETL_ROOT))
