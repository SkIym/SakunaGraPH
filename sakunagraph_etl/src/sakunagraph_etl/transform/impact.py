"""Shared materialization rules for impact dataclasses."""

from __future__ import annotations

from dataclasses import fields
from typing import Any, TypeVar

import polars as pl


T = TypeVar("T")


def impact_entities(df: pl.DataFrame, cls: type[T]) -> list[T]:
    """Build impacts only when a row has a non-location impact property.

    Empty location strings are normalized to ``None`` so RDF mappings cannot
    emit empty ``hasLocation`` IRIs. ``id`` is identity metadata and does not
    make an otherwise empty impact meaningful.
    """
    if "hasLocation" in df.columns:
        location = pl.col("hasLocation").cast(pl.Utf8, strict=False).str.strip_chars()
        df = df.with_columns(
            pl.when(location.str.to_lowercase().is_in(["", "none", "null"]))
            .then(None)
            .otherwise(location)
            .alias("hasLocation")
        )

    class_fields = fields(cls)
    impact_fields = [
        field.name
        for field in class_fields
        if field.name not in {"id", "hasLocation"}
    ]
    entities: list[T] = []

    for row in df.to_dicts():
        data: dict[str, Any] = {}
        for field in class_fields:
            value = row.get(field.name)
            if value is None or (
                isinstance(value, str) and value.strip().lower() in {"", "none", "null"}
            ):
                data[field.name] = None
            else:
                data[field.name] = value

        if any(data[field_name] is not None for field_name in impact_fields):
            entities.append(cls(**data))

    return entities
