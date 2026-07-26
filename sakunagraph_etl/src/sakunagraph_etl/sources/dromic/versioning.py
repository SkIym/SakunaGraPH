"""Pure helpers for selecting the current DROMIC report in each post series."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Mapping

from .identity import canonical_post_url


def _timestamp(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0

    normalized = (
        text[:-1]
        if text.endswith("Z") and ("+" in text[10:] or "-" in text[10:])
        else text.replace("Z", "+00:00")
    )
    for candidate in (text, normalized):
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()

    for pattern in ("%d %B %Y", "%B %d, %Y", "%d %b %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, pattern).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            continue
    return 0.0


def source_recency_key(source: Mapping[str, object]) -> tuple[float, float, float, str]:
    """Order parsed reports by DROMIC post update, acquisition, then report date."""

    return (
        _timestamp(source.get("postDate")),
        _timestamp(source.get("obtainedDate")),
        _timestamp(source.get("lastUpdateDate")),
        str(source.get("reportName") or "").casefold(),
    )


def select_latest_report_folders(
    root: str | Path,
    folders: list[str],
) -> tuple[list[str], int]:
    """Keep one fact-bearing parsed folder for each canonical DROMIC post."""

    base = Path(root)
    grouped: dict[str, list[tuple[str, Mapping[str, object]]]] = {}
    for folder in folders:
        source_path = base / folder / "source.json"
        source: Mapping[str, object] = {}
        try:
            loaded = json.loads(source_path.read_text(encoding="utf-8"))
            if isinstance(loaded, Mapping):
                source = loaded
        except (OSError, ValueError):
            pass

        post_url = canonical_post_url(str(source.get("reportLink") or ""))
        # Missing legacy report links cannot safely be assumed to describe the
        # same event, so retain those folders independently.
        series_key = f"post:{post_url}" if post_url else f"folder:{folder}"
        grouped.setdefault(series_key, []).append((folder, source))

    selected = [
        max(candidates, key=lambda item: (*source_recency_key(item[1]), item[0]))[0]
        for candidates in grouped.values()
    ]
    selected.sort()
    return selected, len(folders) - len(selected)


__all__ = ["select_latest_report_folders", "source_recency_key"]
