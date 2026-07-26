"""DROMIC acquisition manifest models and compatibility helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any, Mapping, Optional

from .identity import canonical_post_url


@dataclass
class ManifestEntry:
    filename: str
    download_url: str
    downloaded_at: str
    post_url: str
    page: int
    post_date: Optional[str] = None
    sha256: Optional[str] = None


@dataclass
class Manifest:
    last_scrape_date: Optional[str] = None
    entries: list[ManifestEntry] = field(default_factory=list)


def _entries_from_payload(payload: object) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        payload = payload.get("entries", [])
    if not isinstance(payload, list):
        return []
    return [entry for entry in payload if isinstance(entry, Mapping)]


def load_manifest(path: Path) -> Manifest:
    if not path.exists():
        return Manifest()

    with path.open("r", encoding="utf-8") as source:
        data = json.load(source)

    entries = [ManifestEntry(**dict(entry)) for entry in _entries_from_payload(data)]
    if isinstance(data, list):
        return Manifest(entries=entries)
    if not isinstance(data, Mapping):
        return Manifest()
    return Manifest(last_scrape_date=data.get("last_scrape_date"), entries=entries)


def save_manifest(path: Path, manifest: Manifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as destination:
        json.dump(
            {
                "last_scrape_date": manifest.last_scrape_date,
                "entries": [asdict(entry) for entry in manifest.entries],
            },
            destination,
            indent=2,
            ensure_ascii=False,
        )


def latest_entry_for_filename(payload: object, filename: str) -> Mapping[str, Any] | None:
    """Return the newest manifest entry for a possibly replaced filename."""

    stem = Path(filename).stem
    matches = [
        entry
        for entry in _entries_from_payload(payload)
        if Path(str(entry.get("filename", ""))).stem == stem
    ]
    return matches[-1] if matches else None


def entries_for_post_url(
    payload: object,
    post_url: str | None,
) -> list[Mapping[str, Any]]:
    """Return every acquired version belonging to one canonical DROMIC post."""

    canonical = canonical_post_url(post_url)
    if canonical is None:
        return []
    return [
        entry
        for entry in _entries_from_payload(payload)
        if canonical_post_url(str(entry.get("post_url") or "")) == canonical
    ]


def source_version_for_filename(payload: object, filename: str) -> str | None:
    """Return the acquisition version token recorded for a source filename."""

    entry = latest_entry_for_filename(payload, filename)
    if entry is None:
        return None
    for key in ("sha256", "downloaded_at", "post_date", "download_url"):
        value = entry.get(key)
        if value:
            return str(value)
    return None


__all__ = [
    "Manifest",
    "ManifestEntry",
    "entries_for_post_url",
    "latest_entry_for_filename",
    "load_manifest",
    "save_manifest",
    "source_version_for_filename",
]
