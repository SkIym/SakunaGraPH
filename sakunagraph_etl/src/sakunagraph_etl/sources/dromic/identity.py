"""Stable identities for DROMIC event series and legacy compatibility."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit
import uuid

from sakunagraph_etl.uuid_namespaces import DROMIC_EVENT_NS


DROMIC_HOST = "dromic.dswd.gov.ph"


def canonical_post_url(value: str | None) -> str | None:
    """Return one query-free canonical URL for a DROMIC event post."""

    if not value or not value.strip():
        return None

    parsed = urlsplit(value.strip())
    host = (parsed.hostname or "").casefold()
    if host == f"www.{DROMIC_HOST}":
        host = DROMIC_HOST

    if not host:
        return None

    scheme = parsed.scheme.casefold() or "https"
    if host == DROMIC_HOST:
        scheme = "https"

    port = parsed.port
    default_port = (scheme == "https" and port == 443) or (scheme == "http" and port == 80)
    netloc = host if port is None or default_port else f"{host}:{port}"

    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/":
        path = f"{path.rstrip('/')}/"

    return urlunsplit((scheme, netloc, path, "", ""))


def legacy_event_id(event_name: str, start_date: str | None) -> str:
    """Return the historical name/date-based DROMIC event identifier."""

    key = f"{event_name.strip().lower()}:{start_date or ''}"
    return uuid.uuid5(DROMIC_EVENT_NS, key).hex


def canonical_event_id(
    report_link: str | None,
    *,
    event_name: str,
    start_date: str | None,
) -> str:
    """Return a fixed event-series ID, falling back for legacy source records."""

    post_url = canonical_post_url(report_link)
    if post_url is None:
        return legacy_event_id(event_name, start_date)
    return uuid.uuid5(DROMIC_EVENT_NS, f"post:{post_url}").hex


def report_series_id(report_link: str | None, report_name: str) -> str:
    """Return the stable identity of the evolving document published at a post."""

    post_url = canonical_post_url(report_link)
    key = f"post:{post_url}" if post_url else f"report:{report_name.strip().casefold()}"
    return uuid.uuid5(DROMIC_EVENT_NS, f"report-series:{key}").hex


def report_version_id(
    report_link: str | None,
    report_name: str,
    *,
    sha256: str | None = None,
    post_date: str | None = None,
    downloaded_at: str | None = None,
    download_url: str | None = None,
) -> str:
    """Return an immutable identity for one acquired version of a DROMIC report."""

    series_id = report_series_id(report_link, report_name)
    if sha256 and sha256.strip():
        version_key = f"sha256:{sha256.strip().casefold()}"
    else:
        version_key = "|".join(
            [
                f"post-date:{(post_date or '').strip()}",
                f"downloaded-at:{(downloaded_at or '').strip()}",
                f"download-url:{(download_url or '').strip()}",
                f"report-name:{report_name.strip().casefold()}",
            ]
        )
    return uuid.uuid5(
        DROMIC_EVENT_NS,
        f"report-version:{series_id}:{version_key}",
    ).hex


__all__ = [
    "canonical_event_id",
    "canonical_post_url",
    "legacy_event_id",
    "report_series_id",
    "report_version_id",
]
