"""Pure-Python UUID namespaces shared by RDF and source identity code."""

from __future__ import annotations

import uuid


SKG_EVENT_NS = uuid.UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
EMDAT_EVENT_NS = uuid.UUID("e9d7c5b3-1f2a-4e6d-8c0b-7a4f3e2d1c5b")
GDA_NS = uuid.UUID("c7a2f3b1-9e4d-4a08-b5c6-1d2e3f4a5b6c")
NDRRMC_EVENT_NS = uuid.UUID("a3f2c1d4-7e8b-4f09-b5a6-2c3d4e5f6a7b")
DROMIC_EVENT_NS = uuid.UUID("f7c14e82-3b9d-4a56-8e01-d2f5a7c93b1e")


__all__ = [
    "DROMIC_EVENT_NS",
    "EMDAT_EVENT_NS",
    "GDA_NS",
    "NDRRMC_EVENT_NS",
    "SKG_EVENT_NS",
]
