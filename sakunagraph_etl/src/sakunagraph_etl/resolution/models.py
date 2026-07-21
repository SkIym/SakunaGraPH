"""Typed event features shared by blocking, scoring, and clustering."""

from ._resolver import (
    DisasterEvent,
    RDF_TYPE_DISASTER_EVENT,
    RDF_TYPE_INCIDENT,
    RDF_TYPE_MAJOR_EVENT,
)

__all__ = [
    "DisasterEvent",
    "RDF_TYPE_DISASTER_EVENT",
    "RDF_TYPE_INCIDENT",
    "RDF_TYPE_MAJOR_EVENT",
]
