

from typing import List

from rdflib import URIRef
from semantic_processing.location_matcher_v2 import LOCATION_MATCHER
from mappings.iris import DROMIC_EVENT_NS
from mappings.dromic import Event, Provenance
import os
import json
from semantic_processing.disaster_classifier import DISASTER_CLASSIFIER
import uuid
from datetime import datetime
import re

def _event_id(event_name: str, start_date: str | None) -> str:
    """Deterministic hex ID from event name + start date."""
    key = f"{event_name.strip().lower()}:{start_date or ''}"
    return uuid.uuid5(DROMIC_EVENT_NS, key).hex

def _extract_barangay(text: str) -> str | None:
    """
    Extracts the barangay name from strings like:
      "Brgy. Proper, Calamba, Laguna"
      "Barangay Holy Spirit, Quezon City"
      "Bgy. Commonwealth, QC"
    
    Returns the raw barangay name (e.g. "Proper"), or None if not found.
    """
    pattern = r'(?:Barangay|Brgy\.?|Bgy\.?)\s+([^,]+)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def load_event(file_path: str) -> Event:

    with open(file_path, "r", encoding="utf-8") as f:
        meta: dict[str, str] = json.load(f)

    event_name = meta.get("eventName", "") 
    remarks = meta.get("remarks", "") 
    pred, _ = DISASTER_CLASSIFIER.classify(
        [event_name + remarks]
    )[0]

    location = meta.get("location", None)

    # if location is explicit add, if not, tag through impact (outside)
    hasLocation = ""
    if location and location != "":
        hasLocation = "|".join(LOCATION_MATCHER.match_cell(location))

    # if not meta["startDate"]:
    #     print('Missing dates in metadata.json: ', event_name)

    event = Event(
        id=_event_id(event_name, meta.get("startDate")),
        eventName=event_name,
        startDate=datetime.fromisoformat(meta["startDate"]) if meta["startDate"] else None,
        endDate=datetime.fromisoformat(meta["endDate"]) if meta["endDate"] else None,
        remarks=remarks,
        hasDisasterType=pred,
        hasBarangay=_extract_barangay(location) if location else None,
        hasLocation=URIRef(str(hasLocation)) if hasLocation else None
    )

    return event

def load_provenance(file_path: str) -> Provenance:

    with open(file_path, "r", encoding="utf-8") as f:
        src: dict[str, str] = json.load(f)

    if not src["lastUpdateDate"]: print("Missing last update date on file: ", src["reportName"])
    
    return Provenance(
        lastUpdateDate=datetime.fromisoformat(src['lastUpdateDate']) if src['lastUpdateDate'] else None,
        reportLink=src.get("reportLink"),
        reportName=src.get("reportName", ""),
        obtainedDate=src.get("obtainedDate"),
    )