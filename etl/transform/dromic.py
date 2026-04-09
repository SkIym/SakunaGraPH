

from typing import List

from rdflib import URIRef
from semantic_processing.location_matcher_v2 import LOCATION_MATCHER
from mappings.iris import DROMIC_EVENT_NS
from mappings.dromic import Event
import os
import json
from semantic_processing.disaster_classifier import DISASTER_CLASSIFIER
import uuid
from datetime import datetime

def _event_id(event_name: str, start_date: str | None) -> str:
    """Deterministic hex ID from event name + start date."""
    key = f"{event_name.strip().lower()}:{start_date or ''}"
    return uuid.uuid5(DROMIC_EVENT_NS, key).hex

def _add_commas(location: str) -> str:
    words = location.split(" ")
    result: list[str] = []
    commas = 0
    i = 0
    while i < len(words):
        word = words[i]
        # treat "Brgy." and the next word as one token
        if word == "Brgy." and i + 1 < len(words):
            result.append(f"{word} {words[i+1]}")
            i += 2
            continue
        if result and commas < 2:
            result[-1] += ","
            commas += 1
        result.append(word)
        i += 1
    return " ".join(result)

def load_events(folder_path: str) -> List[Event]:
    
    events: List[Event] = []

    for folder in next(os.walk(folder_path))[1]:
        meta_path = os.path.join(folder_path, folder, "metadata.json")

        if not os.path.exists(meta_path):
            continue

        with open(meta_path, "r", encoding="utf-8") as f:
            meta: dict[str, str] = json.load(f)

  
        event_name = meta.get("eventName", folder)
        remarks = meta.get("remarks") or ""
        pred, _ = DISASTER_CLASSIFIER.classify(
            [event_name + remarks]
        )[0]

        location = meta.get("location", None)

        
        # if location is explicit add, if not, tag through impact (outside)
        hasLocation = ""
        if location and location != "":
            hasLocation = "|".join(LOCATION_MATCHER.match_cell(_add_commas(location)))

        if not meta["startDate"]:
            print('Missing dates in metadata.json: ', event_name)

        events.append(Event(
            id=_event_id(event_name, meta.get("startDate")),
            eventName=event_name,
            startDate=datetime.fromisoformat(meta["startDate"]),
            endDate=datetime.fromisoformat(meta["endDate"]),
            remarks=remarks,
            hasDisasterType=pred,
            hasLocation=URIRef(str(hasLocation)) if hasLocation else None
        ))

    return events