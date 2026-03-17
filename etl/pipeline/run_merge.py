"""Post-processing pipeline: match events across NDRRMC, EM-DAT, and GDA sources.

Reads the RDF outputs from each pipeline, runs cross-source event matching,
and produces an event_links.ttl file with owl:sameAs triples.
"""

import argparse
import glob
import os

from rdflib import Graph

from semantic_processing.event_matcher import (
    EventMatcher,
    load_events_from_rdf,
    matches_to_rdf,
)


def main():
    parser = argparse.ArgumentParser(
        description="Match events across NDRRMC, EM-DAT, and GDA RDF outputs"
    )
    parser.add_argument(
        "--rdf-dir", default="../data/rdf/",
        help="Directory containing RDF output files (default: ../data/rdf/)"
    )
    parser.add_argument(
        "--output", default="../data/rdf/event_links.ttl",
        help="Output path for owl:sameAs links (default: ../data/rdf/event_links.ttl)"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.7,
        help="Minimum match score (0-1, default: 0.7)"
    )
    parser.add_argument(
        "--date-window", type=int, default=7,
        help="Maximum date difference in days for candidate pairs (default: 7)"
    )
    args = parser.parse_args()

    rdf_dir = args.rdf_dir

    # Load events from each source
    ndrrmc_events = []
    emdat_events = []
    gda_events = []

    # NDRRMC: ndrrmc-*.ttl files
    for path in sorted(glob.glob(os.path.join(rdf_dir, "ndrrmc*.ttl"))):
        if "event_links" in path:
            continue
        print(f"Loading NDRRMC events from {os.path.basename(path)}...")
        ndrrmc_events.extend(load_events_from_rdf(path, "ndrrmc"))

    # EM-DAT: emdat.ttl
    emdat_path = os.path.join(rdf_dir, "emdat.ttl")
    if os.path.exists(emdat_path):
        print(f"Loading EM-DAT events from emdat.ttl...")
        emdat_events = load_events_from_rdf(emdat_path, "emdat")

    # GDA: gda.nt or gda.ttl
    for ext in ["nt", "ttl"]:
        gda_path = os.path.join(rdf_dir, f"gda.{ext}")
        if os.path.exists(gda_path):
            print(f"Loading GDA events from gda.{ext}...")
            gda_events = load_events_from_rdf(gda_path, "gda")
            break

    print(f"\nEvents loaded: NDRRMC={len(ndrrmc_events)}, EM-DAT={len(emdat_events)}, GDA={len(gda_events)}")

    if not any([ndrrmc_events, emdat_events, gda_events]):
        print("No events found. Nothing to match.")
        return

    # Run matching
    matcher = EventMatcher(
        threshold=args.threshold,
        date_window_days=args.date_window,
    )

    matches = matcher.match_all(ndrrmc_events, emdat_events, gda_events)

    print(f"\nFound {len(matches)} cross-source matches:")
    for m in matches:
        print(f"  {m.source_a} <-> {m.source_b}  score={m.score:.2f}")
        print(f"    {m.uri_a}")
        print(f"    {m.uri_b}")

    # Serialize
    if matches:
        g = matches_to_rdf(matches)
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        g.serialize(destination=args.output, format="turtle")
        print(f"\nSaved {len(matches)} owl:sameAs links to {args.output}")
    else:
        print("\nNo matches found above threshold.")


if __name__ == "__main__":
    main()
