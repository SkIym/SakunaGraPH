"""
pipeline.py — End-to-end entity resolution pipeline for SakunaGraPH.

Stages:
  1. Extract  — parse source TTLs → DisasterEvent objects
  2. Block    — generate candidate pairs via blocking keys
  3. Score    — compute weighted similarity for each pair
  4. Align    — write owl:sameAs to alignments.ttl + registry
  5. Merge    — materialize canonical.ttl

Usage:
  python pipeline.py                   # full run
  python pipeline.py --skip-merge      # stop after alignment
  python pipeline.py --stats           # print stats only, no writes
  python pipeline.py --incremental     # skip known pairs from registry
  python pipeline.py --verbose         # print scored pairs
"""

from __future__ import annotations
import argparse
import time
from pathlib import Path

SOURCES_DIR  = "../data/rdf/events"
RESOL_DIR = "../data/rdf/resolution"
OUTPUT_DIR   = "../data/rdf/"

ALIGNMENTS_PATH = RESOL_DIR + "alignments.ttl"
CANONICAL_PATH  = RESOL_DIR + "canonical.ttl"
REGISTRY_PATH   = RESOL_DIR + "dedup_registry.json"


from semantic_processing.event_resolver import (
    MATCH_THRESHOLD,
    load_all_sources,
    generate_candidate_pairs, blocking_stats,
    score_all_pairs,
    write_alignments, save_registry, load_registry, get_known_pairs, build_clusters,
    merge_graphs,
)


def print_section(title: str) -> None:
    print(f"\n{'─'*60}\n  {title}\n{'─'*60}")


def print_match_summary(scored_pairs, threshold: float) -> None:
    matches   = [x for x in scored_pairs if x[2].is_match]
    near_miss = [x for x in scored_pairs if threshold - 0.10 <= x[2].total < threshold]

    print(f"\n  Matches (>={threshold}):     {len(matches)}")
    print(f"  Near-misses ({threshold-0.10:.2f}-{threshold:.2f}): {len(near_miss)}")

    if matches:
        print(f"\n  Top matches:")
        for a, b, sc in matches[:10]:
            print(f"    [{sc.total:.3f}] {a.source}:{a.label!r} <-> {b.source}:{b.label!r}")

    if near_miss:
        print(f"\n  Near-misses (review manually):")
        for a, b, sc in near_miss[:5]:
            print(
                f"    [{sc.total:.3f}] {a.source}:{a.label!r} <-> {b.source}:{b.label!r}"
                f"  label={sc.label:.2f} type={sc.disaster_type:.2f}"
                f" date={sc.date:.2f} loc={sc.location:.2f}"
            )


def run(
    sources_dir: Path = SOURCES_DIR,
    skip_merge:  bool = False,
    stats_only:  bool = False,
    incremental: bool = False,
    verbose:     bool = False,
) -> None:

    t0 = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Stage 1: Extract
    print_section("Stage 1 — Extract")
    events = load_all_sources(sources_dir)
    print(f"  Total events loaded: {len(events)}")

    if len(events) < 2:
        print("  Not enough events to run ER. Add source TTL files to sources/")
        return

    # # Stage 2: Block
    # print_section("Stage 2 — Block")
    # pairs = generate_candidate_pairs(events)
    # stats = blocking_stats(events, pairs)
    # print(f"  Events:          {stats['total_events']}")
    # print(f"  Possible pairs:  {stats['total_possible_pairs']}")
    # print(f"  Candidate pairs: {stats['candidate_pairs']}")
    # print(f"  Reduction:       {stats['reduction_ratio']:.1%}")

    # if incremental:
    #     registry = load_registry(REGISTRY_PATH)
    #     known    = get_known_pairs(registry)
    #     before   = len(pairs)
    #     pairs    = [(a, b) for a, b in pairs if frozenset([a.uri, b.uri]) not in known]
    #     print(f"  Incremental: skipped {before - len(pairs)} known pairs")

    # if not pairs:
    #     print("  No new candidate pairs to evaluate.")
    #     return

    # # Stage 3: Score
    # print_section("Stage 3 — Score")
    # scored_pairs = score_all_pairs(pairs, verbose=verbose)
    # print_match_summary(scored_pairs, MATCH_THRESHOLD)

    # if stats_only:
    #     print("\n  (stats-only mode — no files written)")
    #     return

    # matches = [(a, b, sc) for a, b, sc in scored_pairs if sc.is_match]
    # if not matches:
    #     print("\n  No matches found above threshold.")
    #     return

    # # Stage 4: Align
    # print_section("Stage 4 — Align")
    # write_alignments(matches, ALIGNMENTS_PATH)
    # clusters = build_clusters(matches)
    # save_registry(clusters, REGISTRY_PATH)

    # if skip_merge:
    #     print("\n  (--skip-merge: stopping after alignment)")
    #     print(f"\nDone in {time.time() - t0:.1f}s")
    #     return

    # # Stage 5: Merge
    # print_section("Stage 5 — Merge")
    # merge_graphs(
    #     sources_dir=sources_dir,
    #     alignments_path=ALIGNMENTS_PATH,
    #     output_path=CANONICAL_PATH,
    # )

    # elapsed = time.time() - t0
    # print(f"\nPipeline complete in {elapsed:.1f}s")
    # print(f"  alignments.ttl -> {ALIGNMENTS_PATH}")
    # print(f"  canonical.ttl  -> {CANONICAL_PATH}")
    # print(f"  registry.json  -> {REGISTRY_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SakunaGraPH entity resolution pipeline")
    parser.add_argument("--sources",     type=Path, default=SOURCES_DIR)
    parser.add_argument("--skip-merge",  action="store_true")
    parser.add_argument("--stats",       action="store_true")
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--verbose",     action="store_true")

    args = parser.parse_args()
    run(
        sources_dir=args.sources,
        skip_merge=args.skip_merge,
        stats_only=args.stats,
        incremental=args.incremental,
        verbose=args.verbose,
    )