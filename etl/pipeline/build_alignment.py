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
import logging
import sys
from datetime import datetime
from pathlib import Path

SOURCES_DIR     = "../data/rdf/events"
RESOL_DIR       = "../data/rdf/resolution"
OUTPUT_DIR      = "../data/rdf/"
LOGS_DIR        = "../logs"

ALIGNMENTS_PATH = RESOL_DIR + "/alignments.ttl"
CANONICAL_PATH  = RESOL_DIR + "/canonical.ttl"
REGISTRY_PATH   = RESOL_DIR + "/dedup_registry.json"


from semantic_processing.event_resolver import (
    load_all_sources,
    generate_candidate_pairs, blocking_stats,
    score_all_pairs,
    write_alignments, save_registry, load_registry, get_known_pairs, build_clusters,
    # merge_graphs,
)


log = logging.getLogger(__name__)


def make_log_path(logs_dir: str = LOGS_DIR) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(logs_dir) / f"pipeline_{timestamp}.txt"


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def log_section(title: str) -> None:
    log.info("─" * 60)
    log.info("  %s", title)
    log.info("─" * 60)


def run(
    sources_dir: str = SOURCES_DIR,
    skip_merge:  bool = False,
    stats_only:  bool = False,
    incremental: bool = False,
    verbose:     bool = False,
) -> None:

    log_path = make_log_path()
    setup_logging(log_path)
    log.info("Logging to: %s", log_path)
    log.info("=== Entity resolution pipeline start ===")

    # Stage 1: Extract
    log_section("Stage 1 — Extract")
    log.info("→ load_all_sources(%s)", sources_dir)
    events = load_all_sources(Path(sources_dir))
    log.info("✓ load_all_sources done — total events loaded: %d", len(events))

    if len(events) < 2:
        log.warning("Not enough events to run ER. Add source TTL files to sources/")
        return

    # Stage 2: Block
    log_section("Stage 2 — Block")
    log.info("→ generate_candidate_pairs(%d events)", len(events))
    pairs = generate_candidate_pairs(events)
    log.info("✓ generate_candidate_pairs done — %d candidate pairs", len(pairs))

    log.info("→ blocking_stats")
    stats = blocking_stats(events, pairs)
    log.info("✓ blocking_stats done")
    log.info("  Events:          %d", stats["total_events"])
    log.info("  Possible pairs:  %d", stats["total_possible_pairs"])
    log.info("  Candidate pairs: %d", stats["candidate_pairs"])
    log.info("  Reduction:       %.1f%%", stats["reduction_ratio"] * 100)

    if incremental:
        log.info("→ load_registry(%s)", REGISTRY_PATH)
        registry = load_registry(Path(REGISTRY_PATH))
        log.info("✓ load_registry done")

        log.info("→ get_known_pairs")
        known = get_known_pairs(registry)
        log.info("✓ get_known_pairs done — %d known pairs", len(known))

        before = len(pairs)
        pairs = [(a, b) for a, b in pairs if frozenset([a.uri, b.uri]) not in known]
        log.info("Incremental: skipped %d known pairs", before - len(pairs))

    if not pairs:
        log.info("No new candidate pairs to evaluate.")
        return

    # Stage 3: Score
    log_section("Stage 3 — Score")
    log.info("→ score_all_pairs(%d pairs, verbose=%s)", len(pairs), verbose)
    scored_pairs = score_all_pairs(pairs, verbose=verbose)
    log.info("✓ score_all_pairs done — %d scored pairs", len(scored_pairs))

    if stats_only:
        log.info("(stats-only mode — no files written)")
        return

    matches = [(a, b, sc) for a, b, sc in scored_pairs if sc.is_match]
    if not matches:
        log.info("No matches found above threshold.")
        return

    log.info("No. of event matches: %d", len(matches))

    # Stage 4: Align
    log_section("Stage 4 — Align")
    log.info("→ write_alignments(%d matches → %s)", len(matches), ALIGNMENTS_PATH)
    write_alignments(matches, Path(ALIGNMENTS_PATH))
    log.info("✓ write_alignments done")

    log.info("→ build_clusters(%d matches)", len(matches))
    clusters = build_clusters(matches)
    log.info("✓ build_clusters done — %d clusters", len(clusters))

    log.info("→ save_registry(%s)", REGISTRY_PATH)
    save_registry(clusters, Path(REGISTRY_PATH))
    log.info("✓ save_registry done")

    if skip_merge:
        log.info("(--skip-merge: stopping after alignment)")
        return

    # # Stage 5: Merge
    # log_section("Stage 5 — Merge")
    # log.info("→ merge_graphs")
    # merge_graphs(
    #     sources_dir=Path(sources_dir),
    #     alignments_path=Path(ALIGNMENTS_PATH),
    #     output_path=Path(CANONICAL_PATH),
    # )
    # log.info("✓ merge_graphs done")

    log.info("=== Entity resolution pipeline complete ===")


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