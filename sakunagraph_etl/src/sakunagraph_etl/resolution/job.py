"""
Package-owned entity resolution pipeline for SakunaGraPH.

Stages:
  1. Extract  — parse source TTLs → DisasterEvent objects
  2. Block    — generate candidate pairs via blocking keys
  3. Score    — compute weighted similarity for each pair
  4. Align    — write owl:sameAs to alignments.ttl + registry
  5. Merge    — materialize canonical.ttl

Usage:
  sakuna-etl align                   # full run
  sakuna-etl align --skip-merge      # stop after alignment
  sakuna-etl align --stats           # print stats only, no writes
  sakuna-etl align --incremental     # skip known pairs from registry
  sakuna-etl align --verbose         # print scored pairs
"""

from __future__ import annotations
import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from sakunagraph_etl.config import PROFILE_CHOICES, SETTINGS, EtlSettings, load_settings
from sakunagraph_etl.io import Storage, local_input_paths_from_manifest, record_artifact_run

SOURCES_DIR = SETTINGS.paths.event_rdf_root
RESOL_DIR = SETTINGS.paths.rdf_root / "resolution"
LOGS_DIR = SETTINGS.paths.logs_root


from .blocking import blocking_stats, generate_candidate_pairs
from .clustering import build_clusters, expand_clusters, write_alignments
from .features import load_all_sources, load_source_paths
from .registry import get_known_pairs, load_registry, save_registry
from .scoring import score_all_pairs


log = logging.getLogger(__name__)


def make_log_path(logs_dir: str | Path = LOGS_DIR) -> Path:
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
    sources_dir: str | Path = SOURCES_DIR,
    resolution_dir: str | Path = RESOL_DIR,
    logs_dir: str | Path = LOGS_DIR,
    skip_merge:  bool = False,
    stats_only:  bool = False,
    incremental: bool = False,
    verbose:     bool = False,
    input_manifest: str | Path | None = None,
    artifact_storage: Storage | None = None,
    settings: EtlSettings = SETTINGS,
) -> None:
    sources_dir = Path(sources_dir)
    resolution_dir = Path(resolution_dir)
    alignments_path = resolution_dir / "alignments.ttl"
    registry_path = resolution_dir / "dedup_registry.json"
    log_path = make_log_path(logs_dir)
    setup_logging(log_path)
    log.info("Logging to: %s", log_path)
    log.info("=== Entity resolution pipeline start ===")

    # Stage 1: Extract
    log_section("Stage 1 — Extract")
    log.info("→ load_all_sources(%s)", sources_dir)
    if input_manifest is not None:
        source_paths = tuple(
            path
            for path in local_input_paths_from_manifest(input_manifest)
            if path.suffix.lower() == ".ttl"
        )
        if not source_paths:
            raise ValueError("Alignment input manifest contains no Turtle artifacts")
        events = load_source_paths(source_paths)
    else:
        source_paths = tuple(sorted(sources_dir.rglob("*.ttl")))
        events = load_all_sources(sources_dir)
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
        log.info("→ load_registry(%s)", registry_path)
        registry = load_registry(registry_path)
        log.info("✓ load_registry done")

        log.info("→ get_known_pairs")
        known = get_known_pairs(registry)
        log.info("✓ get_known_pairs done — %d known pairs", len(known))

        before = len(pairs)
        pairs = [
            (a, b) for a, b in pairs
            if not (a.uri in known and b.uri in known)
        ]
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

    # Stage 4: Align
    log_section("Stage 4 — Align")

    registry = load_registry(registry_path)
    log.info("→ load_registry — %d existing clusters", len(registry))

    expanded_clusters, new_matches = expand_clusters(scored_pairs, registry)
    log.info("✓ expand_clusters — %d expansions, %d new matches", 
         len(expanded_clusters), len(new_matches))

    new_clusters = build_clusters(new_matches)
    log.info("✓ build_clusters — %d new clusters", len(new_clusters))

    all_clusters = expanded_clusters + new_clusters

    if not all_clusters:
        log.info("No matches or expansions found.")
        return

    log.info("→ write_alignments(%d clusters → %s)", len(all_clusters), alignments_path)
    write_alignments(all_clusters, alignments_path)
    log.info("✓ write_alignments done")

    log.info("→ save_registry(%s)", registry_path)
    save_registry(all_clusters, registry_path)
    artifact_inputs = list(source_paths)
    record_artifact_run(
        "alignment",
        input_paths=artifact_inputs,
        output_paths=(alignments_path, registry_path),
        validation_status="NOT_RUN",
        settings=settings,
        storage=artifact_storage,
        parameters={"incremental": incremental},
        metadata={"cluster_count": len(all_clusters)},
    )
    log.info("✓ save_registry done")

    if skip_merge:
        log.info("(--skip-merge: stopping after alignment)")
        return

    # # Stage 5: Merge
    # log_section("Stage 5 — Merge")
    # log.info("→ merge_graphs")
    # merge_graphs(
    #     sources_dir=Path(sources_dir),
    #     alignments_path=alignments_path,
    #     output_path=resolution_dir / "canonical.ttl",
    # )
    # log.info("✓ merge_graphs done")

    log.info("=== Entity resolution pipeline complete ===")


def build_parser() -> argparse.ArgumentParser:
    """Build the package-owned alignment command parser."""
    parser = argparse.ArgumentParser(description="SakunaGraPH entity resolution pipeline")
    parser.add_argument("--sources", type=Path, help="Directory containing source event TTLs.")
    parser.add_argument("--resolution-dir", type=Path, help="Alignment and registry output directory.")
    parser.add_argument("--logs-dir", type=Path, help="Pipeline log directory.")
    parser.add_argument("--profile", choices=PROFILE_CHOICES, help="Deployment profile.")
    parser.add_argument("--input-manifest", type=Path, help="Explicit input run manifest.")
    parser.add_argument("--skip-merge",  action="store_true")
    parser.add_argument("--stats",       action="store_true")
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--verbose",     action="store_true")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run cross-source alignment from the unified or module CLI."""
    args = build_parser().parse_args(argv)
    settings = load_settings(args.profile)
    run(
        sources_dir=args.sources or settings.paths.event_rdf_root,
        resolution_dir=args.resolution_dir or settings.paths.rdf_root / "resolution",
        logs_dir=args.logs_dir or settings.paths.logs_root,
        skip_merge=args.skip_merge,
        stats_only=args.stats,
        incremental=args.incremental,
        verbose=args.verbose,
        input_manifest=args.input_manifest,
        settings=settings,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
