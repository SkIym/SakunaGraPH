# pipeline

Per-source ETL drivers and the cross-source entity-resolution stage. Each
`run_*.py` script transforms one upstream dataset into a Turtle graph under
`../data/rdf/events/`; `build_alignment.py` then ingests those graphs and
produces `owl:sameAs` alignments plus a dedup registry.

All scripts are intended to be run from the `etl/` directory so that the
relative `../data/...` paths resolve correctly.

## `run_emdat.py`
Builds `emdat.ttl` from the latest EM-DAT export found in
`../data/raw/emdat/`.

Steps:
1. Create an empty graph via `mappings.graph.create_graph()`.
2. Pick the most recently modified file in `DATA_DIR`.
3. `load_source` + `source_mapping` to mint the source IRI.
4. `transform_emdat` parses the file into typed entity buckets, which are
   handed to the per-class mappers in order: `Event`, `Assistance`,
   `Recovery`, `DamageGeneral`, `Casualties`, `AffectedPopulation`.
5. Serialize to `../data/rdf/events/{--out}` (default `emdat.ttl`).

CLI: `python run_emdat.py [--out emdat.ttl]`

## `run_ndrrmc.py`
Builds NDRRMC event graphs from the parsed sitrep tree under
`../data/parsed/ndrrmc/`. Unlike the other drivers, NDRRMC produces one
subgraph per event and writes them out in batches.

Steps:
1. `load_events(DATA_DIR)` discovers all events in the index.
2. For each batch (default 10 events), spin up a `ProcessPoolExecutor`; each
   worker calls `process_event`, which mints the event IRI, loads provenance,
   then runs the full set of loaders/mappers (incidents, aff_pop, casualties,
   relief, infra, housing, agri, pevac, rnb, power, comms, doc_calamity,
   class_suspension, work_suspension, stranded, water, seaport, airport,
   flight).
3. Subgraphs are merged into a per-batch main graph and serialized to
   `../data/rdf/events/{--out}-{index}.ttl`.
4. The script loops until `file_no` (89) batches have been written.

CLI: `python run_ndrrmc.py [--out ndrrmc] [--start 0] [--count 10]`

Notes:
- Workers re-initialize `logging.basicConfig` because
  `ProcessPoolExecutor` does not inherit the parent's root logger config.
- The hard-coded `file_no = 89` is the current event count and should be
  updated when new sitreps are added.

## `run_gda.py`
Builds `gda.ttl` from the cleaned Geographical Disaster Archive workbook at
`../data/raw/static/geog-archive-cleaned.xlsx`.

Steps:
1. Create an empty graph and use the static `SKG.GDA` IRI as the source.
2. `transform_gda` parses the workbook into typed entity buckets.
3. Run the full mapper sequence: `Event`, `Incident`, `Preparedness`,
   `Evacuation`, `Rescue`, `DeclarationOfCalamity`, `AffectedPopulation`,
   `Casualties`, `HousingDamage`, `InfrastructureDamage`, `DamageGeneral`,
   `PowerDisruption`, `CommunicationLineDisruption`, `RoadAndBridgesDamage`,
   `SeaportDisruption`, `WaterDisruption`, `Assistance`, `Relief`, `Recovery`.
4. Serialize to `../data/rdf/events/{--out}` (default `gda.ttl`).

CLI: `python run_gda.py [--out gda.ttl]`

## `build_alignment.py`
End-to-end entity-resolution pipeline that consumes every TTL under
`../data/rdf/events/` and writes alignments + a cluster registry to
`../data/rdf/resolution/`. All resolution logic lives in
`semantic_processing/event_resolver.py`; this script is the orchestrator.

Stages:
1. **Extract** — `load_all_sources(sources_dir)` parses every source TTL into
   `DisasterEvent` objects.
2. **Block** — `generate_candidate_pairs` produces candidate pairs;
   `blocking_stats` reports total/possible/candidate counts and the reduction
   ratio.
3. **Score** — `score_all_pairs` runs the date / type / label / PSGC gates
   and produces scored pairs.
4. **Align** — pairs flagged `is_match` are written to
   `alignments.ttl` as `owl:sameAs`; `build_clusters` groups them and
   `save_registry` persists the dedup registry to `dedup_registry.json`.
5. **Merge** — currently commented out; would materialize `canonical.ttl`
   from the alignments via `merge_graphs`.

Logging is mirrored to stdout and `../logs/pipeline_<timestamp>.txt`.

CLI:
```
python build_alignment.py [--sources DIR]
                          [--skip-merge]   # stop after Stage 4
                          [--stats]        # score but write nothing
                          [--incremental]  # skip pairs already in registry
                          [--verbose]      # log every scored pair
```

## Typical run order

```
python run_emdat.py
python run_gda.py
python run_ndrrmc.py
python build_alignment.py
```

After the alignment stage, `load_graphdb.py` (not covered here) loads the
resulting graphs into the triple store.
