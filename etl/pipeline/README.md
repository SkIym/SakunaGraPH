# pipeline

Per-source ETL drivers and the cross-source entity-resolution stage. Each
`run_*.py` script transforms one upstream dataset into a Turtle graph under
`../data/rdf/events/`; `build_alignment.py` then ingests those graphs and
produces `prov:alternateOf` alignments plus a dedup registry.

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
5. Optionally run SHACL validation when `--validate` is set. EM-DAT validates
   the full output graph by default; add `--batch` to validate by event-id
   batches instead. Add `--no-context` to validate without
   PSGC/disaster-type/org/provenance context graphs.
6. Serialize to `../data/rdf/events/{--out}` (default `emdat.ttl`).

CLI: `python -m pipeline.run_emdat [--out emdat.ttl] [--validate] [--batch] [--batch-size 100] [--no-context]`

## `run_ndrrmc.py`
Builds NDRRMC event graphs from the parsed sitrep tree under
`../data/parsed/ndrrmc/`. Unlike the other drivers, NDRRMC produces one
subgraph per event and writes them out in batches.

Steps:
1. Discover event folders under `DATA_DIR`, applying `--start` and optional
   `--limit`.
2. For each batch (default 10 event folders), transform that batch with
   `load_events(DATA_DIR, batch_folders)`.
3. Map the transformed events into a temporary batch graph. Each event mints
   the event IRI, loads provenance, then runs the full set of loaders/mappers
   (incidents, aff_pop, casualties, relief, infra, housing, agri, pevac, rnb,
   power, comms, doc_calamity, class_suspension, work_suspension, stranded,
   water, seaport, airport, flight).
4. Optionally run SHACL validation when `--validate` is set. Batches are
   serialized only after validation passes. Add `--no-context` to validate
   without PSGC/disaster-type/org/provenance context graphs.
5. Serialize each valid batch graph to
   `../data/rdf/events/ndrrmc/{--out}-{index}.ttl`.

CLI: `python -m pipeline.run_ndrrmc [--out ndrrmc] [--start 0] [--batch-size 10] [--validate] [--no-context]`

## `run_dromic.py`

Builds DROMIC event graphs per year from the parsed reports tree under
`../data/parsed/dromic/[year]/`. This driver produces one subgraph per year
subfolder, or iterates over all year subfolders when `--all` is passed.

Steps:
1. For a given year directory, discover event subfolders, applying `--start`
   and optional `--limit`, while skipping folders listed in `_needs_rerun.txt`
   for that year.
2. For each batch (default 100 event folders), transform and map those events
   into a temporary batch graph. Per event, `load_event` reads `metadata.json`
   and `event_mapping` mints the event IRI; `load_provenance` + `prov_mapping`
   attach the provenance chain from `source.json`.
3. `load_aff_pop` is called for affected-population and pre-emptive evacuation
   data; non-empty results are passed to `aff_pop_mapping` and `pevac_mapping`
   respectively. `load_housing` populates housing damage via `housing_mapping`
   if present.
4. `load_assistance` + `assistance_mapping` handle assistance data; failures
   are caught, logged, and the offending folder name is appended to
   `_needs_rerun.txt` for later inspection rather than aborting the run.
5. Optionally run SHACL validation when `--validate` is set. Batches are
   merged into the final year graph only after validation passes. Add
   `--no-context` to validate without PSGC/disaster-type/org/provenance
   context graphs.
6. Serialize the year graph to
   `../data/rdf/events/dromic/{--out}-{year}.ttl`.

**`_needs_rerun.txt` mechanic** — each year directory may contain this file.
Folders listed there are silently skipped during `run()`, and any folder whose
`load_assistance` call raises an exception is automatically appended to it,
making failed events easy to retry in isolation.

CLI: `python -m pipeline.run_dromic [--year 2026] [--batch-size 100] [--validate] [--no-context]`

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
4. Optionally run SHACL validation when `--validate` is set. In that mode,
   the transform still runs once, then GDA entities are grouped by event id,
   mapped into temporary batch graphs, validated, and merged into the final
   output graph only after the batch conforms. Add `--no-context` to validate
   those batches without PSGC/disaster-type/org/provenance context graphs.
5. Serialize to `../data/rdf/events/{--out}` (default `gda.ttl`).

CLI: `python run_gda.py [--out gda.ttl] [--validate] [--batch-size 100] [--no-context]`

## `validate.py`
Reusable SHACL validation helper for in-pipeline checks and standalone RDF
validation. Create one `ShaclValidator.from_paths()` per pipeline run to load
`ontology/shapes/shapes.ttl`, `ontology/sakunagraph.ttl`, and existing
PSGC/disaster-type/org/provenance RDF context once, then reuse it for batch calls. The
convenience `validate_graph(graph, focus_nodes=None, label=None)` still
validates an in-memory `rdflib.Graph` against the same defaults. Pass
`include_context_graphs=False` or CLI `--no-context` to validate without
merging PSGC/disaster-type/org/provenance context.

CLI: `python -m validate.validate [--no-context] [path/to/file.ttl ...]`

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
   `alignments.ttl` as `prov:alternateOf`; `build_clusters` groups them and
   `save_registry` persists the dedup registry to `dedup_registry.json`.


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
python -m pipeline.run_emdat
python run_gda.py
python -m pipeline.run_ndrrmc
python build_alignment.py
```

## `load_graphdb.py`

Loads Turtle files through GraphDB's RDF4J statements API into named context
graphs. The default selection is every top-level Turtle file in `ontology/`
(`sakunagraph.ttl` and `disaster_type_scheme.ttl`) plus every Turtle file under
`data/rdf/`; it does not use GraphDB's default graph. Files in the ontology's
`imports/`, `shapes/`, and `validation/` subdirectories are not loaded.

The graph IRI follows the RDF folder structure. For example,
`data/rdf/events/dromic/dromic-2025.ttl` is loaded into
`https://sakuna.ph/events/dromic`; all DROMIC yearly files therefore share
that source graph. A file directly in `data/rdf/events/` has its filename stem
as its source graph, so `data/rdf/events/emdat.ttl` maps to
`https://sakuna.ph/events/emdat`.

Run it from `etl/`:

```
# All data RDF plus the top-level ontology Turtle files
python -m pipeline.load_graphdb

# Select logical subgraphs or nested source subtrees (may be repeated)
python -m pipeline.load_graphdb --scope ontology --scope events
python -m pipeline.load_graphdb --scope events/ndrrmc --replace
python -m pipeline.load_graphdb --scope psgc

# Select individual files; paths may be relative to data/rdf or the repository
python -m pipeline.load_graphdb --file events/dromic/dromic-2025.ttl
python -m pipeline.load_graphdb --file ../ontology/sakunagraph.ttl

# Preview the file-to-context mapping without contacting GraphDB
python -m pipeline.load_graphdb --scope events --dry-run

# Make selected graphs match the selected files, or explicitly clear all data
python -m pipeline.load_graphdb --scope events --replace
python -m pipeline.load_graphdb --clear-repository
```

`--replace` bundles all selected files for each named graph and sends one atomic
Graph Store Protocol `PUT`. When loading one DROMIC year, it replaces the shared
DROMIC graph, so use it only when the selection contains every file that should
remain in that source graph.
Selecting a complete source subtree such as `--scope events/ndrrmc` includes
all Turtle files for that source and can safely replace only its named graph.
Replacement requests default to a one-hour timeout because GraphDB may perform
an expensive inferred-closure update before responding; override this with
`--timeout` when necessary.
`--clear-repository` (with legacy alias `--clear`) deletes every graph in the
repository. Use `--username`/`--password`, or `GRAPHDB_USERNAME` and
`GRAPHDB_PASSWORD`, when GraphDB requires HTTP basic authentication.
