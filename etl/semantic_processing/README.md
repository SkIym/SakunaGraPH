# semantic_processing

Semantic resolution helpers used by the ETL pipeline. Each module wraps a domain
concept (location, organization, disaster type, event identity) and exposes a
singleton or class that other pipeline stages can call to map dirty source
strings into canonical IRIs.

## Modules

### `location_matcher_v2.py`
Resolves multi-tier location strings (e.g. `"Davao City, Davao del Sur, XI"`)
to PSGC IRIs by walking an RDF graph of regions, provinces, and
cities/municipalities/sub-municipalities loaded from `psgc.ttl`.

- Loads `Region`, `Province`, `Municipality`, `City`, and `SubMunicipality`
  individuals plus their `isPartOf` parents.
- Hard-coded `region_map` covers numeric, roman-numeral, acronym, and
  common-name aliases for all 17 regions (including BARMM, NIR, CAR, NCR).
- `match(locations)` parses comma-separated tiers, dispatches to
  `match_region` / `match_province` / `match_municipality`, and falls back to
  region-scoped fuzzy lookups for messy or inverted inputs (e.g. city name in
  the province slot, NCR cities with no province layer, the Maguindanao split,
  ambiguous Region IV).
- `match_cell` handles pipe-delimited multi-location cells.
- Exposes a module-level `LOCATION_MATCHER` singleton bound to
  `../data/rdf/psgc/psgc.ttl`.
- Running the file directly executes a built-in smoke-test suite.

### `location_matcher_single.py`
Lightweight alternative matcher used when locations come in as already-flat
strings rather than tiered hierarchies. Reads `psgc.ttl` via regex (no rdflib),
keeps a flat `name → IRI` index of all `Reg`/`Prov`/`City`/`Mun`/`SubMun`
entries, and matches dirty values with `rapidfuzz`.

- Exact region-alias map first, then `WRatio` + `token_sort_ratio` fuzzy
  match against the full PSGC label list.
- Cities ending in `"City"` are rewritten to `"City of X"` to match PSGC
  canonical labels (e.g. `"Cebu City"` → `"City of Cebu"`).
- Island-group keywords (`Luzon`, `Visayas`, `Mindanao`) and `Philippines`
  expand to fixed lists of constituent region IRIs.
- `canonicalize_column(df, col)` adds `{col}_name`, `{col}_iri`, `{col}_score`
  columns to a Polars DataFrame; pipe-delimited cells are tokenized,
  deduped, and joined back together.

### `org_resolver.py`
Resolves organization name strings to canonical IRIs under
`https://sakuna.ph/org/`, backed by `../constants/org_registry.json`.

- `OrgResolver.resolve(name)` does case-insensitive exact match against the
  slug + every alias, then falls back to `thefuzz` token-sort fuzzy match
  with a configurable threshold (default 79).
- `split_orgs(text)` splits concatenated strings on `, ; / \n` and the word
  `and`; `split_and_resolve(text)` returns a deduplicated list of IRIs.
- Exposes a module-level `ORG_RESOLVER` singleton.

### `org_registry.py`
Builds `orgs.ttl` from `org_registry.json`.

- Each slug becomes an `owl:NamedIndividual` of type `prov:Organization`.
- The slug is treated as the acronym (`skos:altLabel`).
- The first alias becomes the `skos:prefLabel` (proper full name); remaining
  aliases become additional `skos:altLabel`s.
- CLI: `python org_registry.py [-i registry.json] [-o orgs.ttl]`.

### `disaster_classifier.py`
Sentence-embedding classifier that maps free-text incident descriptions to
leaf disaster types in the SakunaGraPH ontology. Classification is a two-layer
pipeline: a fast keyword rule layer narrows the candidate set, then a
SentenceTransformer resolves the final label via cosine similarity.

- Loads leaf `skos:Concept`s in `skg:DisasterTypeScheme` (concepts with no
  narrower children) plus their `skos:note` text from `disaster_type_scheme.ttl`.
- Encodes each definition with a SentenceTransformer model (default
  `all-mpnet-base-v2`) at init time; incoming texts are scored via cosine
  similarity against this embedding matrix.
- `classify(texts)` returns `(class_label, score)` per input. Score is `1.0`
  for hard rule wins, cosine similarity otherwise.
- Exposes a module-level `DISASTER_CLASSIFIER` singleton.

**Routing logic (`_route`)** — for each input text:

1. Run all classification rules and collect every label whose triggers fire
   (`_rule_candidates`). Rules do not short-circuit; all matching labels are
   gathered.
2. **No candidates** — transformer runs over the full label space.
3. **Single unambiguous candidate** — returned immediately as a hard rule win
   (score `1.0`); transformer is skipped.
4. **Ambiguous candidates** — if all fired labels share an ambiguity group
   (e.g. `LandslideWet` vs `LandslideDry`), the transformer resolves among
   the full union of that group's labels, not just the ones that fired.
5. **Mixed or multi-group candidates** — transformer resolves over the union
   of all fired labels and their sibling groups.

#### `classification_rules.py`
Defines the rule table and ambiguity groups consumed by `DisasterClassifier`.

**`CLASSIFICATION_RULES`** is an ordered list of `ClassificationRule` entries,
each of the form `([trigger_tokens], label)` or
`([trigger_tokens], label, [context_tokens])`. Matching is case-insensitive
substring: any trigger token must appear in the text, and if context tokens are
present, at least one must also appear (AND requirement). Rules span all IRDR
hazard categories: hydrological, mass movement, wave action, transport,
technological, geophysical, meteorological, climatological, biological, and
extraterrestrial.

**`AMBIGUOUS_GROUPS`** is a list of `frozenset[str]` groupings for labels that
must never be hard-won against each other — e.g. wet/dry mass-movement pairs
(`LandslideWet`/`LandslideDry`) and industrial/miscellaneous splits
(`FireIndustrial`/`FireMiscellaneous`). When all fired candidates belong to the
same group, the transformer resolves among the entire group so plausible
siblings that did not fire are still considered.

Key helpers:
- `labels_are_ambiguous(candidates)` — returns `True` if all candidates share
  at least one ambiguity group.
- `ambiguous_candidate_set(candidates)` — returns the union of all groups the
  candidates belong to, used to build the restricted label space passed to the
  transformer.
- `_WET_CONTEXT` — shared context token list for wet mass-movement rules
  (rain, flood, typhoon, etc.).

### `event_resolver.py`
End-to-end entity resolution for `DisasterEvent`s across sources (NDRRMC,
DROMIC, EM-DAT, GDA). Combines config, models, extractor, blocker, scorer,
aligner, and cluster builder in a single module, organized into six internal
sections.

**Resolution gates** — all applicable gates must pass (no gate may return
`False`) for two events to be considered a match. A gate returning `None`
means data is missing or the gate is inactive; this is a non-blocking skip.

1. **Date gate** — start dates must be within `DATE_HARD_GATE_DAYS` (5) for
   `MajorEvent`/`DisasterEvent` pairs, or `DATE_HARD_GATE_DAYS_INCIDENT` (2)
   when either event is an `Incident`.
2. **Type gate** — disaster concepts must match exactly, share a wet/dry base
   name (e.g. `LandslideWet` vs `LandslideDry`), share the same parent
   concept, or one must be the direct parent of the other. Returns `None` when
   either concept is unresolvable.
3. **Label/location gate** — when both events carry a label, passes if fuzzy
   similarity ≥ `LABEL_FUZZY_THRESHOLD` (0.70) or at least one capitalised
   proper-noun token (≥ 4 chars) is shared. When labels are absent, the gate
   is skipped (`None`).
4. **PSGC gate** — activates when either event is an `Incident`. Extracts
   10-digit PSGC codes from `location_uris` and checks for a shared prefix at
   region (2-digit), province (4-digit), or exact (10-digit) level. Returns
   `None` if either side has no resolvable PSGC codes (non-blocking skip);
   returns `False` (hard reject) if codes exist on both sides but no prefix
   matches at any level.

**Internal sections:**

- **Config** — gate thresholds (`DATE_HARD_GATE_DAYS`, `LABEL_FUZZY_THRESHOLD`,
  `PSGC_MATCH_LEVELS`, etc.), `SOURCE_PRIORITY` list, `DISASTER_TYPE_ALIASES`
  for normalising free-text type literals, and `DISASTER_TYPE_HIERARCHY` — a
  flat `label → parent` dict covering all IRDR hazard classes used by the type
  gate.
- **Models** — `DisasterEvent` dataclass carrying URI, source, label,
  disaster type (literal + URI), dates, location literal, `rdf_type`
  (`Incident` / `DisasterEvent` / `MajorEvent`), `location_uris` (for Gate 4),
  and `blocking_keys`.  `ScoreBreakdown` dataclass holding per-gate verdicts
  and the final `is_match` flag.
- **Extractor** — `extract_events_from_graph` walks an rdflib `Graph` for
  `skg:DisasterEvent`, `skg:MajorEvent`, and `skg:Incident` typed nodes,
  resolving labels, disaster types, dates, and location URIs. NDRRMC
  `Incident`-typed nodes are skip-listed at extraction time (report-level
  artefacts). `load_all_sources` iterates every `.ttl` under a directory and
  returns a flat event list.
- **Blocker** — `generate_candidate_pairs` builds an inverted index keyed by
  `{disaster_type_base}|{year}` buckets (wet/dry suffixes are stripped to the
  same bucket), then yields unique cross-source pairs. An early date-tolerance
  filter inside the blocker mirrors Gate 1 to avoid surfacing pairs that would
  be immediately rejected. `blocking_stats` reports total/possible/candidate
  counts and the reduction ratio.
- **Scorer** — `score_pair` runs all four gates and returns a `ScoreBreakdown`.
  `score_all_pairs` iterates the candidate list and optionally prints matched
  pairs to stdout when `verbose=True`.
- **Aligner** — `build_clusters` uses union-find to group matched URIs;
  `pick_canonical` selects the authoritative URI from a cluster using
  `SOURCE_PRIORITY`. `write_alignments` serialises `prov:alternateOf` triples
  (both pairwise and via a shared canonical URI) plus a `prov:Activity`
  run-record to `alignments.ttl`. `save_registry` / `load_registry` persist
  the dedup registry as JSON; `get_known_pairs` returns the flat set of all
  clustered member URIs for incremental runs. `expand_clusters` handles
  incremental updates by merging new matches into existing registry clusters
  rather than reprocessing from scratch.

Public surface used by `pipeline/build_alignment.py`:
`load_all_sources`, `generate_candidate_pairs`, `blocking_stats`,
`score_all_pairs`, `write_alignments`, `build_clusters`, `save_registry`,
`load_registry`, `get_known_pairs`.

## Conventions

- All matchers expose a module-level singleton (`LOCATION_MATCHER`,
  `ORG_RESOLVER`, `DISASTER_CLASSIFIER`) so pipeline stages can import and
  reuse them without re-loading the underlying graph or model.
- Paths are written relative to `etl/`, the directory pipeline scripts are
  expected to be run from.
- Fuzzy thresholds are tuned per concept and live as module constants at the
  top of each file.
