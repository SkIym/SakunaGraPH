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
  narrower children) plus their `skos:definition` from `sakunagraph.ttl`.
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
DROMIC, EM-DAT, GDA). Combines extractor, blocker, scorer, aligner, and
cluster builder in a single module.

Resolution gates (all must pass for two events to be merged):
1. **Date gate** — start dates within `DATE_HARD_GATE_DAYS` (5).
2. **Type gate** — same disaster concept or shared parent concept.
3. **Label/location gate** — fuzzy label similarity ≥ threshold or shared
   proper-noun token; falls back to location-token overlap when labels are
   absent.
4. **PSGC gate** — when at least one side is an `Incident`, requires a shared
   PSGC prefix at region (2-digit), province (4-digit), or exact (10-digit)
   level.

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
