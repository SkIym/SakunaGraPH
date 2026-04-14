# etl/transform

Transform modules for each disaster data source. Each module reads raw/parsed source files, cleans and normalizes their contents, resolves locations, and returns typed dataclass entities for downstream RDF minting.

All modules share utilities from `helpers.py` and resolve locations via `semantic_processing.location_matcher_v2`.

---

## Modules

### `dromic.py`
Transforms DROMIC (Disaster Response Operations Monitoring and Information Center) CSV reports for a single disaster event.

**Input:** A folder containing:
- `metadata.json` — event name, dates, location, remarks
- `source.json` — report provenance metadata
- CSV files matching `affected*number*.csv`, `total*displaced*.csv`, or `displaced*.csv`
- CSV files matching `house*.csv`

**Key functions:**
| Function | Description |
|---|---|
| `load_event(file_path)` | Parses `metadata.json` into an `Event` entity; classifies disaster type and matches location |
| `load_provenance(file_path)` | Parses `source.json` into a `Provenance` entity |
| `load_aff_pop(folder_path)` | Loads and merges affected/displaced population CSVs; resolves inside/outside displacement into totals when no aggregate file exists |
| `load_housing(folder_path)` | Loads the housing damage CSV |

---

### `emdat.py`
Transforms the EM-DAT global disaster database XLSX file.

**Input:** EM-DAT `.xlsx` file (`EM-DAT Data` sheet).

**Key functions:**
| Function | Description |
|---|---|
| `transform_emdat(input_path)` | Full pipeline: renames columns, cleans/normalizes data, matches locations, and returns a `dict[type, list]` of entities |
| `clean_columns(df)` | Maps disaster subtypes to ontology IRIs, normalizes dates, fills null locations, casts numeric columns |
| `clean_loc(df, col)` | Strips noise words, normalizes abbreviations and geographic names in the location column |
| `normalize_date(df, start_cols, end_cols)` | Builds `startDate`/`endDate` from separate year/month/day columns; extends partial dates to month/year boundaries |
| `load_source(path)` | Constructs a `Source` entity from file metadata |

**Entities produced:** `Event`, `Assistance`, `Recovery`, `DamageGeneral`, `Casualties`, `AffectedPopulation`, `Source`

---

### `gda.py`
Transforms the GDA (Geographic Disaster Archive) XLSX file with multi-tier headers.

**Input:** GDA `.xlsx` file with 3–4 row merged headers.

**Key functions:**
| Function | Description |
|---|---|
| `transform_gda(path)` | Full pipeline: loads tiered headers, renames columns, parses dates, matches locations, slices into sub-tables, and returns a `dict[type, list]` of entities |
| `load_with_tiered_headers(path)` | Flattens multi-level header rows into underscore-joined column names |
| `clean_date_range(value)` | Normalizes free-text date values (ranges, partial dates, year-only) into `(startDate, endDate)` pairs |
| `export_slices(df, specs, out_dir)` | Writes sub-table CSVs defined by `EXPORT_SPECS` (e.g. affected population, casualties, housing, infrastructure) |
| `to_type_iri(dtype)` | Converts raw GDA type strings to ontology IRI slugs |

**Entities produced:** `Event`, `Incident`, `AffectedPopulation`, `Casualties`, `HousingDamage`, `InfrastructureDamage`, `DamageGeneral`, `Assistance`, `Relief`, `Recovery`, `Preparedness`, `Evacuation`, `Rescue`, `DeclarationOfCalamity`, `PowerDisruption`, `CommunicationLineDisruption`, `RoadAndBridgesDamage`, `SeaportDisruption`, `WaterDisruption`

---

### `ndrrmc.py`
Transforms NDRRMC (National Disaster Risk Reduction and Management Council) per-event report folders into typed entities.

**Input:** A folder containing subfolders per event, each with:
- `metadata.json`, `source.json`
- Optional CSVs: `affected_population.csv`, `damage_to_infrastructure.csv`, `assistance_provided*.csv`, `casualties*.csv`, `related_incidents.csv`, `damaged_houses.csv`, `agriculture*.csv`, `pre-emptive_evacuation.csv`, `road*.csv`, `power*.csv`, `communication*.csv`, `calamity*.csv`, `class*.csv`, `work*.csv`, `stranded*.csv`, `water*.csv`, `seaport*.csv`, `airport*.csv`, `flight*.csv`

**Key functions:**
| Function | Description |
|---|---|
| `load_events(folder_path)` | Iterates event subfolders, classifies disaster type from metadata, returns `list[Event]` |
| `load_provenance(event_folder_path)` | Returns a `Provenance` entity from `source.json` |
| `load_aff_pop` | Affected population with barangay-level location matching |
| `load_infra` | Infrastructure damage amounts (converted to millions PHP) |
| `load_relief` | Assistance provided; merges multiple CSV files and fills missing columns |
| `load_casualties` | Individual casualty records with type normalization (dead/injured) |
| `load_incidents` | Related sub-incidents with disaster type classification and datetime normalization |
| `load_housing` | Housing damage with deduplication and city-summary removal |
| `load_agri` | Agriculture damage with crop area and production loss |
| `load_pevac` | Pre-emptive evacuation; merges evac CSV with cleaned affected population data |
| `load_rnb` / `load_power` / `load_comms` / `load_water` / `load_seaport` / `load_airport` / `load_flight` | Lifeline disruption tables with datetime normalization |
| `load_docalamity` | Declaration of calamity records |
| `load_class_suspension` / `load_work_suspension` | Class and work suspension records |

---

### `psgc_datafile.py`
Converts the PSGC (Philippine Standard Geographic Code) Excel publication into an RDF/Turtle location graph.

**Input:** PSGC `.xlsx` file (sheet: `PSGC`).

**Key functions:**
| Function | Description |
|---|---|
| `load_dataframe(xlsx_path)` | Loads and normalizes the PSGC Excel sheet; zero-pads codes and filters to valid geographic levels |
| `build_abox(g, df, include_barangay)` | Populates an RDFLib graph with one individual per PSGC row, `isPartOf` hierarchy triples, labels, and attributes |
| `add_island_group(g)` | Adds top-level nodes for Philippines, Luzon, Visayas, and Mindanao |
| `init_graph()` | Initializes an RDFLib graph with standard namespace bindings |
| `parent_code(code, level, all_codes)` | Resolves the parent PSGC code for a given administrative level |

**Geographic levels:** Region, Province, City, Municipality, Sub-Municipality, Barangay

**CLI usage:**
```
python psgc_datafile.py --input <xlsx> --output psgc.ttl [--barangay]
```

---

### `helpers.py`
Shared DataFrame utilities used across all transform modules.

| Function | Description |
|---|---|
| `load_csv_df(path, ...)` | Loads a CSV with optional column mapping, forward fill, collapse, and location matching |
| `df_to_entities(df, cls)` | Converts a Polars DataFrame to a list of typed dataclass instances |
| `forward_fill(df, cols)` | Forward-fills specified columns (common in hierarchical report tables) |
| `collapse(df, none_col, baseline_col)` | Filters to the most granular rows and removes "breakdown" label artifacts |
| `normalize_datetime(df, ...)` | Parses date+time columns into a normalized datetime column; tries multiple format patterns |
| `to_int / to_float / to_str / to_million_php` | Type-casting helpers that strip commas, whitespace, and non-numeric characters |
| `concat_loc_levels(df, loc_cols, sep)` | Concatenates region/province/city columns into a single location string for matching |
| `event_name_expander(name)` | Expands PAGASA abbreviations (TY, TS, TC, TD, LPA) to full names |
| `move_col_values(df, arg)` | Moves values from a source column to a destination column under configurable conditions |
| `correct_QTY_Barangay_column(df)` | Renames the REGION-containing column to `QTY` and `Barangay` to `hasBarangay` |
| `replace_column_whitespace_with_underscore(df)` | Normalizes column names by replacing whitespace with `_` |
| `remove_summary_rows(df, nulls)` | Removes rows where all specified columns are null (summary rows) |
