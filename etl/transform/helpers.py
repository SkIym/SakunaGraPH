
from attr import dataclass
import polars as pl
from polars import DataFrame
import re
from typing import Sequence, Mapping, Type, TypeVar, Protocol, ClassVar, Any
from semantic_processing.location_matcher_v2 import LOCATION_MATCHER
from dataclasses import fields

@dataclass
class MoveArg:
    source_col: str
    dest_col: str
    remain: list[str] | None

class DataclassProtocol(Protocol):
    __dataclass_fields__: ClassVar[dict[str, Any]]

T = TypeVar("T", bound=DataclassProtocol)

def load_csv_df(
    path: str,
    *,
    mapping: dict[str, str] | None = None,
    mapping_tokens: dict[str, list[list[str]]] | None = None,
    target_cols: list[str] | None = None,
    collapse_on: str | None = None,
    collapse_key: str | None = None,
    replace_ws: bool = False,
    match_location: bool = True,
    correct_qty_barangay: bool = True,
    schema_overrides: Mapping[str, pl.DataType] | None = None,
    move_values: MoveArg | None = None,
    region_switch: bool = False,
    split_assistance: bool = False, 
) -> pl.DataFrame:
    
    df = pl.read_csv(
        path, 
        schema_overrides=schema_overrides,
        infer_schema_length=10000)
    
    df = df.filter()
    
    df = df.rename({col: col.lower() for col in df.columns})
    df = df.rename(mapping={"city_muni": "municipality"}, strict=False)
    df = df.with_columns(
        pl.col("municipality").str.replace("(capital)", "", literal=True)
    )

    if split_assistance:
        df = split_merged_cost_columns(df)
        df = split_merged_cost_values(df)

    if correct_qty_barangay:
        df = correct_qty_barangay_column(df)

    if replace_ws:
        df = replace_column_whitespace_with_underscore(df)

    if mapping:
        df = df.rename(mapping=mapping, strict=False)

    if mapping_tokens:
        df = normalize_columns(df, mapping_tokens)

    if move_values:
        df = move_col_values(df, move_values)
    else:
        df = move_col_values(df, MoveArg(source_col="summary_type", dest_col="municipality", remain=None))

    if region_switch:
        df = move_invalid_region_values(df, "region", "municipality")

    if target_cols:
        df = df.with_columns(
            pl.col("province").replace("Province", None)
        )
        df = forward_fill(df, target_cols)

    # filter out summary province-wide rwows
    df = remove_rows_by_word(df, "municipality", ["province-wide", "plgu"])

    # filter out ncr value in province column
    df = remove_values_from_column(df, "province", ["ncr"])

    if collapse_key:
        df = collapse(df, collapse_on, collapse_key)

    if match_location:
        locations = concat_loc_levels(df, ["municipality", "province", "region"], ",")
        df = df.with_columns(
            pl.Series("hasLocation", LOCATION_MATCHER.match(locations))
        )

    return df

def df_to_entities(df: pl.DataFrame, cls: Type[T]) -> list[T]:
    class_fields = fields(cls)

    entities: list[T] = []

    for row in df.to_dicts():
        data = {}

        for f in class_fields:
            value = row.get(f.name, None)

            if value is None or (isinstance(value, str) and value.strip().lower() == "none"):
                data[f.name] = None
            else:
                data[f.name] = value

        entities.append(cls(**data))

    return entities


def move_invalid_region_values(
    df: DataFrame,
    source_col: str,
    dest_col: str
) -> DataFrame:
    
    is_invalid = (
        ~pl.col(source_col).str.to_lowercase().is_in([
            "mimaropa", "calabarzon", "ncr", "car", 
            "barmm", "armm", "caraga", "soccsksargen", "nir"
        ])
        & ~pl.col(source_col).str.to_lowercase().str.contains("region")
    )

    return df.with_columns(
        pl.when(is_invalid)
          .then(pl.col(source_col))
          .otherwise(pl.col(dest_col))
          .alias(dest_col),

        pl.when(is_invalid)
          .then(None)
          .otherwise(pl.col(source_col))
          .alias(source_col),
    )


def forward_fill(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Forward fill specified columns.

    :param cols: columns to forward fill
    """
    return df.with_columns([
        pl.col(c).forward_fill() for c in cols
    ])


def collapse(
    df: DataFrame,
    none_col: str | None,
    baseline_col: str | None
) -> DataFrame:

    if baseline_col is None:
        return df  # nothing meaningful to do

    if none_col is not None:
        df = df.filter(
            (pl.col(none_col).is_null()) &
            (pl.col(baseline_col).is_not_null())
        )
    else:
        # baseline-only mode: just keep meaningful rows
        df = df.filter(pl.col(baseline_col).is_not_null())

    df = df.with_columns([
        pl.when(pl.col(c).str.contains("breakdown", literal=False, strict=False))
        .then(None)
        .otherwise(pl.col(c))
        .alias(c)
        for c in ["municipality", "province"]
    ])

    return df


def forward_fill_and_collapse(df: DataFrame, cols: list[str], none_col: str, baseline_col: str) -> DataFrame:
    """Kept for backwards compatibility."""
    df = forward_fill(df, cols)
    df = collapse(df, none_col, baseline_col)
    return df

def event_name_expander(name: str) -> str:
    """
    Docstring for event_name_expander
    
    :param name: Abbreviated cyclone tag
    :type name: str
    :return: Expanded cyclone tag
    :rtype: str
    """

    abbr = {
        "TC": "Tropical cyclone",
        "TD": "Tropical depression",
        "TS": "Tropical storm",
        "LPA": "Low pressure area",
        "TY": "Typhoon"
    }

    for a in abbr.keys():
        if a in name:
            return name.replace(a, abbr[a])
    
    return name

def normalize_datetime(
    df: DataFrame,
    date_col: str,
    time_col: str | None,
    datetime_formats: Sequence[str],  
    date_formats: Sequence[str],      
    new_col: str
) -> DataFrame:
    """
    Normalize date and time values into ISO datetime format.

    :param df: DataFrame
    :param date_col: column containing dates
    :param time_col: column containing time (optional)
    :param datetime_formats: list of possible datetime formats
    :param date_formats: list of possible date-only formats
    :param new_col: name of the resulting normalized column
    """

    # Forward fill date column (common in reports)
    df = df.with_columns(
        pl.col(date_col).forward_fill()
    )

    # Build datetime parsing attempts
    if time_col:
        combined = (
            pl.concat_str([date_col, time_col], separator=" ", ignore_nulls=True)
            .str.strip_chars()
        )

        datetime_parsers = [
            combined.str.strptime(pl.Datetime, fmt, strict=False)
            for fmt in datetime_formats
        ]

        date_parsers = [
            pl.col(date_col)
              .str.strip_chars()
              .str.strptime(pl.Date, fmt, strict=False)
            for fmt in date_formats
        ]

        df = df.with_columns(
            pl.coalesce([
                *datetime_parsers,
                *date_parsers
            ]).alias(new_col)
        )

    else:
        date_parsers = [
            pl.col(date_col)
              .str.strip_chars()
              .str.strptime(pl.Date, fmt, strict=False)
            for fmt in date_formats
        ]

        df = df.with_columns(
            pl.coalesce(date_parsers).alias(new_col)
        )

    return df

def to_int(df: DataFrame, cols: list[str]):
    """
    Cast df columns to integer (handles comma'd values)
    """
    df = df.with_columns(
        pl.col(col)
            .cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.replace_all(" ", "")
            .str.replace_all(r"[^0-9.\-]", "")
            .cast(pl.Float64, strict=False)
            .round(0)
            .cast(pl.Int64, strict=False)
        for col in cols if col in df.columns
    )

    return df

def to_str(df: DataFrame, cols: list[str]):
    """
    Cast df columns to string
    """
    df = df.with_columns(
        pl.col(col)
            .cast(pl.Utf8, strict=False)
        for col in cols if col in df.columns
    )

    return df

def to_float(df: DataFrame, cols: list[str]):
    """
    Cast df columns to decimal (handles comma'd values)
    """
    df = df.with_columns(
        pl.col(col)
            .cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.replace_all(" ", "")
            .str.replace_all(r"[^0-9.\-]", "")
            .cast(pl.Float64, strict=False)
        for col in cols if col in df.columns
    )

    return df

def to_million_php(df: DataFrame, cols: list[str]):
    """
    Convert monetary values to millions PHP
    """
    df = df.with_columns(
        (pl.col(col)
            .cast(pl.Utf8, strict=False)
            .str.replace_all(",", "")
            .str.replace_all(" ", "")
            .str.replace_all("-", "")
            .str.replace_all(r"[^0-9.\-]", "")
            .replace("", None) 
            .cast(pl.Float64)
            / 1000000)
        .round(6)
        for col in cols if col in df.columns
    )

    return df

def concat_loc_levels(df: DataFrame, loc_cols: list[str], sep: str):

    locs = (
        df
        .select(
            pl.concat_str(
                loc_cols,
                separator=sep,
                ignore_nulls=True
            ).alias("full location")
        )
        .to_series()
        .to_list()
    )

    return locs

def correct_qty_barangay_column(df: DataFrame):

    rename_dict = {col: "qty" for col in df.columns if "region_" in col}
    rename_dict["barangay"] = "hasBarangay"

    return df.rename(mapping=rename_dict, strict=False)


def remove_summary_rows(df: DataFrame, nulls: list[str]):
    """
    Removes summary rows
    """
    df = df.filter(
        ~pl.all_horizontal(pl.col(c).is_null() for c in nulls)
    )

    # to be continued

    return df

def remove_rows_by_word(df: DataFrame, column: str, words: list[str]) -> DataFrame:
    words_lower = [w.lower() for w in words]
    return df.filter(~pl.col(column).str.to_lowercase().is_in(words_lower))

def remove_values_from_column(df: DataFrame, column: str, words: list[str]) -> DataFrame:
    words_lower = [w.lower() for w in words]
    return df.with_columns(
        pl.when(pl.col(column).str.to_lowercase().is_in(words_lower))
        .then(None)
        .otherwise(pl.col(column))
        .alias(column)
    )

def replace_column_whitespace_with_underscore(df: DataFrame) -> DataFrame:
    # Replace all whitespace characters with '_'
    return df.rename(lambda c: re.sub(r"\s+", "_", c))

def move_col_values(df: DataFrame, arg: MoveArg) -> DataFrame:
    remain = arg.remain
    source_col = arg.source_col
    dest_col = arg.dest_col

    if source_col not in df.columns or dest_col not in df.columns: return df

    if remain:

        pattern = "|".join(remain)
        df = df.with_columns(
            pl.when(~pl.col(source_col).str.contains(pattern))
            .then(pl.col(source_col))
            .otherwise(pl.col(dest_col))
            .alias(dest_col),

            pl.when(~pl.col(source_col).str.contains(pattern))
            .then(pl.lit(None))
            .otherwise(pl.col(source_col))
            .alias(source_col),
        )
    else:
        df = df.with_columns(
            pl.when(pl.col(dest_col).is_null() & (pl.col(source_col).is_not_null() & ~pl.col(source_col).str.contains(r"(?i)total")))
            .then(pl.col(source_col))
            .otherwise(pl.col(dest_col))
            .alias(dest_col)

        )

    return df


def normalize_columns(
    df: pl.DataFrame,
    token_map: dict[str, list[list[str]]],
) -> pl.DataFrame:
    """
    Rename DataFrame columns by matching ANY group of tokens as substrings
    of column names (case-insensitive).

    Each canonical key maps to a list of token-groups:
        - Inner list = AND condition (all tokens must appear)
        - Outer list = OR condition (any group can match)

    Example:
        {
            "displaced_families_inside": [
                ["displaced", "families", "inside"],
                ["inside", "evac", "families"]
            ]
        }
    """
    used_canonicals: set[str] = set()
    rename_map: dict[str, str] = {}

    for col in df.columns:
        col_lower = col.lower()

        matches: list[str] = []
        # print(col)
        for canonical, token_groups in token_map.items():
            # token_groups is List[List[str]]

            if canonical in used_canonicals:
                continue
            for group in token_groups:
                if all(token.lower() in col_lower for token in group):
                    matches.append(canonical)
                    used_canonicals.add(canonical)
                    break  # stop after first matching group for this canonical

        if matches:
            # keep first match (same behavior as before)
            rename_map[col] = matches[0]

    return df.rename(rename_map)

COST_COL_TOKENS = ["dswd", "lgu", "ngo", "others", "nga", "total"]

def split_merged_cost_columns(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        col_lower = col.lower()

        found_tokens = [t for t in COST_COL_TOKENS if t in col_lower]

        if len(found_tokens) < 2:
            continue

        prefix = ".".join(col.split(".")[:-1])
        n = len(found_tokens)
        new_col_names = [
            f"{prefix}.{t}" if prefix else t
            for t in found_tokens
        ]

        split_exprs = [
            pl.col(col)
              .cast(pl.Utf8)
              .str.strip_chars()
              .str.splitn(" ", n + 1)
              .struct.field(f"field_{i}")
              .alias(new_col_names[i])
            for i in range(n)
        ]

        df = (
            df
            .with_columns(split_exprs)
            .drop(col)
        )

        # print(f"  [split] '{col}' → {new_col_names}")

    return df

def normalize_numeric(val: str) -> str:
    val = re.sub(r',\s*\d\s*,', ',', val)
    val = re.sub(r'(\d)\s*,\s*(\d)', r'\1\2', val)
    val = re.sub(r'(\d)\s*\.\s*(\d)', r'\1.\2', val)
    return val.strip('.,')  # ← strip both

def split_merged_cost_values(df: pl.DataFrame) -> pl.DataFrame:
    """
    For columns that are already properly named (e.g. separate DSWD, LGU columns)
    but whose values contain multiple space-separated numbers
    (e.g. "144,784.20 300,000.00"), split the values across the next N columns.
    
    Works for any number of merged values, not just 2.
    """
    # Pattern: a numeric value (with commas/decimals/dash) followed by space + another
    MERGED_VALUE_RE = re.compile(r'^[\d,\.\-]+(?:\s+[\d,\.\-]+)+$')

    cols = df.columns
    result = df

    for i, col in enumerate(cols):
        # Sample non-null values to detect merging
        sample = (
            result
            .filter(pl.col(col).is_not_null())
            .select(pl.col(col).cast(pl.Utf8).str.strip_chars())
            .head(10)
            .to_series()
            .to_list()
        )
        sample = [normalize_numeric(v) for v in sample if v and v not in ("-", "")]
        if not sample:
            continue

        # Count how many are merged (space-separated numerics)
        merged = [v for v in sample if MERGED_VALUE_RE.match(v)]
        if not merged or len(merged) / len(sample) < 0.5:
            continue

        # Determine split count from the most common part count
        part_counts = [len(v.split()) for v in merged]
        n = max(set(part_counts), key=part_counts.count)
        if n < 2:
            continue

        # Find the next n-1 sibling columns to receive the split values
        siblings = cols[i + 1: i + n]
        if len(siblings) < n - 1:
            continue  # not enough sibling columns

        # print(f"  [split_values] '{col}' has {n} merged values → distributing into {[col] + list(siblings)}")

        # Split: first part stays in col, remaining go to siblings
        split_exprs = [
            pl.col(col)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.replace_all(r',\s*\d\s*,', ',')
            .str.replace_all(r'(\d)\s*,\s*(\d)', '${1}${2}')
            .str.replace_all(r'(\d)\s*\.\s*(\d)', '${1}.${2}')
            .str.strip_chars('.,')                              # ← strip both
            .str.splitn(" ", n + 1)
            .struct.field(f"field_{j}")
            .alias(col if j == 0 else siblings[j - 1])
            for j in range(n)
        ]

        result = result.with_columns(split_exprs)

    return result

