
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
    mapping_tokens: dict[str, list[str]] | None = None,
    target_cols: list[str] | None = None,
    collapse_on: str | None = None,
    collapse_key: str | None = None,
    replace_ws: bool = False,
    match_location: bool = True,
    correct_QTY_Barangay: bool = True,
    schema_overrides: Mapping[str, pl.DataType] | None = None,
    move_values: MoveArg | None = None
) -> pl.DataFrame:
    df = pl.read_csv(
        path, 
        schema_overrides=schema_overrides,
        infer_schema_length=10000)
    
    df = df.filter()

    if correct_QTY_Barangay:

        df = correct_QTY_Barangay_column(df)

    if replace_ws:
        df = replace_column_whitespace_with_underscore(df)

    if mapping:
        df = df.rename(mapping=mapping, strict=False)

    if mapping_tokens:
        df = normalize_columns(df, mapping_tokens)

    if move_values:
        df = move_col_values(df, move_values)
    else:
        df = move_col_values(df, MoveArg(source_col="Summary_Type", dest_col="City_Muni", remain=None))

    if target_cols:
        df = df.with_columns(
            pl.col("Province").replace("Province", None)
        )
        df = forward_fill(df, target_cols)

    if collapse_on and collapse_key:
        df = collapse(df, collapse_on, collapse_key)

    if match_location:
        locations = concat_loc_levels(df, ["City_Muni", "Province", "Region"], ",")
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

# def load_multiple_csvs(
#     folder: str,
#     predicate: Callable[[str], bool],
#     **df_kwargs,
# ) -> pl.DataFrame | None:
#     paths = [
#         os.path.join(folder, f)
#         for f in os.listdir(folder)
#         if predicate(f)
#     ]
#     if not paths:
#         return None

#     dfs = [load_csv_df(p, **df_kwargs) for p in paths]
#     return pl.concat(dfs, rechunk=True)

def forward_fill(df: DataFrame, cols: list[str]) -> DataFrame:
    """
    Forward fill specified columns.

    :param cols: columns to forward fill
    """
    return df.with_columns([
        pl.col(c).forward_fill() for c in cols
    ])


def collapse(df: DataFrame, none_col: str, baseline_col: str) -> DataFrame:
    """
    Collapse rows down to the most granular level and clean breakdown labels.

    :param none_col: column that should be null after collapse
    :param baseline_col: column that dictates the identity of the entry
    """
    df = df.filter(
        (pl.col(none_col).is_null()) & (pl.col(baseline_col).is_not_null())
    )

    df = df.with_columns([
        pl.when(pl.col(c).str.contains_any(["breakdown"], ascii_case_insensitive=True))
        .then(None)
        .otherwise(pl.col(c))
        .alias(c)
        for c in ["City_Muni", "Province"]
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

def correct_QTY_Barangay_column(df: DataFrame):

    rename_dict = {col: "QTY" for col in df.columns if "REGION" in col}
    rename_dict["Barangay"] = "hasBarangay"

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

def replace_column_whitespace_with_underscore(df: DataFrame) -> DataFrame:
    # Replace all whitespace characters with '_'
    return df.rename(lambda c: re.sub(r"\s+", "_", c))

def move_col_values(df: DataFrame, arg: MoveArg) -> DataFrame:
    remain = arg.remain
    source_col = arg.source_col
    dest_col = arg.dest_col

    if source_col not in df.columns and dest_col not in df.columns: return df

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
    token_map: dict[str, list[str]],
) -> pl.DataFrame:
    """
    Rename DataFrame columns by matching all tokens as substrings of column names.

    Args:
        df:        Input Polars DataFrame.
        token_map: Maps canonical names to a list of tokens that must ALL appear
                   as substrings in a column name (case-insensitive AND match).
                   e.g. {"displaced_families_inside": ["displaced", "families", "inside"]}

    Raises:
        ValueError: If a column matches more than one canonical entry.
    """
    rename_map: dict[str, str] = {}

    for col in df.columns:
        col_lower = col.lower()
        matches = [
            canonical
            for canonical, tokens in token_map.items()
            if all(token.lower() in col_lower for token in tokens)
        ]

        # if len(matches) > 1:
        #     raise ValueError(
        #         f"Column '{col}' matched multiple canonicals: {matches}. "
        #         "Resolve ambiguity in token_map."
        #     )
        # elif len(matches) == 1:
        #     rename_map[col] = matches[0]
        if matches:
            rename_map[col] = matches[0]

    return df.rename(rename_map)

# if __name__ == "__main__":
#     DATA_DIR = "./data/ndrrmc/Undas 2023/related_incidents.csv"
#     target_cols = ["Region", "Province", "City_Muni"]
#     df = forward_fill_and_collapse(None, DATA_DIR, target_cols, "Column_1", "Column_2")

#     texts = df.select("Column_2").to_series()


#     predictions = DISASTER_CLASSIFIER.classify(list(texts))

#     for text, (pred_class, score) in zip(texts, predictions):
#         print(f"\nType of incident: {text}")
#         print(f"→ Predicted class: {pred_class}")
#         print(f"→ Similarity score: {score:.4f}")

