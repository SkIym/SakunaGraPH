
import polars as pl
from polars import DataFrame
import re

def forward_fill_and_collapse(df: DataFrame, cols: list[str], none_col: str, baseline_col: str) -> DataFrame:
    """
    Forward fill location values and collapse rows down only to most granular and detailed level

    :param cols: list of columns to forward fill
    :type cols: list[str]
    :param none_col: column that should be none 
    :param baseline_col: column that dictates the identity of the entry
    """

    # Move erratic municities placement
    # if "Summary_Type" in df.columns:

    #     df = df.filter(pl.col("Summary_Type") != "GRAND TOTAL")
    #     df = df.with_columns([
    #         pl.coalesce(["City_Muni", "Summary_Type"]).alias("City_Muni"),
    #     ])
    
    # 1. Forward fill specified columns
    # 2. Filter out rows where Column_1 is null AND Column_2 > 0
    df = (
        df.with_columns([
            pl.col(c).forward_fill() for c in cols
        ])
        .filter(
            ((pl.col(none_col).is_null()) & (pl.col(baseline_col).is_not_null()))
        )
    )

    # set no breakdown to null

    df = df.with_columns([
        pl.when(pl.col(c).str.contains_any(["breakdown"], ascii_case_insensitive=True))
        .then(None)
        .otherwise(pl.col(c))
        .alias(c)
        for c in ["City_Muni", "Province"]
    ])

    # output_path = os.path.join(folder_path, "ffilled.csv")

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
        "LPA": "Low pressure area"
    }

    for a in abbr.keys():
        if a in name:
            return name.replace(a, abbr[a])
    
    return name

def normalize_datetime(df: DataFrame, date_col: str, time_col: str | None, datetime_format: str, date_format: str, new_col: str):
    """
    Normalize date and time values into ISO datetime format
    
    :param df: Dataframe
    :type df: DataFrame
    :param date_col: name of the column containing the dates
    :type date_col: str
    :param time_col: name of the column containing the time
    :type time_col: str
    :param datetime_format: format of the datetime to convert
    :param date_fromat: format of the date to convert (no time)
    """
    df = df.with_columns(
        pl.col(date_col).forward_fill()
    )

    if time_col:

        df = df.with_columns(
            pl.coalesce([
                # try date + time
                pl.concat_str(
                    [date_col, time_col], 
                    separator=" ", 
                    ignore_nulls=True)
                .str.strip_chars()
                .str.strptime(pl.Datetime, 
                            datetime_format, 
                            strict=False),

                # fallback: date only
                pl.col(date_col)
                .str.strip_chars()
                .str.strptime(pl.Datetime, 
                            date_format, 
                            strict=False),
        
            ])
            .alias(new_col)
        )

    else:
        df = df.with_columns(
            pl.col(date_col)
                .str.strip_chars()
                .str.strptime(pl.Date, 
                            date_format, 
                            strict=False),
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
            .cast(pl.Int64, strict=False)
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

