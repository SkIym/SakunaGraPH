import polars as pl
from polars import DataFrame

def forward_fill_and_collapse(df: DataFrame, cols: list[str], none_col: str, baseline_col: str) -> DataFrame:
    """
    Forward fill location values and collapse rows down only to most granular and detailed level

    :param cols: list of columns to forward fill
    :type cols: list[str]
    :param none_col: column that should be none 
    :param baseline_col: column that dictates the identity of the entry
    """
    
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

    # output_path = os.path.join(folder_path, "ffilled.csv")

    return df

def event_name_expander(name: str) -> str:

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

# def normalize_datetime(folder_path: str, date_col: str, time_col: str):


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

