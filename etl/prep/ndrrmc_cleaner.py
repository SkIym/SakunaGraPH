import os
import polars as pl

def forward_fill_and_collapse(folder_path: str, cols: list[str]):
    """
    Forward fill location values and collapse rows down only to most granular and detailed level
    
    :param folder_path: subfolder path
    :type folder_path: str
    :param cols: list of columns to forward fill
    :type cols: list[str]
    """
    files = os.listdir(folder_path)
    print(files)
    
    for cpt in files:
        if cpt.endswith("incidents.csv"):
            print(cpt)
            file_path = os.path.join(folder_path, cpt)
            
            # Load the data
            df = pl.read_csv(file_path)

            # 1. Forward fill specified columns
            # 2. Filter out rows where Column_1 is null AND Column_2 > 0
            df = (
                df.with_columns([
                    pl.col(c).forward_fill() for c in cols
                ])
                .filter(
                    ((pl.col("Column_1").is_null()) & (pl.col("Column_2").is_not_null()))
                )
            )

            output_path = os.path.join(folder_path, "ffilled.csv")
            df.write_csv(output_path)

if __name__ == "__main__":
    DATA_DIR = "./data/ndrrmc/TY Ambo 2020/"
    target_cols = ["Region", "Province", "City_Muni"]
    forward_fill_and_collapse(DATA_DIR, target_cols)