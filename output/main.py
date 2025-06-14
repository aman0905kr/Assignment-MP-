import pandas as pd
from memory_profiler import profile
from line_profiler import profile as Lprofile

# @profile
def dedupe_and_enrich(file_path: str) -> pd.DataFrame:
    """
    1) Reads the CSV at `file_path`
    2) Drops duplicate log_id rows (keeps the first occurrence)
    3) Adds a `geo_region` column (mapping ip_address → Region_x)
    4) Returns the transformed DataFrame
    """
    df = pd.read_csv(file_path)
    
    df_transformed = df.drop_duplicates(subset=["log_id"], keep="first").copy()
    
    df_transformed["geo_region"] = df_transformed["ip_address"].apply(lambda ip: f"Region_{int(ip)}")
    
    user_totals = (
    df_transformed
    .groupby("user_id", as_index=False)["watch_time(min)"]
    .sum()
    .rename(columns={"watch_time(min)": "total_watch_time"})
)
    return user_totals

@profile
# @Lprofile
def final_aggregate():

    final_aggregate_df = pd.DataFrame()

    for i in range(1,100):
        df = dedupe_and_enrich(f"/Users/aman/Desktop/VS Code/DE Assignment (IC)/Data_generation/data/mock_csvs/user_activity_{i}.csv")
        final_aggregate_df = pd.concat([final_aggregate_df, df], ignore_index=True)
        final_aggregate_df = (final_aggregate_df
        .groupby("user_id", as_index=False)["total_watch_time"]
        .sum()
        )
    
    return final_aggregate_df


result = final_aggregate()
print(result)