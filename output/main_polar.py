import polars as pl
from memory_profiler import profile
from line_profiler import profile as Lprofile

# @profile
def dedupe_and_enrich(file_path: str) -> pl.DataFrame:
    """
    1) Reads the CSV at `file_path`
    2) Drops duplicate log_id rows (keeps the first occurrence)
    3) Adds a `geo_region` column (mapping ip_address → Region_x)
    4) Returns a DataFrame with per-user total watch_time
    """

    df = pl.read_csv(file_path)

    df_transformed = df.unique(subset=["log_id"])

    df_transformed = df_transformed.with_columns(
        (pl.lit("Region_") + pl.col("ip_address").cast(pl.Utf8)).alias("geo_region")
    )

    user_totals = (
        df_transformed
        .group_by("user_id")
        .agg(
            pl.col("watch_time(min)").sum().alias("total_watch_time")
        )
    )
    return user_totals

@Lprofile
# @profile
def final_aggregate() -> pl.DataFrame:
    """
    Reads all user_activity files, transforms them, and computes a global per-user total.
    """
    dfs = []
    for i in range(1, 100):
        file_path = f"/Users/aman/Desktop/VS Code/DE Assignment (IC)/Data_generation/data/mock_csvs/user_activity_{i}.csv"
        dfs.append(dedupe_and_enrich(file_path))

    # Concatenate per-file results
    all_df = pl.concat(dfs)

    # Final aggregation across all files
    final_df = (
        all_df
        .group_by("user_id")
        .agg(
            pl.col("total_watch_time").sum().alias("total_watch_time")
        )
    )
    final_df = final_df.sort(by=["user_id"])
    final_df.write_parquet("/Users/aman/Desktop/VS Code/DE Assignment (IC)/output/result.parquet_zstd", compression = "zstd")
    return final_df

if __name__ == "__main__":
    result = final_aggregate()
    print(result)
