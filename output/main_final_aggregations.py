import polars as pl
from line_profiler import profile as Lprofile
from memory_profiler import profile

# pl.Config.set_streaming_chunk_size(100)
@profile
# @Lprofile
def final_aggregations():
    input_parquet = "Data_generation/data/user_activity_combined.parquet"

    with pl.Config():
        # lazy aggregation
        agg = (
            pl.scan_parquet(input_parquet, parallel="row_groups")
            .group_by("user_id")                            
            .agg(
                pl.col("watch_time(min)").sum().alias("watch_time(min)")
            )
        )

        return agg.collect()

df = final_aggregations()
print(df)