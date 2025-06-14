import polars as pl
from memory_profiler import profile
from line_profiler import profile as Lprofile

from multiprocessing import Process, Queue, cpu_count


def dedupe_and_enrich(file_path: str) -> pl.DataFrame:
    df = pl.read_csv(file_path)
    df = df.unique(subset=["log_id"])
    df = df.with_columns(
        (pl.lit("Region_") + pl.col("ip_address").cast(pl.Utf8))
        .alias("geo_region")
    )
    return (
        df
        .group_by("user_id")
        .agg(pl.col("watch_time(min)").sum().alias("total_watch_time"))
    )

def worker(task_q: Queue, result_q: Queue):
    while True:
        file_path = task_q.get()
        if file_path is None:
            result_q.put(None)
            break
        try:
            df = dedupe_and_enrich(file_path)
            result_q.put(df)
        except Exception as e:
            result_q.put(e)

@Lprofile
def final_aggregate() -> pl.DataFrame:
    """
    Parallelized over file‐level tasks using multiprocessing.Process + Queue.
    """
    file_paths = [
        f"/Users/aman/Desktop/VS Code/DE Assignment (IC)/Data_generation/data/mock_csvs/user_activity_{i}.csv"
        for i in range(1, 100)
    ]

    # Number of workers
    # num_workers = min(cpu_count(), 8)
    # num_workers = 4
    num_workers = 2

    # task/result queues
    task_q   = Queue()
    result_q = Queue()

    # spawning the worker processes
    workers = [
        Process(target=worker, args=(task_q, result_q))
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()

    for fp in file_paths:
        task_q.put(fp)
    for _ in workers:
        task_q.put(None)

    # collecting back the results
    dfs = []
    finished = 0
    while finished < num_workers:
        res = result_q.get()
        if res is None:
            finished += 1
        elif isinstance(res, Exception):
            print("Worker error:", res)
        else:
            dfs.append(res)

    for w in workers:
        w.join()

    # post‐processing
    all_df = pl.concat(dfs)
    final_df = (
        all_df
        .group_by("user_id")
        .agg(pl.col("total_watch_time").sum().alias("total_watch_time"))
        .sort("user_id")
    )
    final_df.write_parquet(
        "/Users/aman/Desktop/VS Code/DE Assignment (IC)/output/result.parquet_zstd_1",
        compression="zstd"
    )
    return final_df

if __name__ == "__main__":
    result = final_aggregate()
    print(result)
