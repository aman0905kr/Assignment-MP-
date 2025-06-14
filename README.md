# ETL Pipeline for Log Data Processing

## Overview
This project provides a Python-based ETL (Extract, Transform, Load) pipeline to process large-scale user activity log data. It performs the following operations:

1. **Data Loading**: Reads CSV files containing user activity logs.
2. **Deduplication**: Removes duplicate records based on `log_id`.
3. **Enrichment**: Adds a `geo_region` column by mapping `ip_address` to a mock region.
4. **Aggregation**: Calculates the total watch time per user.
5. **Output**: Writes the transformed and aggregated results to Parquet format for efficient storage and downstream analysis.

## Features
- **Parallel Processing**: Leverages Polars for fast, memory-efficient data handling.
- **Profiling Support**: Integrates `line_profiler` and `memory_profiler` to help you measure execution time and memory usage.
- **Flexible Configuration**: Easily adjust file paths, worker counts, and profiling settings.

## Prerequisites
- Python 3.9 or higher
- Git (optional, for version control)

## Installation
1. Clone this repository:
   ```bash
   git clone <repository_url>
   cd <repository_folder>

2. Create and activate a virtual environment

3. Install dependencies:
    pip install -r requirements.txt

4. Data Generation (if needed):
    Use : Data_generation/data_generation.ipynb to generate data

5. 
    For transformation/aggregation logic check through pandas:
    output/main.py

    Final_transformation:
    pandas dataframe: output/main_polar.py
    polar dataframe: /output/main_polar_MP.py
    polar + Multiprocessing + parquet compressed files --> output/main_final_aggregations.py

    Note: The output for the above files is the Final answer : total_time_per_user

