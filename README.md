# PySpark ETL Sample

Small, self-contained PySpark demo that reads a CSV, cleans/enriches it, aggregates by category, and writes results to Parquet. Great for quick local experimentation or a minimal ETL walkthrough.

## What It Does
- Reads `sample.csv`
- Trims `name`, casts `amount` to `double`, drops rows with missing `amount`
- Adds `status` (`ok` if `amount` > 0, else `bad`)
- Aggregates by `category` with `total_amount` and `row_count`
- Writes output to `output_parquet`

## Requirements
- Python 3.x
- PySpark installed and working locally

## Run It
From the repo root:

```powershell
python etl_sample.py
```

The script uses a local Spark session (`master("local[*]")`) and writes results to `output_parquet`.

## Output
The output is a Parquet dataset in `output_parquet`. The script also prints the row count and shows the aggregated results in the console.

## Notebook
`etl_sample.ipynb` contains a notebook version of the same ETL flow for interactive exploration.

## Notes
- `etl_sample.py` uses hardcoded Windows paths:
  - `d:\Python\pyspark\sample.csv`
  - `d:\Python\pyspark\output_parquet`
- If you move the repo or run on another OS, update those paths in `etl_sample.py`.
