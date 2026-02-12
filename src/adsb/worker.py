"""
Map worker: processes a date range chunk, uploads result to S3.

Environment variables:
  START_DATE  — inclusive, YYYY-MM-DD
  END_DATE    — exclusive, YYYY-MM-DD
  S3_BUCKET   — bucket for intermediate results
  RUN_ID      — unique run identifier for namespacing S3 keys
"""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import polars as pl

from compress_adsb_to_aircraft_data import (
    load_historical_for_day,
    deduplicate_by_signature,
    COLUMNS,
)


def main():
    start_date_str = os.environ["START_DATE"]
    end_date_str = os.environ["END_DATE"]
    s3_bucket = os.environ["S3_BUCKET"]
    run_id = os.environ.get("RUN_ID", "default")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    total_days = (end_date - start_date).days
    print(f"Worker: processing {total_days} days [{start_date_str}, {end_date_str})")

    dfs = []
    current_date = start_date

    while current_date < end_date:
        day_str = current_date.strftime("%Y-%m-%d")
        print(f"  Loading {day_str}...")

        df_compressed = load_historical_for_day(current_date)
        if df_compressed.height == 0:
            raise RuntimeError(f"No data found for {day_str}")

        dfs.append(df_compressed)
        total_rows = sum(df.height for df in dfs)
        print(f"  +{df_compressed.height} rows (total: {total_rows})")

        # Delete local cache after each day to save disk in container
        cache_dir = Path("data/adsb")
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        current_date += timedelta(days=1)

    # Concatenate all days
    df_accumulated = pl.concat(dfs) if dfs else pl.DataFrame()

    # Deduplicate within this chunk
    df_accumulated = deduplicate_by_signature(df_accumulated)
    print(f"After dedup: {df_accumulated.height} rows")

    # Write to local file then upload to S3
    local_path = Path(f"/tmp/chunk_{start_date_str}_{end_date_str}.csv")
    df_accumulated.write_csv(local_path)
    
    # Compress with gzip
    import gzip
    import shutil
    gz_path = Path(f"/tmp/chunk_{start_date_str}_{end_date_str}.csv.gz")
    with open(local_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    local_path.unlink()  # Remove uncompressed file

    s3_key = f"intermediate/{run_id}/chunk_{start_date_str}_{end_date_str}.csv.gz"
    print(f"Uploading to s3://{s3_bucket}/{s3_key}")

    s3 = boto3.client("s3")
    s3.upload_file(str(gz_path), s3_bucket, s3_key)
    print("Done.")


if __name__ == "__main__":
    main()
