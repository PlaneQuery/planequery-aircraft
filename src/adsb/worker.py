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
import pandas as pd

from compress_adsb_to_aircraft_data import load_historical_for_day, COLUMNS


def deduplicate_by_signature(df: pd.DataFrame) -> pd.DataFrame:
    """For each icao, keep only the earliest row with each unique signature."""
    df["_signature"] = df[COLUMNS].astype(str).agg("|".join, axis=1)
    df_deduped = df.groupby(["icao", "_signature"], as_index=False).first()
    df_deduped = df_deduped.drop(columns=["_signature"])
    df_deduped = df_deduped.sort_values("time")
    return df_deduped


def main():
    start_date_str = os.environ["START_DATE"]
    end_date_str = os.environ["END_DATE"]
    s3_bucket = os.environ["S3_BUCKET"]
    run_id = os.environ.get("RUN_ID", "default")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    total_days = (end_date - start_date).days
    print(f"Worker: processing {total_days} days [{start_date_str}, {end_date_str})")

    df_accumulated = pd.DataFrame()
    current_date = start_date

    while current_date < end_date:
        day_str = current_date.strftime("%Y-%m-%d")
        print(f"  Loading {day_str}...")

        try:
            df_compressed = load_historical_for_day(current_date)
        except Exception as e:
            print(f"  WARNING: Failed to load {day_str}: {e}")
            current_date += timedelta(days=1)
            continue

        if df_accumulated.empty:
            df_accumulated = df_compressed
        else:
            df_accumulated = pd.concat(
                [df_accumulated, df_compressed], ignore_index=True
            )

        print(f"  +{len(df_compressed)} rows (total: {len(df_accumulated)})")

        # Delete local cache after each day to save disk in container
        cache_dir = Path("data/adsb")
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)

        current_date += timedelta(days=1)

    if df_accumulated.empty:
        print("No data collected — exiting.")
        return

    # Deduplicate within this chunk
    df_accumulated = deduplicate_by_signature(df_accumulated)
    print(f"After dedup: {len(df_accumulated)} rows")

    # Write to local file then upload to S3
    local_path = Path(f"/tmp/chunk_{start_date_str}_{end_date_str}.csv.gz")
    df_accumulated.to_csv(local_path, index=False, compression="gzip")

    s3_key = f"intermediate/{run_id}/chunk_{start_date_str}_{end_date_str}.csv.gz"
    print(f"Uploading to s3://{s3_bucket}/{s3_key}")

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), s3_bucket, s3_key)
    print("Done.")


if __name__ == "__main__":
    main()
