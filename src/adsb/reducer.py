"""
Reduce step: downloads all chunk CSVs from S3, combines them,
deduplicates across the full dataset, and uploads the final result.

Environment variables:
  S3_BUCKET         — bucket with intermediate results
  RUN_ID            — run identifier matching the map workers
  GLOBAL_START_DATE — overall start date for output filename
  GLOBAL_END_DATE   — overall end date for output filename
"""
import os
from pathlib import Path

import boto3
import pandas as pd


COLUMNS = ["dbFlags", "ownOp", "year", "desc", "aircraft_category", "r", "t"]


def deduplicate_by_signature(df: pd.DataFrame) -> pd.DataFrame:
    """For each icao, keep only the earliest row with each unique signature."""
    df["_signature"] = df[COLUMNS].astype(str).agg("|".join, axis=1)
    df_deduped = df.groupby(["icao", "_signature"], as_index=False).first()
    df_deduped = df_deduped.drop(columns=["_signature"])
    df_deduped = df_deduped.sort_values("time")
    return df_deduped


def main():
    s3_bucket = os.environ["S3_BUCKET"]
    run_id = os.environ.get("RUN_ID", "default")
    global_start = os.environ["GLOBAL_START_DATE"]
    global_end = os.environ["GLOBAL_END_DATE"]

    s3 = boto3.client("s3")
    prefix = f"intermediate/{run_id}/"

    # List all chunk files for this run
    paginator = s3.get_paginator("list_objects_v2")
    chunk_keys = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv.gz"):
                chunk_keys.append(obj["Key"])

    chunk_keys.sort()
    print(f"Found {len(chunk_keys)} chunks to combine")

    if not chunk_keys:
        print("No chunks found — nothing to reduce.")
        return

    # Download and concatenate all chunks
    download_dir = Path("/tmp/chunks")
    download_dir.mkdir(parents=True, exist_ok=True)

    df_accumulated = pd.DataFrame()

    for key in chunk_keys:
        local_path = download_dir / Path(key).name
        print(f"Downloading {key}...")
        s3.download_file(s3_bucket, key, str(local_path))

        df_chunk = pd.read_csv(local_path, compression="gzip", keep_default_na=False)
        print(f"  Loaded {len(df_chunk)} rows from {local_path.name}")

        if df_accumulated.empty:
            df_accumulated = df_chunk
        else:
            df_accumulated = pd.concat(
                [df_accumulated, df_chunk], ignore_index=True
            )

        # Free disk space after loading
        local_path.unlink()

    print(f"Combined: {len(df_accumulated)} rows before dedup")

    # Final global deduplication
    df_accumulated = deduplicate_by_signature(df_accumulated)
    print(f"After dedup: {len(df_accumulated)} rows")

    # Write and upload final result
    output_name = f"planequery_aircraft_adsb_{global_start}_{global_end}.csv.gz"
    local_output = Path(f"/tmp/{output_name}")
    df_accumulated.to_csv(local_output, index=False, compression="gzip")

    final_key = f"final/{output_name}"
    print(f"Uploading to s3://{s3_bucket}/{final_key}")
    s3.upload_file(str(local_output), s3_bucket, final_key)

    print(f"Final output: {len(df_accumulated)} records -> {final_key}")


if __name__ == "__main__":
    main()
