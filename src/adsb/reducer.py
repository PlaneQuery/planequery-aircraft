"""
Reduce step: downloads all chunk CSVs from S3, combines them,
deduplicates across the full dataset, and uploads the final result.

Environment variables:
  S3_BUCKET         — bucket with intermediate results
  RUN_ID            — run identifier matching the map workers
  GLOBAL_START_DATE — overall start date for output filename
  GLOBAL_END_DATE   — overall end date for output filename
"""
import gzip
import os
import shutil
from pathlib import Path

import boto3
import polars as pl

from compress_adsb_to_aircraft_data import COLUMNS, deduplicate_by_signature


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

    dfs = []

    for key in chunk_keys:
        gz_path = download_dir / Path(key).name
        csv_path = gz_path.with_suffix("")  # Remove .gz
        print(f"Downloading {key}...")
        s3.download_file(s3_bucket, key, str(gz_path))

        # Decompress
        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        gz_path.unlink()

        df_chunk = pl.read_csv(csv_path)
        print(f"  Loaded {df_chunk.height} rows from {csv_path.name}")
        dfs.append(df_chunk)

        # Free disk space after loading
        csv_path.unlink()

    df_accumulated = pl.concat(dfs) if dfs else pl.DataFrame()
    print(f"Combined: {df_accumulated.height} rows before dedup")

    # Final global deduplication
    df_accumulated = deduplicate_by_signature(df_accumulated)
    print(f"After dedup: {df_accumulated.height} rows")

    # Write and upload final result
    output_name = f"openairframes_adsb_{global_start}_{global_end}.csv.gz"
    csv_output = Path(f"/tmp/openairframes_adsb_{global_start}_{global_end}.csv")
    gz_output = Path(f"/tmp/{output_name}")
    
    df_accumulated.write_csv(csv_output)
    with open(csv_output, 'rb') as f_in:
        with gzip.open(gz_output, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    csv_output.unlink()

    final_key = f"final/{output_name}"
    print(f"Uploading to s3://{s3_bucket}/{final_key}")
    s3.upload_file(str(gz_output), s3_bucket, final_key)

    print(f"Final output: {df_accumulated.height} records -> {final_key}")


if __name__ == "__main__":
    main()
