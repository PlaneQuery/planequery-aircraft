"""
Processes trace files from pre-extracted directory for a single day.
This is the map phase of the map-reduce pipeline.

Expects extract_dir to already exist with trace files.

Usage:
    python -m src.adsb.process_icao_chunk --chunk-id 0 --date 2026-01-01
"""
import gc
import os
import sys
import argparse
import time
import concurrent.futures
from datetime import datetime, timedelta
import tarfile
import tempfile
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from src.adsb.download_adsb_data_to_parquet import (
    OUTPUT_DIR,
    PARQUET_DIR,
    PARQUET_SCHEMA,
    COLUMNS,
    MAX_WORKERS,
    process_file,
    get_resource_usage,
    collect_trace_files_with_find,
)


CHUNK_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "adsb_chunks")
os.makedirs(CHUNK_OUTPUT_DIR, exist_ok=True)

# Smaller batch size for memory efficiency
BATCH_SIZE = 100_000

def build_trace_file_map(archive_path: str) -> dict[str, str]:
    """Build a map of ICAO -> trace file path by extracting tar.gz archive."""
    print(f"Extracting {archive_path}...")
    
    temp_dir = tempfile.mkdtemp(prefix="adsb_extract_")
    
    with tarfile.open(archive_path, 'r:gz') as tar:
        tar.extractall(path=temp_dir, filter='data')
    
    trace_map = collect_trace_files_with_find(temp_dir)
    print(f"Found {len(trace_map)} trace files")
    
    return trace_map


def safe_process(filepath: str) -> list:
    """Safely process a file, returning empty list on error."""
    try:
        return process_file(filepath)
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return []


def rows_to_table(rows: list) -> pa.Table:
    """Convert list of rows to PyArrow table."""
    import pandas as pd
    df = pd.DataFrame(rows, columns=COLUMNS)
    if not df['time'].dt.tz:
        df['time'] = df['time'].dt.tz_localize('UTC')
    return pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)


def process_chunk(
    trace_map: dict[str, str] | dict[str, list[str]],
    chunk_id: int,
    output_id: str,
) -> str | None:
    """Process trace files and write to a single parquet file.
    
    Args:
        trace_map: Map of ICAO -> trace file path (str) or list of trace file paths (list[str])
        chunk_id: This chunk's ID (0-indexed)
        output_id: Identifier for output file (date or date range)
    """
    
    # Get trace file paths from the map
    trace_files = list(trace_map.values())
    
    # Single output file
    output_path = os.path.join(CHUNK_OUTPUT_DIR, f"chunk_{chunk_id}_{output_id}.parquet")
    
    start_time = time.perf_counter()
    total_rows = 0
    batch_rows = []
    writer = None
    
    try:
        # Open writer once at the start
        writer = pq.ParquetWriter(output_path, PARQUET_SCHEMA, compression='snappy')
        
        # Process files in batches
        files_per_batch = MAX_WORKERS * 100
        for offset in range(0, len(trace_files), files_per_batch):
            batch_files = trace_files[offset:offset + files_per_batch]
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                for rows in executor.map(safe_process, batch_files):
                    if rows:
                        batch_rows.extend(rows)
                        
                        # Write when batch is full
                        if len(batch_rows) >= BATCH_SIZE:
                            table = rows_to_table(batch_rows)
                            writer.write_table(table)
                            total_rows += len(batch_rows)
                            
                            batch_rows = []
                            del table
                            gc.collect()
                            
                            elapsed = time.perf_counter() - start_time
                            print(f"Chunk {chunk_id}: {total_rows} rows, {elapsed:.1f}s | {get_resource_usage()}")
            
            gc.collect()
        
        # Write remaining rows
        if batch_rows:
            table = rows_to_table(batch_rows)
            writer.write_table(table)
            total_rows += len(batch_rows)
            del table
    
    finally:
        if writer:
            writer.close()
    
    elapsed = time.perf_counter() - start_time
    print(f"Chunk {chunk_id}: Done! {total_rows} rows in {elapsed:.1f}s | {get_resource_usage()}")
    
    if total_rows > 0:
        return output_path
    return None


def process_single_day(
    chunk_id: int,
    target_day: datetime,
) -> str | None:
    """Process a single day for this chunk."""
    date_str = target_day.strftime("%Y-%m-%d")
    archive_dir = os.path.join(OUTPUT_DIR, "adsb_archives", date_str)
    
    archive_files = sorted([
        os.path.join(archive_dir, f)
        for f in os.listdir(archive_dir)
        if f.startswith(f"{date_str}_part_") and f.endswith(".tar.gz")
    ])
    
    print(f"Processing {len(archive_files)} archive files")
    
    all_trace_files = []
    for archive_path in archive_files:
        trace_map = build_trace_file_map(archive_path)
        all_trace_files.extend(trace_map.values())
    
    print(f"Total trace files: {len(all_trace_files)}")
    
    # Convert list to dict for process_chunk compatibility
    trace_map = {str(i): path for i, path in enumerate(all_trace_files)}
    
    return process_chunk(trace_map, chunk_id, date_str)


def main():
    parser = argparse.ArgumentParser(description="Process a chunk of ICAOs for a single day")
    parser.add_argument("--chunk-id", type=int, required=True, help="Chunk ID (0-indexed)")
    parser.add_argument("--date", type=str, required=True, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    print(f"Processing chunk {args.chunk_id} for {args.date}")
    print(f"Resource usage: {get_resource_usage()}")
    
    target_day = datetime.strptime(args.date, "%Y-%m-%d")
    output_path = process_single_day(args.chunk_id, target_day)
    
    if output_path:
        print(f"Output: {output_path}")
    else:
        print("No output generated")


if __name__ == "__main__":
    main()