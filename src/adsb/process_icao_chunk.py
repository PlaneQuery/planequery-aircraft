"""
Processes a chunk of ICAOs from pre-extracted trace files.
This is the map phase of the map-reduce pipeline.

Expects extract_dir to already exist with trace files.
Reads ICAO manifest to determine which ICAOs to process based on chunk-id.

Usage:
    python -m src.adsb.process_icao_chunk --chunk-id 0 --total-chunks 4
"""
import gc
import os
import sys
import argparse
import time
import concurrent.futures
from datetime import datetime, timedelta

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


def get_target_day() -> datetime:
    """Get yesterday's date (the day we're processing)."""
    return datetime.utcnow() - timedelta(days=1)


def read_manifest(date_str: str) -> list[str]:
    """Read ICAO manifest file."""
    manifest_path = os.path.join(OUTPUT_DIR, f"icao_manifest_{date_str}.txt")
    if not os.path.exists(manifest_path):
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    
    with open(manifest_path, "r") as f:
        icaos = [line.strip() for line in f if line.strip()]
    return icaos


def deterministic_hash(s: str) -> int:
    """Return a deterministic hash for a string (unlike Python's hash() which is randomized)."""
    # Use sum of byte values - simple but deterministic
    return sum(ord(c) for c in s)


def get_chunk_icaos(icaos: list[str], chunk_id: int, total_chunks: int) -> list[str]:
    """Get the subset of ICAOs for this chunk based on deterministic hash partitioning."""
    return [icao for icao in icaos if deterministic_hash(icao) % total_chunks == chunk_id]


def build_trace_file_map(extract_dir: str) -> dict[str, str]:
    """Build a map of ICAO -> trace file path using find command."""
    print(f"Building trace file map from {extract_dir}...")
    
    # Debug: check what's in extract_dir
    if os.path.isdir(extract_dir):
        items = os.listdir(extract_dir)[:10]
        print(f"First 10 items in extract_dir: {items}")
        # Check if there are subdirectories
        for item in items[:3]:
            subpath = os.path.join(extract_dir, item)
            if os.path.isdir(subpath):
                subitems = os.listdir(subpath)[:5]
                print(f"  Contents of {item}/: {subitems}")
    
    trace_map = collect_trace_files_with_find(extract_dir)
    print(f"Found {len(trace_map)} trace files")
    
    if len(trace_map) == 0:
        # Debug: try manual find
        import subprocess
        result = subprocess.run(
            ['find', extract_dir, '-type', 'f', '-name', 'trace_full_*'],
            capture_output=True, text=True
        )
        print(f"Manual find output (first 500 chars): {result.stdout[:500]}")
        print(f"Manual find stderr: {result.stderr[:200]}")
    
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
    chunk_id: int,
    total_chunks: int,
    trace_map: dict[str, str],
    icaos: list[str],
    date_str: str,
) -> str | None:
    """Process a chunk of ICAOs and write to parquet."""
    chunk_icaos = get_chunk_icaos(icaos, chunk_id, total_chunks)
    print(f"Chunk {chunk_id}/{total_chunks}: Processing {len(chunk_icaos)} ICAOs")
    
    if not chunk_icaos:
        print(f"Chunk {chunk_id}: No ICAOs to process")
        return None
    
    # Get trace file paths from the map
    trace_files = []
    for icao in chunk_icaos:
        if icao in trace_map:
            trace_files.append(trace_map[icao])
    
    print(f"Chunk {chunk_id}: Found {len(trace_files)} trace files")
    
    if not trace_files:
        print(f"Chunk {chunk_id}: No trace files found")
        return None
    
    # Process files and write parquet in batches
    output_path = os.path.join(CHUNK_OUTPUT_DIR, f"chunk_{chunk_id}_{date_str}.parquet")
    
    start_time = time.perf_counter()
    total_rows = 0
    batch_rows = []
    writer = None
    
    try:
        # Process in parallel batches
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
                            total_rows += len(batch_rows)
                            
                            if writer is None:
                                writer = pq.ParquetWriter(output_path, PARQUET_SCHEMA, compression='snappy')
                            writer.write_table(table)
                            
                            batch_rows = []
                            del table
                            gc.collect()
                            
                            elapsed = time.perf_counter() - start_time
                            print(f"Chunk {chunk_id}: {total_rows} rows, {elapsed:.1f}s | {get_resource_usage()}")
            
            gc.collect()
        
        # Write remaining rows
        if batch_rows:
            table = rows_to_table(batch_rows)
            total_rows += len(batch_rows)
            
            if writer is None:
                writer = pq.ParquetWriter(output_path, PARQUET_SCHEMA, compression='snappy')
            writer.write_table(table)
            del table
    
    finally:
        if writer:
            writer.close()
    
    elapsed = time.perf_counter() - start_time
    print(f"Chunk {chunk_id}: Done! {total_rows} rows in {elapsed:.1f}s | {get_resource_usage()}")
    
    if total_rows > 0:
        return output_path
    return None


def main():
    parser = argparse.ArgumentParser(description="Process a chunk of ICAOs")
    parser.add_argument("--chunk-id", type=int, required=True, help="Chunk ID (0-indexed)")
    parser.add_argument("--total-chunks", type=int, required=True, help="Total number of chunks")
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format (default: yesterday)")
    args = parser.parse_args()
    
    if args.date:
        target_day = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        target_day = get_target_day()
    
    date_str = target_day.strftime("%Y-%m-%d")
    version_date = f"v{target_day.strftime('%Y.%m.%d')}"
    
    print(f"Processing chunk {args.chunk_id}/{args.total_chunks} for {date_str}")
    print(f"OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"CHUNK_OUTPUT_DIR: {CHUNK_OUTPUT_DIR}")
    print(f"Resource usage at start: {get_resource_usage()}")
    
    # Debug: List what's in OUTPUT_DIR
    print(f"\nContents of {OUTPUT_DIR}:")
    if os.path.isdir(OUTPUT_DIR):
        for item in os.listdir(OUTPUT_DIR)[:20]:
            print(f"  - {item}")
    else:
        print(f"  Directory does not exist!")
    
    # Find extract directory
    extract_dir = os.path.join(OUTPUT_DIR, f"{version_date}-planes-readsb-prod-0.tar_0")
    print(f"\nLooking for extract_dir: {extract_dir}")
    if not os.path.isdir(extract_dir):
        print(f"Extract directory not found: {extract_dir}")
        # Try to find any extracted directory
        import glob
        pattern = os.path.join(OUTPUT_DIR, "*-planes-readsb-prod-0*")
        matches = glob.glob(pattern)
        print(f"Searching for pattern: {pattern}")
        print(f"Found matches: {matches}")
        sys.exit(1)
    
    # Build trace file map using find
    trace_map = build_trace_file_map(extract_dir)
    if not trace_map:
        print("No trace files found in extract directory")
        sys.exit(1)
    
    # Read manifest
    icaos = read_manifest(date_str)
    print(f"Total ICAOs in manifest: {len(icaos)}")
    
    # Process chunk
    output_path = process_chunk(
        args.chunk_id,
        args.total_chunks,
        trace_map,
        icaos,
        date_str,
    )
    
    if output_path:
        print(f"Output: {output_path}")
    else:
        print("No output generated")


if __name__ == "__main__":
    main()
