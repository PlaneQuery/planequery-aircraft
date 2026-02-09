# SOME KIND OF MAP REDUCE SYSTEM
import os

COLUMNS = ['dbFlags', 'ownOp', 'year', 'desc', 'aircraft_category', 'r', 't']
def compress_df(df):
    icao = df.name
    df["_signature"] = df[COLUMNS].astype(str).agg('|'.join, axis=1)
    
    # Compute signature counts before grouping (avoid copy)
    signature_counts = df["_signature"].value_counts()
    
    df = df.groupby("_signature", as_index=False).first() # check if it works with both last and first.
    # For each row, create a dict of non-empty column values. This is using sets and subsets...
    def get_non_empty_dict(row):
        return {col: row[col] for col in COLUMNS if row[col] != ''}
    
    df['_non_empty_dict'] = df.apply(get_non_empty_dict, axis=1)
    df['_non_empty_count'] = df['_non_empty_dict'].apply(len)
    
    # Check if row i's non-empty values are a subset of row j's non-empty values
    def is_subset_of_any(idx):
        row_dict = df.loc[idx, '_non_empty_dict']
        row_count = df.loc[idx, '_non_empty_count']
        
        for other_idx in df.index:
            if idx == other_idx:
                continue
            other_dict = df.loc[other_idx, '_non_empty_dict']
            other_count = df.loc[other_idx, '_non_empty_count']
            
            # Check if all non-empty values in current row match those in other row
            if all(row_dict.get(k) == other_dict.get(k) for k in row_dict.keys()):
                # If they match and other has more defined columns, current row is redundant
                if other_count > row_count:
                    return True
        return False
    
    # Keep rows that are not subsets of any other row
    keep_mask = ~df.index.to_series().apply(is_subset_of_any)
    df = df[keep_mask]

    if len(df) > 1:
        # Use pre-computed signature counts instead of original_df
        remaining_sigs = df['_signature']
        sig_counts = signature_counts[remaining_sigs]
        max_signature = sig_counts.idxmax()
        df = df[df['_signature'] == max_signature]

    df['icao'] = icao
    df = df.drop(columns=['_non_empty_dict', '_non_empty_count', '_signature'])
    # Ensure empty strings are preserved, not NaN
    df[COLUMNS] = df[COLUMNS].fillna('')
    return df

# names of releases something like
# planequery_aircraft_adsb_2024-06-01T00-00-00Z.csv.gz

# Let's build historical first. 

def load_raw_adsb_for_day(day):
    """Load raw ADS-B data for a day from parquet file."""
    from datetime import timedelta
    from pathlib import Path
    import pandas as pd
    
    start_time = day.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Check for parquet file first
    version_date = f"v{start_time.strftime('%Y.%m.%d')}"
    parquet_file = Path(f"data/output/parquet_output/{version_date}.parquet")
    
    if not parquet_file.exists():
        # Try to generate parquet file by calling the download function
        print(f"  Parquet file not found: {parquet_file}")
        print(f"  Attempting to download and generate parquet for {start_time.strftime('%Y-%m-%d')}...")
        
        from download_adsb_data_to_parquet import create_parquet_for_day
        result_path = create_parquet_for_day(start_time, keep_folders=False)
        
        if result_path:
            print(f"  Successfully generated parquet file: {result_path}")
        else:
            raise Exception("Failed to generate parquet file")
    
    if parquet_file.exists():
        print(f"  Loading from parquet: {parquet_file}")
        df = pd.read_parquet(
            parquet_file, 
            columns=['time', 'icao', 'r', 't', 'dbFlags', 'ownOp', 'year', 'desc', 'aircraft_category']
        )
        
        # Convert to timezone-naive datetime
        df['time'] = df['time'].dt.tz_localize(None)
        return df
    else:
        # Return empty DataFrame if parquet file doesn't exist
        print(f"  No data available for {start_time.strftime('%Y-%m-%d')}")
        import pandas as pd
        return pd.DataFrame(columns=['time', 'icao', 'r', 't', 'dbFlags', 'ownOp', 'year', 'desc', 'aircraft_category'])

def load_historical_for_day(day):
    from pathlib import Path
    import pandas as pd
    df = load_raw_adsb_for_day(day)
    if df.empty:
        return df
    print(f"Loaded {len(df)} raw records for {day.strftime('%Y-%m-%d')}")
    df = df.sort_values(['icao', 'time'])
    print("done sort")
    df[COLUMNS] = df[COLUMNS].fillna('')
    
    # First pass: quick deduplication of exact duplicates
    df = df.drop_duplicates(subset=['icao'] + COLUMNS, keep='first')
    print(f"After quick dedup: {len(df)} records")
    
    # Second pass: sophisticated compression per ICAO
    print("Compressing per ICAO...")
    df_compressed = df.groupby('icao', group_keys=False).apply(compress_df)
    print(f"After compress: {len(df_compressed)} records")
    
    cols = df_compressed.columns.tolist()
    cols.remove('time')
    cols.insert(0, 'time')
    cols.remove("icao")
    cols.insert(1, "icao")
    df_compressed = df_compressed[cols]
    return df_compressed


def concat_compressed_dfs(df_base, df_new):
    """Concatenate base and new compressed dataframes, keeping the most informative row per ICAO."""
    import pandas as pd
    
    # Combine both dataframes
    df_combined = pd.concat([df_base, df_new], ignore_index=True)
    
    # Sort by ICAO and time
    df_combined = df_combined.sort_values(['icao', 'time'])
    
    # Fill NaN values
    df_combined[COLUMNS] = df_combined[COLUMNS].fillna('')
    
    # Apply compression logic per ICAO to get the best row
    df_compressed = df_combined.groupby('icao', group_keys=False).apply(compress_df)
    
    # Sort by time
    df_compressed = df_compressed.sort_values('time')
    
    return df_compressed


def get_latest_aircraft_adsb_csv_df():
    """Download and load the latest ADS-B CSV from GitHub releases."""
    from get_latest_planequery_aircraft_release import download_latest_aircraft_adsb_csv
    
    import pandas as pd
    import re
    
    csv_path = download_latest_aircraft_adsb_csv()
    df = pd.read_csv(csv_path)
    df = df.fillna("")
    
    # Extract start date from filename pattern: planequery_aircraft_adsb_{start_date}_{end_date}.csv
    match = re.search(r"planequery_aircraft_adsb_(\d{4}-\d{2}-\d{2})_", str(csv_path))
    if not match:
        raise ValueError(f"Could not extract date from filename: {csv_path.name}")
    
    date_str = match.group(1)
    return df, date_str

