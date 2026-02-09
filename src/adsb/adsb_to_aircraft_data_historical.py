"""
Process historical ADS-B data by date range.
Downloads and compresses ADS-B messages for each day in the specified range.
"""
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from compress_adsb_to_aircraft_data import load_historical_for_day, COLUMNS

def deduplicate_by_signature(df):
    """For each icao, keep only the earliest row with each unique signature."""
    df["_signature"] = df[COLUMNS].astype(str).agg('|'.join, axis=1)
    # Group by icao and signature, keep first (earliest) occurrence
    df_deduped = df.groupby(['icao', '_signature'], as_index=False).first()
    df_deduped = df_deduped.drop(columns=['_signature'])
    df_deduped = df_deduped.sort_values('time')
    return df_deduped


def main(start_date_str: str, end_date_str: str):
    """Process historical ADS-B data for the given date range."""
    OUT_ROOT = Path("data/planequery_aircraft")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Parse dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Calculate total number of days
    total_days = (end_date - start_date).days
    print(f"Processing {total_days} days from {start_date_str} to {end_date_str}")

    # Initialize accumulated dataframe
    df_accumulated = pd.DataFrame()

    # Cache directory path
    cache_dir = Path("data/adsb")

    # Iterate through each day
    current_date = start_date
    while current_date < end_date:
        print(f"Processing {current_date.strftime('%Y-%m-%d')}...")
        
        df_compressed = load_historical_for_day(current_date)
        
        # Concatenate to accumulated dataframe
        if df_accumulated.empty:
            df_accumulated = df_compressed
        else:
            df_accumulated = pd.concat([df_accumulated, df_compressed], ignore_index=True)
        
        print(f"  Added {len(df_compressed)} records (total: {len(df_accumulated)})")
        
        # Save intermediate output after each day
        current_date_str = current_date.strftime('%Y-%m-%d')
        output_file = OUT_ROOT / f"planequery_aircraft_adsb_{start_date_str}_{current_date_str}.csv.gz"
        df_deduped = deduplicate_by_signature(df_accumulated.copy())
        df_deduped.to_csv(output_file, index=False, compression='gzip')
        print(f"  Saved to {output_file.name}")
        
        # Delete cache after processing if processing more than 10 days
        if total_days > 5 and cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
            print(f"  Deleted cache directory to save space")
        
        # Move to next day
        current_date += timedelta(days=1)

    # Save the final accumulated data
    output_file = OUT_ROOT / f"planequery_aircraft_adsb_{start_date_str}_{end_date_str}.csv.gz"
    df_accumulated = deduplicate_by_signature(df_accumulated)
    df_accumulated.to_csv(output_file, index=False, compression='gzip')

    print(f"Completed processing from {start_date_str} to {end_date_str}")
    print(f"Saved {len(df_accumulated)} total records to {output_file}")


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Process historical ADS-B data from ClickHouse")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD, inclusive)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD, exclusive)")
    args = parser.parse_args()
    
    main(args.start_date, args.end_date)
