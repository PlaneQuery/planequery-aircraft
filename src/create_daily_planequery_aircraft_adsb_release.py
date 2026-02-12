from pathlib import Path
from datetime import datetime, timezone, timedelta
import sys

import polars as pl

# Add adsb directory to path
sys.path.insert(0, str(Path(__file__).parent / "adsb")) # TODO: Fix this hacky path manipulation

from adsb.compress_adsb_to_aircraft_data import (
    load_historical_for_day,
    concat_compressed_dfs,
    get_latest_aircraft_adsb_csv_df,
)

if __name__ == '__main__':
    # Get yesterday's date (data for the previous day)
    day = datetime.now(timezone.utc) - timedelta(days=1)

    # Find a day with complete data
    max_attempts = 2  # Don't look back more than a week
    for attempt in range(max_attempts):
        date_str = day.strftime("%Y-%m-%d")
        print(f"Processing ADS-B data for {date_str}")
        
        print("Loading new ADS-B data...")
        df_new = load_historical_for_day(day)
        if df_new.height == 0:
            day = day - timedelta(days=1)
            continue
        max_time = df_new['time'].max()
        if max_time is not None:
            # Handle timezone
            max_time_dt = max_time
            if hasattr(max_time_dt, 'replace'):
                max_time_dt = max_time_dt.replace(tzinfo=timezone.utc)
            
            end_of_day = day.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc) - timedelta(minutes=5)
            
            # Convert polars datetime to python datetime if needed
            if isinstance(max_time_dt, datetime):
                if max_time_dt.replace(tzinfo=timezone.utc) >= end_of_day:
                    break
            else:
                # Polars returns python datetime already
                if max_time >= day.replace(hour=23, minute=54, second=59):
                    break
        
        print(f"WARNING: Latest data time is {max_time}, which is more than 5 minutes before end of day.")
        day = day - timedelta(days=1)
    else:
        raise RuntimeError(f"Could not find complete data in the last {max_attempts} days")

    try:
        # Get the latest release data
        print("Downloading latest ADS-B release...")
        df_base, start_date_str = get_latest_aircraft_adsb_csv_df()
        # Combine with historical data
        print("Combining with historical data...")
        df_combined = concat_compressed_dfs(df_base, df_new)
    except Exception as e:
        print(f"Error downloading latest ADS-B release: {e}")
        df_combined = df_new
        start_date_str = date_str

    # Sort by time for consistent ordering
    df_combined = df_combined.sort('time')
    
    # Convert any list columns to strings for CSV compatibility
    for col in df_combined.columns:
        if df_combined[col].dtype == pl.List:
            df_combined = df_combined.with_columns(
                pl.col(col).list.join(",").alias(col)
            )

    # Save the result
    OUT_ROOT = Path("data/planequery_aircraft")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    output_file = OUT_ROOT / f"planequery_aircraft_adsb_{start_date_str}_{date_str}.csv"
    df_combined.write_csv(output_file)

    print(f"Saved: {output_file}")
    print(f"Total aircraft: {df_combined.height}")
