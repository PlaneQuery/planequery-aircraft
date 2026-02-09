from pathlib import Path
from datetime import datetime, timezone, timedelta
import sys

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
        if df_new.empty:
            day = day - timedelta(days=1)
            continue
        max_time = df_new['time'].max()    
        max_time = max_time.replace(tzinfo=timezone.utc)
        
        if max_time >= day.replace(hour=23, minute=59, second=59) - timedelta(minutes=5):
            # Data is complete
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
    df_combined = df_combined.sort_values('time').reset_index(drop=True)

    # Save the result
    OUT_ROOT = Path("data/planequery_aircraft")
    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    output_file = OUT_ROOT / f"planequery_aircraft_adsb_{start_date_str}_{date_str}.csv"
    df_combined.to_csv(output_file, index=False)

    print(f"Saved: {output_file}")
    print(f"Total aircraft: {len(df_combined)}")
