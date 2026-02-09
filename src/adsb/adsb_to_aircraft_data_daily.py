from pathlib import Path
from datetime import datetime, timezone,timedelta
from adsb_to_aircraft_data_historical import load_historical_for_day


day = datetime.now(timezone.utc) - timedelta(days=1)
load_historical_for_day(day)