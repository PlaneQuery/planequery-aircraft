#!/usr/bin/env python3
"""
Download Mictronics aircraft database zip.

Usage:
    python -m src.contributions.create_daily_microtonics_release [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

URL = "https://www.mictronics.de/aircraft-database/indexedDB_old.php"
OUT_ROOT = Path("data/openairframes")
MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds


def main() -> None:
    parser = argparse.ArgumentParser(description="Create daily Mictronics database release")
    parser.add_argument("--date", type=str, help="Date to process (YYYY-MM-DD format, default: today UTC)")
    args = parser.parse_args()

    date_str = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    zip_path = OUT_ROOT / f"mictronics-db_{date_str}.zip"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Downloading {URL} (attempt {attempt}/{MAX_RETRIES})...")
            req = Request(URL, headers={"User-Agent": "Mozilla/5.0 (compatible; openairframes-downloader/1.0)"}, method="GET")
            with urlopen(req, timeout=120) as r, zip_path.open("wb") as f:
                shutil.copyfileobj(r, f)
            print(f"Wrote: {zip_path}")
            return
        except (URLError, TimeoutError) as e:
            print(f"Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print("All retries exhausted. Mictronics download failed.")
                sys.exit(1)


if __name__ == "__main__":
    main()
