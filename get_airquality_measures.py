"""
NILU Air Quality Downloader
---------------------------

Purpose
-------
Fetch historical air-quality measurements from https://api.nilu.no for every station,
year-by-year, with robust retries and logging. Data is written both as per-station/year
checkpoints (so you can resume later) and as optional aggregated outputs.

What it does (high level)
-------------------------
1) Downloads the station lookup table (id, name, first/last measurement dates).
2) Loops over each station and each year in its available range.
3) Requests historical observations, normalizes them, and adds:
   - time (UTC), component, station id, plus NILU-provided fields (value, unit, etc.).
4) Writes a compressed CSV **per station-year** so progress is checkpointed.
5) (Optional) Appends to an in-memory DataFrame and writes final aggregated files.

Outputs
-------
In STORE_DIR (default: E:/airquality/):
- stations.csv
  Station metadata from NILU (indexed by station id), with dates parsed as UTC.

- raw/measurements_<station_id>_<year>.csv.gz
  Per station-year checkpoint files (compressed CSV). Safe to resume: existing files are skipped.

- measurements.csv
  Aggregated CSV of all downloaded data (if you keep the in-memory aggregation).

- measurements.pq
  Aggregated Parquet of all downloaded data (engine='fastparquet').

Key behaviors / features
------------------------
- Reuses a single requests.Session() for speed.
- Retries failed requests with exponential backoff + jitter.
- Uses logging with timestamps and levels (INFO/WARNING/ERROR).
- Parses timestamps with utc=True, drops fromTime/toTime defensively if present.
- Checkpoint strategy: per station-year files allow safe reruns (skip if file exists).

Configuration
-------------
Edit these constants near the top of the file:
- STORE_DIR      : base output directory (Path)
- WRITE_DIR_RAW  : STORE_DIR / "raw" (created automatically)
- DEFAULT_TIMEOUT: request timeout in seconds
- MAX_RETRIES    : number of retry attempts per request

Requirements
------------
Python 3.8+ recommended
pip install: requests, pandas, numpy, fastparquet (for Parquet writes)

Usage
-----
Run the script directly:
    python your_script.py

Notes
-----
- Consider adding 'raw/', '*.csv.gz', '*.parquet', 'stations.csv', 'measurements.*'
  to .gitignore to avoid committing large data artifacts.
- If you only need checkpointed outputs and want to reduce memory, you can skip
  the final aggregation step by removing the in-memory concat and final writes.
"""

from pathlib import Path
import requests
import numpy as np
import pandas as pd
import time
import random
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ---------- config ----------
def pick_store_dir() -> Path:
    """Choose a writable output directory with sensible fallbacks."""
    candidates = [
        Path("E:/airquality"),                              # original target (if E: exists)
        Path.home() / "airquality",                         # e.g., C:\Users\<you>\airquality
        Path(__file__).resolve().parent / "data" / "airquality",  # inside repo
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    # last resort: current working directory
    p = Path.cwd() / "airquality"
    p.mkdir(parents=True, exist_ok=True)
    return p

STORE_DIR = pick_store_dir()
WRITE_DIR_RAW = STORE_DIR / "raw"
WRITE_DIR_RAW.mkdir(exist_ok=True)

logging.info("Output directory: %s", STORE_DIR)

apiurl = 'https://api.nilu.no/'
obshistoryurl = f'{apiurl}obs/historical/'
stationlookupurl = f'{apiurl}lookup/stations'

# ---------- one reusable HTTP session + retry/backoff ----------
session = requests.Session()
DEFAULT_TIMEOUT = 8.0
MAX_RETRIES = 6

def sendrequest(stationname, year,
                nattempts=MAX_RETRIES, timeoutlo=4, timeouthi=9, sleepfactor=5):
    querystring = f'{obshistoryurl}{year}-01-01/{year}-12-31/{stationname}'
    last_err = None
    for attempt in range(nattempts):
        try:
            timeout = np.random.randint(timeoutlo, timeouthi)
            resp = session.get(querystring, timeout=max(timeout, DEFAULT_TIMEOUT))
            if resp.ok:
                return resp
            else:
                last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as err:
            last_err = err

        wait = min(1.8 ** attempt, 60) + random.uniform(-0.4, 0.4)
        wait = max(0.2, wait)
        logging.warning(
            "Retrying %s %d after error %s. Waiting %.1fs…",
            stationname, year, last_err, wait
        )
        time.sleep(wait)

    raise RuntimeError(f"Failed {stationname} {year} after {nattempts} attempts ({last_err})")

# ---------- fetch station metadata ----------
resp = session.get(stationlookupurl, timeout=DEFAULT_TIMEOUT)
resp.raise_for_status()

stationdata = pd.DataFrame(resp.json())
stationdata.set_index('id', inplace=True)
# timezone-aware + defensive parsing
stationdata['firstMeasurment'] = pd.to_datetime(stationdata['firstMeasurment'], errors='coerce', utc=True)
stationdata['lastMeasurment']  = pd.to_datetime(stationdata['lastMeasurment'],  errors='coerce', utc=True)

stationdata.to_csv(STORE_DIR / 'stations.csv')

# ---------- main download loop ----------
failure = []
measuredata = pd.DataFrame()

for sid in stationdata.index:
    stationname = stationdata.loc[sid, 'station']  # define first
    logging.info("Loading data for %s (%s)", sid, stationname)
    tic = time.time()
    startyear = int(stationdata.loc[sid, 'firstMeasurment'].year)
    endyear   = int(stationdata.loc[sid, 'lastMeasurment'].year)

    for year in range(startyear, endyear + 1):
        resp = sendrequest(stationname, year)
        payload = resp.json()

        if payload:
            # build a small list, then concat once (faster)
            block_frames = []
            for cmeasure in payload:
                temp = pd.DataFrame(cmeasure.get('values', []))
                if temp.empty:
                    continue
                temp['component'] = cmeasure.get('component')
                temp['id'] = sid

                # timezone-aware and defensive
                if 'fromTime' in temp:
                    temp['time'] = pd.to_datetime(temp['fromTime'], errors='coerce', utc=True)
                elif 'toTime' in temp:
                    temp['time'] = pd.to_datetime(temp['toTime'],   errors='coerce', utc=True)

                for c in ('fromTime', 'toTime'):
                    if c in temp.columns:
                        temp.drop(columns=[c], inplace=True)

                block_frames.append(temp)

            if block_frames:
                year_df = pd.concat(block_frames, ignore_index=True)

                # checkpoint per station-year (skip if exists)
                out_csv = WRITE_DIR_RAW / f"measurements_{sid}_{year}.csv.gz"
                if out_csv.exists():
                    logging.info("  %s exists, skipping", out_csv.name)
                else:
                    year_df.to_csv(out_csv, index=False, compression='gzip')
                    logging.info("  wrote %s rows to %s", len(year_df), out_csv.name)

                # keep aggregated output (optional)
                measuredata = pd.concat((measuredata, year_df), ignore_index=True)
        else:
            failure.append((sid, year))

    logging.info("Time taken: %.2f min", (time.time() - tic) / 60.0)

# ---------- final outputs (optional aggregated files) ----------
measuredata.to_csv(STORE_DIR / 'measurements.csv', index=False)
measuredata.to_parquet(STORE_DIR / 'measurements.pq', engine='fastparquet', index=False)
