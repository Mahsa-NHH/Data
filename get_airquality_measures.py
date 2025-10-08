#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
NILU Air Quality Downloader
---------------------------

Purpose
-------
Fetch historical air-quality measurements from https://api.nilu.no for every station,
year-by-year, with robust retries and logging. Data is written as per-station/year
checkpoints (so you can resume later) and as aggregated outputs.

Outputs (written to STORE_DIR)
------------------------------
- stations.csv
  Station metadata from NILU (indexed by station id), with dates parsed as UTC.

- raw/measurements_<station_id>_<year>.csv.gz
  Per station-year checkpoint files (compressed CSV). Safe to resume: existing files are skipped.

- measurements.csv
  Aggregated CSV of all downloaded data, rebuilt from checkpoint files at the end.

- measurements.pq
  Aggregated Parquet of all downloaded data (engine='fastparquet'), rebuilt from the CSV.

Notes
-----
- The script ALWAYS skips station-year files that already exist (resume-friendly).
- Aggregated outputs are reconstructed from all checkpoint files at the end,
  so they are complete even if many years were skipped during this run.
"""

from pathlib import Path
import logging
import time
import random
import requests
import numpy as np
import pandas as pd

# -------------------------- Logging setup --------------------------
# Timestamped logs for progress and troubleshooting.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# -------------------------- Output directory -----------------------
def pick_store_dir() -> Path:
    """
    Choose a writable output directory with sensible fallbacks.
    Order:
      1) E:/airquality
      2) <home>/airquality
      3) <repo>/data/airquality
      4) <cwd>/airquality
    """
    candidates = [
        Path("E:/airquality"),
        Path.home() / "airquality",
        Path(__file__).resolve().parent / "data" / "airquality",
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    p = Path.cwd() / "airquality"
    p.mkdir(parents=True, exist_ok=True)
    return p

STORE_DIR = pick_store_dir()
WRITE_DIR_RAW = STORE_DIR / "raw"
WRITE_DIR_RAW.mkdir(exist_ok=True)

logging.info("Output directory: %s", STORE_DIR)

# -------------------------- API endpoints --------------------------
apiurl = "https://api.nilu.no/"
obshistoryurl = f"{apiurl}obs/historical/"
stationlookupurl = f"{apiurl}lookup/stations"

# -------------------------- HTTP client & retry --------------------
# Reuse a single Session (fewer TCP handshakes, faster).
session = requests.Session()
DEFAULT_TIMEOUT = 8.0   # seconds per HTTP request
MAX_RETRIES = 6         # retry attempts per request

def sendrequest(stationname: str, year: int,
                nattempts: int = MAX_RETRIES,
                timeoutlo: int = 4, timeouthi: int = 9) -> requests.Response:
    """
    GET historical observations for a station-year with exponential backoff + jitter.
    Returns a successful Response (status 2xx) or raises RuntimeError after MAX_RETRIES.
    """
    url = f"{obshistoryurl}{year}-01-01/{year}-12-31/{stationname}"
    last_err = None
    for attempt in range(nattempts):
        try:
            # Randomized timeout, but never below DEFAULT_TIMEOUT
            timeout = np.random.randint(timeoutlo, timeouthi)
            resp = session.get(url, timeout=max(timeout, DEFAULT_TIMEOUT))
            if resp.ok:
                return resp
            last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as err:
            last_err = err

        # Exponential backoff with small jitter (polite and robust)
        wait = min(1.8 ** attempt, 60) + random.uniform(-0.4, 0.4)
        wait = max(0.2, wait)
        logging.warning(
            "Retrying %s %d after error %s. Waiting %.1fs…",
            stationname, year, last_err, wait
        )
        time.sleep(wait)

    raise RuntimeError(f"Failed {stationname} {year} after {nattempts} attempts ({last_err})")

# -------------------------- Fetch station metadata ----------------
# Get the station table, set id as index, and parse first/last dates (UTC).
resp = session.get(stationlookupurl, timeout=DEFAULT_TIMEOUT)
resp.raise_for_status()

stationdata = pd.DataFrame(resp.json())
stationdata.set_index("id", inplace=True)
stationdata["firstMeasurment"] = pd.to_datetime(
    stationdata["firstMeasurment"], errors="coerce", utc=True
)
stationdata["lastMeasurment"] = pd.to_datetime(
    stationdata["lastMeasurment"], errors="coerce", utc=True
)

# Persist the metadata so you can inspect stations later.
stationdata.to_csv(STORE_DIR / "stations.csv")

# -------------------------- Download per-station/year -------------
# For each station and each available year, fetch & normalize values.
# Write a checkpoint file per station-year so runs can resume safely.
failures = []

for sid in stationdata.index:
    stationname = stationdata.loc[sid, "station"]  # define before logging
    logging.info("Loading data for %s (%s)", sid, stationname)
    tic = time.time()

    startyear = int(stationdata.loc[sid, "firstMeasurment"].year)
    endyear   = int(stationdata.loc[sid, "lastMeasurment"].year)

    for year in range(startyear, endyear + 1):
        # Skip if a checkpoint already exists (resume-friendly behavior).
        out_csv = WRITE_DIR_RAW / f"measurements_{sid}_{year}.csv.gz"
        if out_csv.exists():
            logging.info("  %s exists, skipping", out_csv.name)
            continue

        # Fetch this station-year block with robust retries.
        resp = sendrequest(stationname, year)
        payload = resp.json()
        if not payload:
            failures.append((sid, year))
            continue

        # Normalize all components for this station-year into one DataFrame.
        frames = []
        for cmeasure in payload:
            # Each block contains 'component' + 'values' (list of readings).
            temp = pd.DataFrame(cmeasure.get("values", []))
            if temp.empty:
                continue

            temp["component"] = cmeasure.get("component")
            temp["id"] = sid

            # Parse timestamps defensively and in UTC; keep a single 'time' column.
            if "fromTime" in temp:
                temp["time"] = pd.to_datetime(temp["fromTime"], errors="coerce", utc=True)
            elif "toTime" in temp:
                temp["time"] = pd.to_datetime(temp["toTime"], errors="coerce", utc=True)

            # Drop raw time columns if present.
            for c in ("fromTime", "toTime"):
                if c in temp.columns:
                    temp.drop(columns=[c], inplace=True)

            frames.append(temp)

        if not frames:
            failures.append((sid, year))
            continue

        year_df = pd.concat(frames, ignore_index=True)

        # Write checkpoint (compressed CSV) for this station-year.
        year_df.to_csv(out_csv, index=False, compression="gzip")
        logging.info("  wrote %s rows to %s", len(year_df), out_csv.name)

    logging.info("Time taken: %.2f min", (time.time() - tic) / 60.0)

# -------------------------- Rebuild aggregated outputs ------------
# Rebuild measurements.csv and measurements.pq from ALL checkpoint files,
# so the aggregation is complete even if many years were skipped in this run.
agg_csv = STORE_DIR / "measurements.csv"
agg_pq  = STORE_DIR / "measurements.pq"

# Start fresh each run for consistency.
if agg_csv.exists():
    agg_csv.unlink()
if agg_pq.exists():
    agg_pq.unlink()

# Collect all checkpoint files deterministically.
files = sorted(WRITE_DIR_RAW.glob("measurements_*.csv.gz"))

# Append each checkpoint into the aggregated CSV without loading everything into RAM.
CHUNK = 250_000  # adjust if you expect very large files
header_written = False
for f in files:
    # If your per-year files are modest, you can read without chunks.
    # Using chunks keeps memory steady for very large files.
    for chunk in pd.read_csv(f, chunksize=CHUNK, compression="gzip"):
        chunk.to_csv(
            agg_csv,
            index=False,
            mode="a",
            header=not header_written
        )
        header_written = True

# Build Parquet from the aggregated CSV (single read/write for simplicity).
if agg_csv.exists():
    agg_df = pd.read_csv(agg_csv)
    agg_df.to_parquet(agg_pq, engine="fastparquet", index=False)
    logging.info("Wrote aggregated CSV (%s) and Parquet (%s)", agg_csv.name, agg_pq.name)
else:
    logging.warning("No aggregated CSV was built (no checkpoint files found).")

# -------------------------- Failure log (optional) ----------------
if failures:
    fail_path = STORE_DIR / "failures.csv"
    pd.DataFrame(failures, columns=["station_id", "year"]).to_csv(fail_path, index=False)
    logging.warning("Finished with %d failures. See %s", len(failures), fail_path)
