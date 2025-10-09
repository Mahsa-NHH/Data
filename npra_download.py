#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
npra_download.py
================

Purpose
-------
Command-line interface for downloading NPRA traffic data using helpers from
`npra_client.py`. Keeps the same outputs you've been using:

Outputs (written to STORE_DIR)
------------------------------
- trafficregpoints.csv
- aggvol.csv
- lengthvol.csv

Commands
--------
1) Fetch stations metadata:
    python npra_download.py --fetch-stations

2) Download all data for all stations (100-hour windows; append to CSVs):
    python npra_download.py --download-all

3) Resume from a given station index (skip earlier stations):
    python npra_download.py --resume --start-index 4088

Arguments
---------
--store-dir PATH     Set output directory (default: smart fallbacks)
--resume            Resume mode (use with --start-index)
--start-index INT   First station index to process in resume mode

Notes
-----
- We write headers for aggvol.csv and lengthvol.csv if they don't exist.
- We keep the original behavior: CET-based "now" fallback for latest hour,
  GraphQL API max 100-hour windows, and coverage as fraction (percentage/100).
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd
import pytz
import datetime as dt

from npra_client import (
    VV_API, LENGTH_CATEGORIES, pick_store_dir, session_with_defaults,
    fetch_all_stations, load_stations, post_with_retries, gql_by_hour,
    normalize_total_row, normalize_length_rows, iter_100h_windows,
    to_iso_plus0100, ensure_csv_with_header, append_lines
)

# -------------------------- Logging --------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# -------------------------- CLI --------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download NPRA traffic data (stations, hourly volumes, by length).")
    p.add_argument("--store-dir", type=Path, default=None,
                   help="Output directory. Defaults to E:/traffic, <home>/traffic, <repo>/data/traffic, or <cwd>/traffic.")
    p.add_argument("--fetch-stations", action="store_true",
                   help="Fetch station metadata and write trafficregpoints.csv.")
    p.add_argument("--download-all", action="store_true",
                   help="Download hourly data for all stations, appending to aggvol.csv and lengthvol.csv.")
    p.add_argument("--resume", action="store_true",
                   help="Resume mode (skips stations < --start-index).")
    p.add_argument("--start-index", type=int, default=0,
                   help="First station index to process when --resume is set (e.g., 4088).")
    return p.parse_args()

# -------------------------- Main tasks --------------------------

def task_fetch_stations(store_dir: Path) -> None:
    session = session_with_defaults()
    fetch_all_stations(session, store_dir)

def task_download_all(store_dir: Path, resume: bool, start_index: int) -> None:
    """
    Read trafficregpoints.csv, then for each station:
      - find [firsttime, lasthour or now]
      - iterate in 100-hour windows
      - request hourly data
      - append to aggvol.csv and lengthvol.csv (create headers if missing)
    """
    df = load_stations(store_dir)

    # Ensure output CSVs have headers (append mode will be used)
    agg_path = store_dir / "aggvol.csv"
    len_path = store_dir / "lengthvol.csv"
    ensure_csv_with_header(agg_path, "id,time,volume,coverage\n")
    ensure_csv_with_header(len_path, "id,time,length,volume,coverage\n")

    # Build the station processing list (like your idlist approach)
    idlist = df[df["firsttime"].notnull()].index.tolist()
    if resume:
        idlist = idlist[start_index:]

    session = session_with_defaults()
    tz_cet = pytz.timezone("CET")
    currenttime = dt.datetime.now(tz=tz_cet)

    tic = time.time()
    for mpid in idlist:
        npra_id = df.loc[mpid, "npra_id"]
        firsttime = df.loc[mpid, "firsttime"]
        lasthour = df.loc[mpid, "lasthour"]

        # If station is still operating (lasthour missing), we consider now
        lasttime = lasthour if pd.notnull(lasthour) else currenttime

        logging.info("Station %s (%s): %s → %s", mpid, npra_id, firsttime, lasttime)

        for fromtime, totime in iter_100h_windows(firsttime, lasttime):
            logging.info("  Window: %s -- %s", fromtime, totime)

            # Build and send GraphQL for this window
            query = gql_by_hour(npra_id, to_iso_plus0100(fromtime), to_iso_plus0100(totime))
            resp = post_with_retries(session, VV_API, json={"query": query})

            # Read edges safely
            payload = resp.json()
            edges = (((payload.get("data") or {}).get("trafficData") or {}).get("volume") or {}).get("byHour", {}).get("edges")
            if not edges:
                continue

            # Normalize rows and append
            agg_lines = []
            len_lines = []
            for edge in edges:
                node = edge.get("node") or {}
                agg_lines.append(normalize_total_row(mpid, node))
                len_lines.extend(normalize_length_rows(mpid, node))

            append_lines(agg_path, agg_lines)
            append_lines(len_path, len_lines)

    logging.info("Download finished in %.2f minutes", (time.time() - tic) / 60.0)

# -------------------------- Entrypoint --------------------------

def main() -> None:
    args = parse_args()
    store_dir = args.store_dir or pick_store_dir()
    store_dir.mkdir(parents=True, exist_ok=True)
    logging.info("Output directory: %s", store_dir)

    if args.fetch_stations:
        task_fetch_stations(store_dir)

    if args.download_all or args.resume:
        task_download_all(store_dir, resume=args.resume, start_index=args.start_index)

    if not (args.fetch_stations or args.download_all or args.resume):
        # Default help if no action provided
        logging.info("No action specified. Try one of: --fetch-stations, --download-all, --resume --start-index N")

if __name__ == "__main__":
    main()
