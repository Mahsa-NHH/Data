#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
npra_client.py
==============

Purpose
-------
Reusable client utilities for downloading road traffic measurements from NPRA's
(Statens vegvesen) GraphQL API.

What this module does
---------------------
- Builds GraphQL queries for station metadata and hourly traffic volumes.
- Performs HTTP requests with a shared `requests.Session()` and robust
  retry/backoff.
- Normalizes API responses to flat rows:
  - total hourly volume
  - hourly volume by vehicle length category
- Handles file output (ensuring CSV headers) and time window chunking.

Inputs
------
- NPRA GraphQL endpoint (constant in this file)
- Station IDs and time windows (usually driven by npra_download.py)
- Output directory (Path). Default selection logic is provided.

Outputs
-------
- Writes/append to CSV files via helper functions:
  - trafficregpoints.csv
  - aggvol.csv
  - lengthvol.csv

How to use
----------
Import from `npra_download.py`:

    from npra_client import (
        VV_API, LENGTH_CATEGORIES, pick_store_dir, session_with_defaults,
        fetch_all_stations, normalize_total_row, normalize_length_rows,
        iter_100h_windows, ensure_csv_with_header, append_lines
    )

    store = pick_store_dir()
    session = session_with_defaults()
    stations_df = fetch_all_stations(session, store)   # writes trafficregpoints.csv

    # later: iterate stations and windows, call post_with_retries, normalize, append

Notes
-----
- This module does not implement a CLI; use `npra_download.py` for commands.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator, List, Dict, Any, Optional, Tuple

import logging
import random
import time
import datetime as dt

import numpy as np
import pandas as pd
import requests

# -------------------------- Constants --------------------------

# NPRA GraphQL endpoint
VV_API = "https://www.vegvesen.no/trafikkdata/api/"

# Vehicle length categories to fill when byLengthRange is missing for an hour
LENGTH_CATEGORIES: List[str] = [
    "[...,5.6)", "[5.6,...)", "[5.6,7.6)", "[7.6,12.5)",
    "[12.5,16.0)", "[16.0,24.0)", "[24.0,..)"
]

DEFAULT_TIMEOUT = 8.0   # seconds per HTTP request
MAX_RETRIES     = 6     # retry attempts per request

# -------------------------- Paths & storage --------------------------

def pick_store_dir() -> Path:
    """
    Choose a writable output directory with sensible fallbacks.
    Order:
      1) E:/traffic
      2) <home>/traffic
      3) <repo>/data/traffic
      4) <cwd>/traffic
    """
    candidates = [
        Path("E:/traffic"),
        Path.home() / "traffic",
        Path(__file__).resolve().parent / "data" / "traffic",
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    p = Path.cwd() / "traffic"
    p.mkdir(parents=True, exist_ok=True)
    return p

# -------------------------- HTTP session & retries --------------------------

def session_with_defaults() -> requests.Session:
    """
    Create a reusable HTTP session. Use this session for all requests
    to reduce handshake overhead.
    """
    s = requests.Session()
    return s

def post_with_retries(
    session: requests.Session,
    url: str,
    json: Dict[str, Any],
    *,
    max_retries: int = MAX_RETRIES,
    base_timeout: float = DEFAULT_TIMEOUT,
    timeout_lo: int = 4,
    timeout_hi: int = 9,
) -> requests.Response:
    """
    POST with exponential backoff + jitter. Raises RuntimeError after retries.
    """
    last_err: Optional[Any] = None
    for attempt in range(max_retries):
        try:
            # randomized per-attempt timeout, never below base_timeout
            t = np.random.randint(timeout_lo, timeout_hi)
            resp = session.post(url, json=json, timeout=max(base_timeout, t))
            if resp.ok:
                return resp
            last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            last_err = e

        wait = min(1.8 ** attempt, 60) + random.uniform(-0.4, 0.4)
        wait = max(0.2, wait)
        logging.warning("POST retry after error %s. Waiting %.1fs…", last_err, wait)
        time.sleep(wait)

    raise RuntimeError(f"POST failed after {max_retries} attempts: {last_err}")

# -------------------------- GraphQL queries --------------------------

def gql_all_points() -> str:
    """
    GraphQL query to fetch all traffic registration points (stations) with
    metadata including location, timespan, and latest data timestamps.
    """
    return """{
  trafficRegistrationPoints(searchQuery: {}) {
    id
    name
    trafficRegistrationType
    operationalStatus
    registrationFrequency
    dataTimeSpan {
      firstData
      firstDataWithQualityMetrics
      latestData {
        volumeByHour
        volumeByDay
      }
    }
    location {
      municipality { number }
      roadReference { shortForm }
      coordinates { latLon { lat lon } }
    }
  }
}"""

def gql_by_hour(npra_id: str, from_iso: str, to_iso: str) -> str:
    """
    GraphQL query to fetch hourly traffic between two ISO timestamps (inclusive range).
    """
    return f"""{{
  trafficData(trafficRegistrationPointId: "{npra_id}") {{
    volume {{
      byHour(from: "{from_iso}", to: "{to_iso}") {{
        edges {{
          node {{
            from
            total {{
              coverage {{ percentage }}
              volumeNumbers {{ volume }}
            }}
            byLengthRange {{
              lengthRange {{ representation }}
              total {{
                coverage {{ percentage }}
                volumeNumbers {{ volume }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}"""

# -------------------------- Station metadata --------------------------

def fetch_all_stations(session: requests.Session, store_dir: Path) -> pd.DataFrame:
    """
    Fetch all traffic registration points and write trafficregpoints.csv.
    Returns the DataFrame (indexed by integer id with column npra_id).
    """
    resp = post_with_retries(session, VV_API, json={"query": gql_all_points()})
    payload = resp.json()
    points = payload["data"]["trafficRegistrationPoints"]

    # Flatten nested fields
    rows: List[Dict[str, Any]] = []
    for d in points:
        rows.append({
            "npra_id": d["id"],
            "name": d.get("name"),
            "municipality": (d.get("location", {}).get("municipality") or {}).get("number"),
            "road_ref": (d.get("location", {}).get("roadReference") or {}).get("shortForm"),
            "lat": (d.get("location", {}).get("coordinates") or {}).get("latLon", {}).get("lat"),
            "lon": (d.get("location", {}).get("coordinates") or {}).get("latLon", {}).get("lon"),
            "firsttime": (d.get("dataTimeSpan") or {}).get("firstData"),
            "lasthour": ((d.get("dataTimeSpan") or {}).get("latestData") or {}).get("volumeByHour"),
            "lastday": ((d.get("dataTimeSpan") or {}).get("latestData") or {}).get("volumeByDay"),
            "bike": d.get("trafficRegistrationType") == "BICYCLE",
            "periodic": d.get("registrationFrequency") == "PERIODIC",
            "retired": d.get("operationalStatus") == "RETIRED",
            "tempout": d.get("operationalStatus") == "TEMPORARILY_OUT_OF_SERVICE",
        })

    df = pd.DataFrame(rows)
    # Parse times
    for col in ["firsttime", "lasthour", "lastday"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Local integer index id, keep npra_id as provided by NPRA
    df.index.name = "id"
    df.to_csv(store_dir / "trafficregpoints.csv")
    logging.info("Wrote %s", store_dir / "trafficregpoints.csv")
    return df

def load_stations(store_dir: Path) -> pd.DataFrame:
    """
    Load previously saved station metadata.
    """
    df = pd.read_csv(store_dir / "trafficregpoints.csv", index_col="id")
    for col in ["firsttime", "lasthour", "lastday"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df

# -------------------------- Normalization helpers --------------------------

def normalize_total_row(station_id: int, node: Dict[str, Any]) -> str:
    """
    Build one CSV line for total hourly volume and coverage.
    Schema: id,time,volume,coverage
    """
    time_hour = node.get("from", "")
    total = node.get("total") or {}
    volnumber = (total.get("volumeNumbers") or {}).get("volume")
    coverage = (total.get("coverage") or {}).get("percentage")
    vol = volnumber if volnumber is not None else ""
    cov = (coverage / 100) if coverage is not None else ""
    return f"{station_id},{time_hour},{vol},{cov}\n"

def normalize_length_rows(station_id: int, node: Dict[str, Any]) -> List[str]:
    """
    Build CSV lines for hourly volume by vehicle length category.
    Schema: id,time,length,volume,coverage
    If API returns no byLengthRange for the hour, fill with empty rows.
    """
    rows: List[str] = []
    time_hour = node.get("from", "")
    lnode = node.get("byLengthRange") or []
    if not lnode:
        for lc in LENGTH_CATEGORIES:
            rows.append(f"{station_id},{time_hour},{lc},,\n")
        return rows

    for lr in lnode:
        lc = (lr.get("lengthRange") or {}).get("representation")
        total = lr.get("total") or {}
        volnumber = (total.get("volumeNumbers") or {}).get("volume")
        coverage = (total.get("coverage") or {}).get("percentage")
        vol = volnumber if volnumber is not None else ""
        cov = (coverage / 100) if coverage is not None else ""
        rows.append(f"{station_id},{time_hour},{lc},{vol},{cov}\n")
    return rows

# -------------------------- Windows & time helpers --------------------------

def iter_100h_windows(start: pd.Timestamp, end: pd.Timestamp) -> Iterator[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Yield half-open windows [from, to) stepping 100 hours each time.
    """
    fromtime = start
    hour100 = pd.offsets.Hour(100)
    while fromtime < end:
        totime = fromtime + hour100
        yield fromtime, totime
        fromtime = totime

def to_iso_plus0100(ts: pd.Timestamp) -> str:
    """
    Format timestamp as ISO with +01:00 offset (to match original scripts).
    """
    return ts.strftime("%Y-%m-%dT%H:00:00+01:00")

# -------------------------- CSV helpers --------------------------

def ensure_csv_with_header(path: Path, header: str) -> None:
    """
    Ensure a CSV file exists with a header line; create it if missing.
    """
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(header, encoding="utf-8")

def append_lines(path: Path, lines: Iterable[str]) -> None:
    """
    Append many pre-built lines to a CSV file efficiently.
    """
    with path.open("a", encoding="utf-8", newline="") as f:
        for line in lines:
            f.write(line)
