#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
get_ssb_data_api.py
===================

Purpose
-------
1) Quarterly GDP (total & mainland) from SSB table 09190, constant prices, seasonally adjusted.
2) Quarterly population constructed by:
   - Yearly population (06913) placed at Q4 of previous year,
   - Linear interpolation to quarters up to 1997Q4,
   - Spliced with true quarterly population (01222) from 1997Q4 onward.
3) Merge to a single quarterly dataset.

Output (same filename)
----------------------
- gdp_population.csv   (time: Period[Q], gdp, gdp_mainland, population)

Improvements
------------
- HTTPS, single Session, timeouts, retries with backoff.
- Logging progress & row counts.
- Clear comments on the splice and the 1997Q4 overwrite you already do.
"""

from pathlib import Path
from io import StringIO
import logging
import random
import time
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd
import requests

# -------------------------- logging --------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------------- paths ----------------------------
def pick_store_dir() -> Path:
    candidates = [
        Path(r"E:/utility"),
        Path.home() / "utility",
        Path(__file__).resolve().parent / "data" / "utility",
        Path.cwd() / "utility",
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    p = Path.cwd() / "utility"
    p.mkdir(parents=True, exist_ok=True)
    return p

STORE_DIR = pick_store_dir()
logging.info("Output directory: %s", STORE_DIR)

# -------------------------- HTTP helpers ---------------------
DEFAULT_TIMEOUT = 15.0
MAX_RETRIES = 6

def session_with_defaults() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "SSB-Downloader/1.0 (+contact: your.email@example.com)"})
    return s

def _sleep_backoff(attempt: int) -> None:
    wait = min(1.8 ** attempt, 60) + random.uniform(-0.4, 0.4)
    time.sleep(max(0.2, wait))

def session_post_text(session: requests.Session, url: str, json: Dict[str, Any]) -> str:
    last_err: Optional[Any] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(url, json=json, timeout=DEFAULT_TIMEOUT)
            if resp.status_code == 429 and "Retry-After" in resp.headers:
                ra = float(resp.headers.get("Retry-After", "1"))
                logging.warning("429 received. Respecting Retry-After=%.1fs", ra)
                time.sleep(ra)
                continue
            if resp.ok:
                return resp.text
            last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as e:
            last_err = e
        logging.warning("POST retry %s (attempt %d).", last_err, attempt + 1)
        _sleep_backoff(attempt)
    raise RuntimeError(f"POST failed for {url}: {last_err}")

# -------------------------- endpoints ------------------------
BASE = "https://data.ssb.no/api/v0/no/table/"
GDP_URL   = f"{BASE}09190"
QPOP_URL  = f"{BASE}01222"
YPOP_URL  = f"{BASE}06913"

session = session_with_defaults()

# -------------------------- queries --------------------------
gdp_query = {
    "query": [
        {"code": "Makrost", "selection": {"filter": "item", "values": ["bnpb.nr23_9", "bnpb.nr23_9fn"]}},
        {"code": "ContentsCode", "selection": {"filter": "item", "values": ["FastePriserSesJust"]}},
    ],
    "response": {"format": "csv3"},
}

qpop_query = {
    "query": [
        {"code": "Region", "selection": {"filter": "vs:Landet", "values": ["0"]}},
        {"code": "ContentsCode", "selection": {"filter": "item", "values": ["Folketallet11"]}},
    ],
    "response": {"format": "csv3"},
}

ypop_query = {
    "query": [
        {"code": "Region", "selection": {"filter": "vs:Landet", "values": ["0"]}},
        {"code": "ContentsCode", "selection": {"filter": "item", "values": ["Folkemengde"]}},
    ],
    "response": {"format": "csv3"},
}

# -------------------------- fetch & tidy ----------------------
# GDP
txt_gdp = session_post_text(session, GDP_URL, json=gdp_query)
gdp = pd.read_csv(StringIO(txt_gdp))
gdp = gdp.rename(columns={"Tid": "time", "09190": "gdp", "Makrost": "gdp_type"})
gdp = gdp.drop(columns=["ContentsCode"], errors="ignore")
gdp["time"] = pd.PeriodIndex(gdp["time"].str.replace("K", "Q"), freq="Q")
gdp = gdp.pivot(index="time", columns="gdp_type", values="gdp").rename(
    columns={"bnpb.nr23_9": "gdp", "bnpb.nr23_9fn": "gdp_mainland"}
)

# Quarterly population (true quarterly)
txt_qpop = session_post_text(session, QPOP_URL, json=qpop_query)
qpop = pd.read_csv(StringIO(txt_qpop)).rename(columns={"Tid": "time", "01222": "population"})
qpop = qpop.drop(columns=["ContentsCode", "Region"], errors="ignore")
qpop["time"] = pd.PeriodIndex(qpop["time"].str.replace("K", "Q"), freq="Q")

# Yearly population (place at end of previous year, interpolate to quarters)
txt_ypop = session_post_text(session, YPOP_URL, json=ypop_query)
ypop = pd.read_csv(StringIO(txt_ypop)).rename(columns={"Tid": "year", "06913": "population"})
ypop = ypop.drop(columns=["ContentsCode", "Region"], errors="ignore")
ypop["time"] = pd.PeriodIndex((ypop["year"] - 1).astype(str) + "Q4", freq="Q")
ypop = ypop.set_index("time").sort_index().drop(columns=["year"])

# Overwrite 1997Q4 with value from qpop (documented splice decision)
try:
    ypop.loc["1997Q4", "population"] = qpop[qpop["time"] == pd.Period("1997Q4")]["population"].values[0]
except Exception:
    logging.warning("Could not overwrite 1997Q4 from qpop; proceeding with interpolation value.")

# Interpolate quarters over the historical range up to 1997Q4
quarterly_index = pd.period_range(start="1977Q4", end="1997Q4", freq="Q")
interp = ypop.reindex(quarterly_index).interpolate(method="linear")
interp = interp.reset_index().rename(columns={"index": "time"})

# Combine interpolated historical (excluding 1998Q1 onward) with true quarterly series
full_qpop = pd.concat([interp.iloc[:-1], qpop], ignore_index=True).sort_values("time").reset_index(drop=True)

# Merge GDP + population
out = gdp.merge(full_qpop, on="time", how="inner").reset_index()

out_path = STORE_DIR / "gdp_population.csv"
out.to_csv(out_path, index=False)
logging.info("Wrote %s rows -> %s", len(out), out_path.name)
