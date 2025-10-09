#!/usr/bin/env python3
# -*- coding: utf-8 -*-

r"""
get_ssb_cpi.py
==============

Purpose
-------
Download CPI (2015=100) from SSB:
- Monthly CPI 1920–2024 (table 08981)
- Yearly  CPI 1865–2024 (table 08184)

Outputs (same filenames, written to robust STORE_DIR)
-----------------------------------------------------
- cpi_monthly_1920_2024.csv   (date: YYYY-MM-01, cpi: float)
- cpi_yearly_1920_2024.csv    (year: int, cpi: float)

Improvements
------------
- HTTPS, single Session, timeouts, retries with backoff.
- Logging progress & row counts.
- Cleans placeholders before casting to numeric.
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
SSB = "https://data.ssb.no/api/v0/no/table/"
CPI_MONTH_URL = f"{SSB}08981/"
CPI_YEAR_URL  = f"{SSB}08184/"

session = session_with_defaults()

# -------------------------- monthly CPI ----------------------
cpi_query = {
    "query": [
        {"code": "Maaned", "selection": {"filter": "item", "values": [f"{i:02d}" for i in range(1, 13)]}},
        {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}},
    ],
    "response": {"format": "csv3"},
}
text = session_post_text(session, CPI_MONTH_URL, json=cpi_query)
cpi = pd.read_csv(StringIO(text))

cpi = cpi.rename(columns={"Maaned": "month", "Tid": "year", "08981": "cpi"})
cpi = cpi.drop(columns=["ContentsCode"], errors="ignore")
# build date, clean numeric
cpi["date"] = pd.to_datetime(cpi["year"].astype(str) + "-" + cpi["month"].astype(str) + "-01", errors="coerce")
cpi["cpi"] = cpi["cpi"].replace(".", np.nan).astype(float)
cpi = cpi.sort_values("date")[["date", "cpi"]]

out_m = STORE_DIR / "cpi_monthly_1920_2024.csv"
cpi.to_csv(out_m, index=False)
logging.info("Wrote monthly CPI: %s rows -> %s", len(cpi), out_m.name)

# -------------------------- yearly CPI -----------------------
cpi_year_query = {
    "query": [{"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}],
    "response": {"format": "csv3"},
}
text_y = session_post_text(session, CPI_YEAR_URL, json=cpi_year_query)
cpi_y = pd.read_csv(StringIO(text_y))
cpi_y = cpi_y.rename(columns={"Tid": "year", "08184": "cpi"})
cpi_y = cpi_y.drop(columns=["ContentsCode"], errors="ignore")

out_y = STORE_DIR / "cpi_yearly_1920_2024.csv"
cpi_y[["year", "cpi"]].to_csv(out_y, index=False)
logging.info("Wrote yearly CPI: %s rows -> %s", len(cpi_y), out_y.name)
