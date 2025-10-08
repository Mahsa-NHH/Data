#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 14:58:01 2022

@author: morten

Get air quality measures from api.nilu.no
"""
"""
SSB Municipality Data Pipeline

Purpose
-------
Fetch municipal population (07459) and household income (06944), map all municipality
codes to a fixed anchor date (default 2020-01-01), and export clean CSVs.

Outputs
-------
- centrality2020.csv         # munid (2020), centrality (KLASS 128)
- muni2020_codes.csv         # code, name (valid on 2020-01-01)
- population_muni_year_age.csv
  - year:int, munid:int (anchor), age:int, population:int
- income_muni_year.csv
  - year:int, munid:int (anchor), nhouseholds:float, income_posttax:float, income:float

Notes
-----
- Code mapping uses KLASS 131 changes. Forward to anchor date, then (optionally) back-map
  post-anchor changes so everything stays aligned to the anchor geography.
"""

from pathlib import Path
import requests
import numpy as np
import pandas as pd
import time
import random   # NEW

# ---------- config (same as step 1) ----------
STORE_DIR = Path('E:/airquality/')
STORE_DIR.mkdir(parents=True, exist_ok=True)

apiurl = 'https://api.nilu.no/'
obshistoryurl = f'{apiurl}obs/historical/'
stationlookupurl = f'{apiurl}lookup/stations'

# ---------- NEW: one reusable HTTP session ----------
session = requests.Session()  # NEW
DEFAULT_TIMEOUT = 8.0         # NEW
MAX_RETRIES = 6               # NEW

# ---------- stations unchanged except using session ----------
resp = session.get(stationlookupurl, timeout=DEFAULT_TIMEOUT)  # CHANGED
stationdata = pd.DataFrame(resp.json())
stationdata.set_index('id', inplace=True)
stationdata['firstMeasurment'] = pd.to_datetime(stationdata.firstMeasurment)
stationdata['lastMeasurment'] = pd.to_datetime(stationdata.lastMeasurment)

stationdata.to_csv(STORE_DIR / 'stations.csv')

#componentlist = ['CO', 'NO', 'NO2', 'NOx', 'O3', 'PM1', 'PM2.5', 'PM10', 'SO2']
#stationcomponents = stationdata.components.apply(lambda x: pd.Series([z in x for z in componentlist], index=componentlist))

# ---------- CHANGED: safer retry with exponential backoff ----------
def sendrequest(stationname, year,
                nattempts=MAX_RETRIES, timeoutlo=4, timeouthi=9, sleepfactor=5):
    querystring = f'{obshistoryurl}{year}-01-01/{year}-12-31/{stationname}'
    last_err = None  # NEW
    for attempt in range(nattempts):
        try:
            # use a consistent default timeout, still allow small random int if you like
            timeout = np.random.randint(timeoutlo, timeouthi)
            resp = session.get(querystring, timeout=max(timeout, DEFAULT_TIMEOUT))  # CHANGED
            if resp.ok:  # CHANGED: no bare assert; check properly
                return resp
            else:
                last_err = f"HTTP {resp.status_code}"
        except requests.RequestException as err:  # CHANGED: catch network errors only
            last_err = err
        # CHANGED: exponential backoff with a bit of jitter
        wait = min(1.8 ** attempt, 60) + random.uniform(-0.4, 0.4)
        wait = max(0.2, wait)
        print(f"Retrying {stationname} {year} after error {last_err!s}. Waiting {wait:.1f}s…")
        time.sleep(wait)
    # if we exit loop, return the last response or raise
    raise RuntimeError(f"Failed {stationname} {year} after {nattempts} attempts ({last_err})")

failure = []
measuredata = pd.DataFrame()
for sid in stationdata.index:
    print("Loading data for ", sid)
    tic = time.time()
    stationname = stationdata.loc[sid, 'station']
    startyear = stationdata.loc[sid, 'firstMeasurment'].year
    endyear = stationdata.loc[sid, 'lastMeasurment'].year
    
    for year in range(startyear, endyear + 1):
        resp = sendrequest(stationname, year)
        payload = resp.json()  # NEW: parse once
        if len(payload) > 0:
            for cmeasure in payload:
                temp = pd.DataFrame(cmeasure['values'])
                temp['component'] = cmeasure['component']
                temp['id'] = sid
                temp['time'] = pd.to_datetime(temp.fromTime)
                temp.drop(['fromTime', 'toTime'], axis=1, inplace=True)
                measuredata = pd.concat((measuredata, temp), ignore_index=True)
        else:
            failure.append((sid, year))
    print("Time taken: ", (time.time() - tic) / 60)

measuredata.to_csv(STORE_DIR / 'measurements.csv', index=False)
measuredata.to_parquet(STORE_DIR / 'measurements.pq', engine='fastparquet', index=False)
