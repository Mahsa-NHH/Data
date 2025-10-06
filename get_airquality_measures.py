#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 20 14:58:01 2022

@author: morten

Get air quality measures from api.nilu.no
"""

import requests
import numpy as np
import pandas as pd
import time

storefolder = 'E:/airquality/'

apiurl = 'https://api.nilu.no/'
obshistoryurl = f'{apiurl}obs/historical/'
stationlookupurl = f'{apiurl}lookup/stations'

resp = requests.get(stationlookupurl)
stationdata = pd.DataFrame(resp.json())
stationdata.set_index('id', inplace=True)
stationdata['firstMeasurment'] = pd.to_datetime(stationdata.firstMeasurment)
stationdata['lastMeasurment'] = pd.to_datetime(stationdata.lastMeasurment)

stationdata.to_csv(f'{storefolder}stations.csv')

#componentlist = ['CO', 'NO', 'NO2', 'NOx', 'O3', 'PM1', 'PM2.5', 'PM10', 'SO2']
#stationcomponents = stationdata.components.apply(lambda x: pd.Series([z in x for z in componentlist], index=componentlist))

def sendrequest(stationname, year,
                nattempts=20, timeoutlo=4, timeouthi=9, sleepfactor=5):
    querystring = f'{obshistoryurl}{year}-01-01/{year}-12-31/{stationname}'
    for attempt in range(nattempts):
        try:
            resp = requests.get(querystring, timeout=np.random.randint(timeoutlo,timeouthi))
            assert resp.status_code == 200
        except Exception as err:
            print(f"Error {err}, taking 5 x attempts and retrying")
            time.sleep(sleepfactor * attempt)
        else:
            break
    return resp

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
        if len(resp.json()) > 0:
            for cmeasure in resp.json():
                temp = pd.DataFrame(cmeasure['values'])
                temp['component'] = cmeasure['component']
                temp['id'] = sid
                temp['time'] = pd.to_datetime(temp.fromTime)
                temp.drop(['fromTime', 'toTime'], axis=1, inplace=True)
                measuredata = pd.concat((measuredata, temp), ignore_index=True)
        else:
            failure.append((sid, year))
    print("Time taken: ", (time.time() - tic) / 60)

measuredata.to_csv(f'{storefolder}measurements.csv', index=False)
# Store as parquet
measuredata.to_parquet(f'{storefolder}measurements.pq', engine='fastparquet', index=False)