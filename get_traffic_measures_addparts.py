#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 10:46:55 2021

Retrieve road traffic measurements from NPRA's API

@author: morten
"""

#%% SETUP
import requests
import pandas as pd
import numpy as np
import pytz
import datetime
import time

storefolder = 'E:\\traffic\\'

vvapi = 'https://www.vegvesen.no/trafikkdata/api/'

data = pd.read_csv(f'{storefolder}trafficregpoints.csv', index_col='id')

for col in ['firsttime', 'lasthour', 'lastday']:
    data[col] = pd.to_datetime(data[col])

#%% FUNCTIONS

def makequerystring(npra_id, fromtime, totime):
    strfromtime = fromtime.strftime('%Y-%m-%dT%H:00:00+01:00')
    strtotime = totime.strftime('%Y-%m-%dT%H:00:00+01:00')
    
    querystring = """{
      trafficData(trafficRegistrationPointId: """
    querystring += f'"{npra_id}"'
    querystring += """) {
        volume {
          byHour("""
    querystring += f'from: "{strfromtime}", to: "{strtotime}"'
    querystring += """) {
            edges {
              node {
                from
                total {
                    coverage {
                        percentage
                        }
                    volumeNumbers {
                        volume
                        }
                    }
                byLengthRange {
                    lengthRange {
                        representation
                        }
                    total {
                        coverage {
                            percentage
                            }
                        volumeNumbers {
                            volume
                            }
                        }
                    }
              }
            }
          }
        }
      }
    }"""
    return querystring

def sendrequest(npra_id, fromtime, totime,
                nattempts=20, timeoutlo=4, timeouthi=9, sleepfactor=5):
    querystring = makequerystring(npra_id, fromtime, totime)
    for attempt in range(nattempts):
        try:
            resp = requests.post(vvapi,
                                 timeout=np.random.randint(timeoutlo,timeouthi),
                                 json={'query': querystring}
                                 )
            assert resp.status_code == 200
            assert resp.json()['data']['trafficData']['volume']['byHour']['edges'] is not None
        except Exception as err:
            print(f"Error {err}, taking 5 x attempts and retrying")
            time.sleep(sleepfactor * attempt)
        else:
            break
    return resp

def makeaggline(mpid, node):
    time_hour = node['from']
    volnumber = node['total']['volumeNumbers']
    volnumber = volnumber['volume'] if volnumber is not None else ''
    coverage = node['total']['coverage']
    coverage = coverage['percentage'] / 100 if coverage is not None else ''
    line = f'{mpid},{time_hour},{volnumber},{coverage}\n'
    return line

def makelengthlines(mpid, node):
    lrows = []
    time_hour = node['from']
    lnode = node['byLengthRange']
    if len(lnode) == 0:
        for lc in lengthcats:
            lrows.append(f'{mpid},{time_hour},{lc},,\n')
    else:
        for lr in lnode:
            lc = lr['lengthRange']['representation']
            volnumber = lr['total']['volumeNumbers']
            volnumber = volnumber['volume'] if volnumber is not None else ''
            coverage = lr['total']['coverage']
            coverage = coverage['percentage'] / 100 if coverage is not None else ''
            
            lrows.append(f'{mpid},{time_hour},{lc},{volnumber},{coverage}\n')
    return lrows


#%% SETUP FOR LOADING TRAFFIC MEASURES
# Get the current hour as the latest possible measurement
currenttime = datetime.datetime.now(tz=pytz.timezone('CET'))

# Retrieving traffic by length of vehicle
# List of categories to fill in hours where measures are missing
lengthcats = ['[...,5.6)', '[5.6,...)', '[5.6,7.6)', '[7.6,12.5)',
              '[12.5,16.0)', '[16.0,24.0)', '[24.0,..)']

# Initialize files for data on total volumes and volumes by length
aggfile = open(f'{storefolder}aggvol.csv', 'a')
lengthfile = open(f'{storefolder}lengthvol.csv', 'a')

#%% RETRIEVE MEASURES OVER SETS OF STATIONS

idlist = data[data.firsttime.notnull()].index.values
tic = time.time()
# Actually left off finishing index position 1844, now jumped to 1871
# lacking positions 1845--1870
for mpid in idlist[4088:]:
    npra_id = data.loc[mpid, 'npra_id']
    
    firsttime = data.loc[mpid, 'firsttime']
    lasttime = data.loc[mpid, 'lasthour']
    # Last hour variable is missing if station still operational
    if pd.isnull(lasttime):
        lasttime = currenttime
    
    # Can only retrieve 100 records at a time
    fromtime = firsttime
    totime = firsttime + pd.offsets.Hour(100)
    
    # Loop over spans of 100 hours until full period retrieved
    while fromtime < lasttime:
        # Just to look at the progress
        print(f'{npra_id}: {fromtime}--{totime}')
        
        resp = sendrequest(npra_id, fromtime, totime)
        
        for edge in resp.json()['data']['trafficData']['volume']['byHour']['edges']:
            aggline = makeaggline(mpid, edge['node'])
            lengthlines = makelengthlines(mpid, edge['node'])
            
            aggfile.write(aggline)
            lengthfile.writelines(lengthlines)
            
        fromtime = totime
        totime = totime + pd.offsets.Hour(100)
toc = time.time()

aggfile.close()
lengthfile.close()
print(f'Took {np.round((toc - tic) / 60, 0)} minutes')
