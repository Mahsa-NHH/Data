"""
author: morten
date: 2024-10-13

Get data from SSB API (Statistics Norway) and save it to a local file

1. Get Quarterly GDP and population
    - GDP in table 09190
    - Population in table 01222

2. municipality codes and changes over time from klass (131)
    - population per municipality in Norway from befolkning in table 07459
"""

import requests
import pandas as pd
import numpy as np
from io import StringIO

# Base URL for SSB API
baseurl = "http://data.ssb.no/api/"

####################################################
# 1. Quarterly GDP and population
####################################################
# URL for GDP table 09190
gdpurl = f'{baseurl}v0/no/table/09190'
# URL for population table 01222
qpopurl = f'{baseurl}v0/no/table/01222'
# URL for population table 06913
ypopurl = f'{baseurl}v0/no/table/06913'
# Query for GDP table 09190
gdpquery = {
  "query": [
    {
      "code": "Makrost",
      "selection": {
        "filter": "item",
        "values": [
          "bnpb.nr23_9",
          "bnpb.nr23_9fn"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "FastePriserSesJust"
        ]
      }
    }
  ],
  "response": {
    "format": "csv3"
  }
}

# Query for population table 01222
qpopquery = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Folketallet11"
        ]
      }
    }
  ],
  "response": {
    "format": "csv3"
  }
}

# Also get yearly population from table 06913
ypopquery = {
  "query": [
    {
      "code": "Region",
      "selection": {
        "filter": "vs:Landet",
        "values": [
          "0"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Folkemengde"
        ]
      }
    }
  ],
  "response": {
    "format": "csv3"
  }
}

# Get GDP data
gdp_response = requests.post(gdpurl, json=gdpquery)
gdp_data = pd.read_csv(StringIO(gdp_response.text))
# Name of variable in "Makrost", bnpb.nr23_9 is "GDP" and bnpb.nr23_9fn is "Mainland GDP" - want to have these in columns
# time in "Tid" in format YYYYKQ (e.g., 1990K2)
# values in "09190"
gdp_data.rename(columns={'Tid': 'time', '09190': 'gdp', 'Makrost': 'gdp_type'}, inplace=True)
# Drop column "ContentsCode"
gdp_data.drop(['ContentsCode'], axis=1, inplace=True)
# Convert time to quarterly date format
gdp_data["time"] = pd.PeriodIndex(gdp_data["time"].str.replace("K", "Q"), freq="Q")
# Pivot data to have gdp_type as columns
gdp_data = gdp_data.pivot(index='time', columns='gdp_type', values='gdp')
# Rename columns to "gdp" and "gdp_mainland"
gdp_data.rename(columns={'bnpb.nr23_9': 'gdp', 'bnpb.nr23_9fn': 'gdp_mainland'}, inplace=True)

# Get population data
qpop_response = requests.post(qpopurl, json=qpopquery)
qpop_data = pd.read_csv(StringIO(qpop_response.text))
# Rename columns to "time" and "population"
qpop_data.rename(columns={'Tid': 'time', '01222': 'population'}, inplace=True)
# Drop column "ContentsCode" and "Region"
qpop_data.drop(['ContentsCode', 'Region'], axis=1, inplace=True)
# Convert time to quarterly date format
qpop_data["time"] = pd.PeriodIndex(qpop_data["time"].str.replace("K", "Q"), freq="Q")

# Yearly
ypop_response = requests.post(ypopurl, json=ypopquery)
ypop_data = pd.read_csv(StringIO(ypop_response.text))
# Rename columns to "year" and "population"
ypop_data.rename(columns={'Tid': 'year', '06913': 'population'}, inplace=True)
# Drop column "ContentsCode" and "Region"
ypop_data.drop(['ContentsCode', 'Region'], axis=1, inplace=True)

ypop_data["time"] = pd.PeriodIndex((ypop_data["year"] - 1).astype(str) + 'Q4', freq="Q")
# This now marks population as of end-of-Q4 of the previous year

# Drop "year", set time as index and reindex quarterly
ypop_data.drop(['year'], axis=1, inplace=True)
ypop_data = ypop_data.set_index("time").sort_index()

# Replace 1997Q4 value with the corresponding value from qpop_data
ypop_data.loc['1997Q4', 'population'] = qpop_data[qpop_data.time == '1997Q4'].population.values[0]

# Create quarterly date range from 1990Q1 to 1997Q3 (inclusive)
quarterly_index = pd.period_range(start='1977Q4', end='1997Q4', freq='Q')

# Reindex and interpolate
interp_data = ypop_data.reindex(quarterly_index).interpolate(method="linear")

# Reset index and rename columns
interp_data = interp_data.reset_index().rename(columns={"index": "time"})


# Combine
full_qpop = pd.concat([interp_data.iloc[:-1], qpop_data], ignore_index=True).sort_values("time").reset_index(drop=True)


# Merge GDP and population data on time
gdp_data = gdp_data.merge(full_qpop, on='time', how='inner')

# Save to CSV
gdp_data.to_csv('E:/utility/gdp_population.csv', index=False)




###################################################
# 2. Municipality codes and changes over time
###################################################

# Statistics Norway URL for municipality classification
classurl = f'{baseurl}klass/v1/classifications/131/'

# URL for population table 07459
popurl = f'{baseurl}v0/no/table/07459'

# Population aggregated across changes only available for 2024 codes
# Get codesAt.csv with params date=2024-01-01 and put in a DataFrame
rmuni2024 = requests.get(f'{classurl}codesAt.csv',
                         params={'date': '2024-01-01'})
muni2024 = pd.read_csv(StringIO(rmuni2024.text))
# Get codesAt.csv with params date=2020-01-01 and put in a DataFrame
rmuni2020 = requests.get(f'{classurl}codesAt.csv',
                            params={'date': '2020-01-01'})
muni2020 = pd.read_csv(StringIO(rmuni2020.text))

# Query for population table 07459
# Get codes for all municipalities except 9999 (missing)
muniaggcodes = [f'K-{x:04}' for x in muni2024.code.values if x != 9999]
# Create query for population table
popquery = {
    "query": [
        {
            'code': 'Region',
            'selection': {
                'filter': 'agg:KommSummer',
                'values': muniaggcodes
            }
        },
        {
            'code': 'Tid',
            'selection': {
                'filter': 'all',
                'values': ['*']
            }
        }
        ],
    "response": {
        "format": "csv3"
    }
}

# Get population data
popr = requests.post(popurl, json=popquery)

popdata = pd.read_csv(StringIO(popr.text))
popdata['munid'] = popdata.Region.str.extract(r'K-(\d{4})').astype(int)
popdata.rename(columns={'07459': 'population', 'Tid': 'year'}, inplace=True)
popdata.drop(['Region', 'ContentsCode'], axis=1, inplace=True)



# Get total population from table 07459 by eliminating Region
totpopquery = {
    "query": [
        {
            'code': 'Tid',
            'selection': {
                'filter': 'all',
                'values': ['*']
            }
        }
        ],
    "response": {
        "format": "csv3"
    }
}

totpopr = requests.post(popurl, json=totpopquery)
totpopdata = pd.read_csv(StringIO(totpopr.text))
totpopdata.rename(columns={'07459': 'population', 'Tid': 'year'}, inplace=True)


# Population for all municipalities code: Region, filter = 'item' and values = ['*'], and for all years
popqueryall = {
    "query": [
        {
            'code': 'Region',
            'selection': {
                'filter': 'all',
                'values': ['*']
            }
        },
        {
            'code': 'Tid',
            'selection': {
                'filter': 'all',
                'values': ['*']
            }
        }
        ],
    "response": {
        "format": "csv3"
    }
}

poprall = requests.post(popurl, json=popqueryall)
popdataall = pd.read_csv(StringIO(poprall.text))
popdataall.drop(popdataall[popdataall.Region.str.len() != 4].index, inplace=True)
popdataall['munid'] = popdataall.Region.astype(int)
popdataall.rename(columns={'07459': 'population', 'Tid': 'year'}, inplace=True)
# Drop observations with zero population, use drop
popdataall.drop(popdataall[popdataall.population == 0].index, inplace=True)

# Indicator for munid being in the set of municipalities in 2024
popdataall['in2024'] = popdataall.munid.isin(muni2024.code.values)
# Indicator for munid being in the set of municipalities in 2020
popdataall['in2020'] = popdataall.munid.isin(muni2020.code.values)

# Get changes in municipality codes from 2020-01-01 to 2024-01-01
# Use changes.csv with params from=2020-01-01
rchanges = requests.get(f'{classurl}changes.csv',
                        params={'from': '1986-01-01'})

munichanges = pd.read_csv(StringIO(rchanges.text))
# Rename oldCode: munid_from, newCode: munid_to, changeOccured to date (and date format)
munichanges.rename(columns={'oldCode': 'munid_from',
                            'newCode': 'munid_to',
                            'changeOccurred': 'date'}, inplace=True)
munichanges['date'] = pd.to_datetime(munichanges['date'])

# Show number and type of changes for each date
munichanges.groupby(['date', 'munid_from']).munid_to.nunique().groupby('date').value_counts().unstack()

# check one to three change in 2020 with munid=5012
# Get all of the municipalities that were part of the change
initlen = 0
t5012 = [5012]
diff = len(t5012) - initlen
while diff > 0:
    initlen = len(t5012)
    t5012 = np.unique(
        munichanges[munichanges.munid_from.isin(t5012) | munichanges.munid_to.isin(t5012)][['munid_from', 'munid_to']].values.flatten()
    )
    diff = len(t5012) - initlen

# Population numbers for these municipalities
popdataall[popdataall.munid.isin(t5012)].groupby('year').population.sum()

# Cleaned changes
municlean = munichanges.copy()
# Remove change from 114 to 128, 412 to 403, 720 to 704, 1850 to 1806, 5012 to 5056 and 5012 to 5055
mask = (
    ((municlean.munid_from == 114) & (municlean.munid_to == 128)) |
    ((municlean.munid_from == 412) & (municlean.munid_to == 403)) |
    ((municlean.munid_from == 720) & (municlean.munid_to == 704)) |
    ((municlean.munid_from == 1850) & (municlean.munid_to == 1806)) |
    ((municlean.munid_from == 5012) & (municlean.munid_to == 5056)) |
    ((municlean.munid_from == 5012) & (municlean.munid_to == 5055))
)
municlean.drop(municlean[mask].index, inplace=True)

# Handle merger of 1534 (Haram) and 1504 (Ålesund) plus others into 1507, and subsequent split of 1507 into 1580 (Haram) and 1508 (Ålesund)
# Remove both changes for Haram, and add change for 1534 to 1580
mask = (
    ((municlean.munid_from == 1534) & (municlean.munid_to == 1507)) |
    ((municlean.munid_from == 1507) & (municlean.munid_to == 1580))
)
municlean.drop(municlean[mask].index, inplace=True)
municlean = pd.concat((municlean, pd.DataFrame({'munid_from': 1534, 'munid_to': 1580, 'date': pd.to_datetime('2024-01-01')}, index=[0])), ignore_index=True)

# Roll changes forward from first date of munichanges to last with popdataall, creating munid2024 and munid2018
popdataall['munid2024'] = popdataall.munid
popdataall['munid2018'] = popdataall.munid
changedates = municlean.date.unique()
# Make sure the dates are sorted in ascending order
changedates = np.sort(changedates)
for d in changedates:
    changes = municlean[municlean.date == d].set_index('munid_from').munid_to
    popdataall['munid2024'] = popdataall.munid2024.replace(changes)
    if d <= pd.to_datetime('2018-01-01'):
        popdataall['munid2018'] = popdataall.munid2018.replace(changes)

# Population numbers for recoded municipalities
temppop = popdataall[['year', 'munid2024', 'population']].rename(columns={'munid2024': 'munid'}).groupby(['year', 'munid']).population.sum()
popdiff = (temppop - popdata.set_index(['year', 'munid']).population.sort_index())
popdiff.describe()
popdiff[popdiff > 0].reset_index().munid.unique()
# Missing people for 2024-municipalities 1806, 1875, 5055, 5056, 5059
# All part of combined mergers in 2020 (municipalities 1850 and 5012 split across several municipalities)
# Start with the set of Trøndelag municipalities, where 5012 was split across 5055, 5056 and 5059
t5012 = [5055, 5056, 5059]
donors = munichanges[munichanges.munid_to.isin(t5012)].munid_from.unique()
popdataall.groupby(['year', 'munid2018']).population.sum().unstack().loc[:, donors].sum(axis=1)

# Walkback to find the municipality code at 2020-01-01
# Create a dictionary for the changes
# Start with changes in 2024, then 2023 and then 2022 (no further changes necessary)
changedates = munichanges.date.unique()
# Make sure the dates are sorted in descending order
changedates = np.sort(changedates)[::-1]

municode = {x: x for x in muni2024.code.values if x != 9999}
for date in changedates:
    changes = munichanges[munichanges.date == date]
    for _, change in changes.iterrows():
        municode[change.munid_to] = change.munid_from

