"""
author: morten
date: 2024-10-13

1. Get population data per municipality and year from SSB API (Statistics Norway)
Use 2020 municipality codes

2. Get centrality index for 2020 municipalities from SSB API
classification ID for municipality: 131
classification ID for centrality: 128 
use classurl for municipality with extension correspondsAt and targetClassificationId=128 and date=2020-01-01

3. Get income measures per municipality from SSB API
Table 06944
    - Region selection filter all ('values': ['*'])
    - Tid selection filter all
    - ContentsCode selection filter item values 'InntSkatt' and 'AntallHushold'
    - HusholdType selection filter all
    - Response format csv3

Procedure:
municipality codes and changes over time from klass
population per municipality in Norway from table 07459 for all years and all municipality codes and 1-year age groups
"""

import requests
import pandas as pd
import numpy as np
from io import StringIO

# Base URL for SSB API
baseurl = "http://data.ssb.no/api/"

# Statistics Norway URL for municipality classification
classurl = f'{baseurl}klass/v1/classifications/131/'

# Get correspondence to centrality
rcentrality = requests.get(f'{classurl}correspondsAt',
                            params={'targetClassificationId': '128', 'date': '2020-01-01'})
# Make DataFrame from response json 'correspondenceItems'
centrality2020 = pd.DataFrame(rcentrality.json()['correspondenceItems'])
coldict = {'sourceCode': 'munid', 'targetCode': 'centrality'}
centrality2020.rename(columns=coldict, inplace=True)
centrality2020['munid'] = centrality2020.munid.astype(int)
centrality2020['centrality'] = centrality2020.centrality.astype(int)
# Store centrality munid and centrality as csv in E:/utility, keeping only those columns
centrality2020[['munid', 'centrality']].to_csv('E:/utility/centrality2020.csv', index=False)

############################
# Municipality codes
############################
# Get codesAt.csv with params date=2020-01-01 and put in a DataFrame
rmuni2020 = requests.get(f'{classurl}codesAt.csv',
                            params={'date': '2020-01-01'})
muni2020 = pd.read_csv(StringIO(rmuni2020.text))

############################
# Population data
############################
# URL for population table 07459
popurl = f'{baseurl}v0/no/table/07459'

# Get total population for Norway from table 07459 by eliminating Region
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


# Population for all municipality codes: Region, filter = 'item' and values = ['*'], and for all years and age groups
# Limit on 800.000 observations, so split query by year
popdata = pd.DataFrame()
for y in range(1986, 2025):
    popquery = {
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
                    'filter': 'item',
                    'values': [f'{y}']
                }
            },
            {
                'code': 'Alder',
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

    popr = requests.post(popurl, json=popquery)
    popdata = pd.concat([popdata, pd.read_csv(StringIO(popr.text))], ignore_index=True)
    
# Remove Region codes that are not municipality codes (length != 4)
popdata.drop(popdata[popdata.Region.str.len() != 4].index, inplace=True)
popdata['munid'] = popdata.Region.astype(int)
popdata.rename(columns={'07459': 'population', 'Tid': 'year', 'Alder': 'age'}, inplace=True)
# Drop observations with zero population in municipality for all age categories within a given year
popdata.drop(popdata[popdata.groupby(['year', 'munid']).population.transform('sum') == 0].index, inplace=True)
# Drop region and ContentsCode columns
popdata.drop(['Region', 'ContentsCode'], axis=1, inplace=True)


# Get changes in municipality codes from 1986-01-0
# Use changes.csv with params from=1986-01-01
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
popdata[popdata.munid.isin(t5012)].groupby('year').population.sum()

# Cleaned changes
municlean = munichanges.copy()
# Remove change from 114 to 128 - Varteig split between Sarpsborg and Rakkestad
## almost no change in Rakkestad population, sum of pop for merged munids suggests virtually all in Sarpsborg
# 412 to 403 - Parts of Ringsaker to Hamar (Ringsaker keeps existing)
## very minor change in population for Ringsaker
# 720 to 704  - Stokke split between Tønsberg and Sandefjord
## Minor break in trend for Tønsberg, larger increase for Sandefjord
# 1850 to 1806 - Tysfjord split between Narvik and Hamarøy
## Roughly similar change in population for both, keep Narvik since Narvik is larger
## Note: Address designation from registry data can refine this
# 5012 to 5056 and 5012 to 5055 - Snillfjord split between Heim, Hitra and Orkland
## Most continuity for sum of pop of munids going to Orkland (5059)
mask = (
    ((municlean.munid_from == 114) & (municlean.munid_to == 128)) |
    ((municlean.munid_from == 412) & (municlean.munid_to == 403)) |
    ((municlean.munid_from == 720) & (municlean.munid_to == 704)) |
    ((municlean.munid_from == 1850) & (municlean.munid_to == 1806)) |
    ((municlean.munid_from == 5012) & (municlean.munid_to == 5056)) |
    ((municlean.munid_from == 5012) & (municlean.munid_to == 5055))
)
# Mark rows that can be dropped in munichanges
munichanges['multi_drop'] = mask

municlean.drop(municlean[mask].index, inplace=True)

# Store munichanges in E:/utility/munid_changes.csv
munichanges.to_csv('E:/utility/munid_changes.csv', index=False)

# Get municipality codes for each of version date (unique dates in munichanges)
# Fetch codesAt.csv for each date={d} as parameter, keep fields "code" and "name" and add "date", and concatenate
municodes = pd.DataFrame()
for d in munichanges.date.unique():
    rmunid = requests.get(f'{classurl}codesAt.csv',
                            params={'date': d.strftime('%Y-%m-%d')})
    tempdf = pd.read_csv(StringIO(rmunid.text))
    tempdf['date'] = d
    municodes = pd.concat([municodes, tempdf[['code', 'name', 'date']]], ignore_index=True)

# Store municodes in E:/utility/munid_codes_{minyear}_{maxyear}.csv
minyear = municodes.date.min().year
maxyear = municodes.date.max().year
municodes.to_csv(f'E:/utility/munid_codes_{minyear}_{maxyear}.csv', index=False)

# Roll changes forward from first date of munichanges up to and including 2020 with popdata, creating munid2020
popdata['munid2020'] = popdata.munid
changedates = municlean[municlean.date <= '2020-01-01'].date.unique()
# Make sure the dates are sorted in ascending order
changedates = np.sort(changedates)
for d in changedates:
    changes = municlean[municlean.date == d].set_index('munid_from').munid_to
    popdata['munid2020'] = popdata.munid2020.replace(changes)

# Roll back changes from 2024-01-01 to 2020-01-01
changedates = municlean[municlean.date > '2020-01-01'].date.unique()
# Make sure the dates are sorted in descending order
changedates = np.sort(changedates)[::-1]
for d in changedates:
    changes = municlean[municlean.date == d].set_index('munid_to').munid_from
    popdata['munid2020'] = popdata.munid2020.replace(changes)

# Check coverage based on muni2020 code
popdata.munid2020.isin(muni2020.code).mean()

# Check coverage of population data for each year
(popdata.groupby('year').population.sum() - totpopdata.set_index('year').population).describe()

# Drop munid, rename munid2020 to munid and aggregate by year, munid and age
popdata.drop('munid', axis=1, inplace=True)
popdata.rename(columns={'munid2020': 'munid'}, inplace=True)
popdata = popdata.groupby(['year', 'munid', 'age']).population.sum().reset_index()

# Make age into numeric, recode first 105+ to 105
popdata['age'] = popdata.age.replace({'105+': '105'}).astype(int)

# Store data as csv
popdata.to_csv('E:/utility/population_muni_year_age.csv', index=False)

############################
# Income measures
############################
# URL for income table 06944
incurl = f'{baseurl}v0/no/table/06944'

# Get income measures for all municipalities for all years
incquery = {
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
        },
        {
            'code': 'ContentsCode',
            'selection': {
                'filter': 'item',
                'values': ['InntSkatt', 'AntallHushold']
            }
        },
        {
            'code': 'HusholdType',
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

incr = requests.post(incurl, json=incquery)
incdata = pd.read_csv(StringIO(incr.text))
incdata.rename(columns={'06944': 'value', 'Tid': 'year', 'Region': 'munid', 'ContentsCode': 'measure'}, inplace=True)
# Convert munid to string
incdata['munid'] = incdata.munid.astype(str)
# Keep only where munid is 4 digits
incdata.drop(incdata[incdata.munid.str.len() != 4].index, inplace=True)
# convert munid to int
incdata['munid'] = incdata.munid.astype(int)
# Drop observations where value == '.' or ':'
incdata.drop(incdata[incdata.value.isin(['.', ':'])].index, inplace=True)
# Roll changes forward from first date of munichanges up to and including 2020 with popdata, creating munid2020
incdata['munid2020'] = incdata.munid
changedates = municlean[municlean.date <= '2020-01-01'].date.unique()
# Make sure the dates are sorted in ascending order
changedates = np.sort(changedates)
for d in changedates:
    changes = municlean[municlean.date == d].set_index('munid_from').munid_to
    incdata['munid2020'] = incdata.munid2020.replace(changes)

# only keep HusholdType=0, Drop HusholdType column
incdata.drop(incdata[incdata.HusholdType != 0].index, inplace=True)
incdata.drop('HusholdType', axis=1, inplace=True)

# measure and value are pairs of InntSkatt and AntallHushold, so pivot to wide format
incdata['value'] = incdata.value.astype(int)
incdata = incdata.pivot_table(index=['year', 'munid', 'munid2020'], columns='measure', values='value').reset_index()
incdata.rename(columns={'AntallHushold': 'nhouseholds', 'InntSkatt': 'income_posttax'}, inplace=True)
# Convert nhouseholds and income_posttax to float
incdata['nhouseholds'] = incdata.nhouseholds.astype(float)
incdata['income_posttax'] = incdata.income_posttax.astype(float)
# Generate total income, aggregate by year and munid with sum of totincome and nhouseholds, then calculate income as totincome / nhouseholds
incdata['totincome'] = incdata.income_posttax * incdata.nhouseholds
incdata = incdata.groupby(['year', 'munid2020'])[['totincome', 'nhouseholds']].sum().reset_index()
incdata['income'] = incdata.totincome / incdata.nhouseholds
# Drop totincome
incdata.drop(['totincome'], axis=1, inplace=True)
# Rename munid2020 to munid
incdata.rename(columns={'munid2020': 'munid'}, inplace=True)

# # Categorize each municipality by income quartile separately by year
# incdata['income_quartile'] = incdata.groupby('year').income.transform(lambda x: pd.qcut(x, 4, labels=False))
# # Sort by munid and year
# incdata.sort_values(['munid', 'year'], inplace=True)
# # Generate lagged income quartile
# incdata['income_quartile_lag'] = incdata.groupby('munid').income_quartile.shift(1)

# Store data as csv
incdata.to_csv('E:/utility/income_muni_year.csv', index=False)