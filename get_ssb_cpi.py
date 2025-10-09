"""
author: morten
date: 2025-02-01

Get data from SSB API or table downloads

1. Obtain monthly CPI from 1920 to 2024
Table 08981: Konsumprisindeks, historisk serie, etter måned (2015=100) 1920 - 2024

2. Obtain yearly CPI from 1920 to 2024
Table 08184: Konsumprisindeks, historisk serie (2015=100) 1865 - 2024
"""

import requests
import pandas as pd
import numpy as np
from io import StringIO

ssb_table_url = 'https://data.ssb.no/api/v0/no/table/'
cpi_url = f'{ssb_table_url}08981/'
cpi_year_url = f'{ssb_table_url}08184/'

cpi_year_url
cpi_query = {
  "query": [
    {
      "code": "Maaned",
      "selection": {
        "filter": "item",
        "values": [
          "01",
          "02",
          "03",
          "04",
          "05",
          "06",
          "07",
          "08",
          "09",
          "10",
          "11",
          "12"
        ]
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

# Get data from SSB API
r = requests.post(cpi_url, json=cpi_query)

cpi = pd.read_csv(StringIO(r.text))
# rename Maaned -> month, Tid -> year, 08981 -> cpi
cpi.rename(columns={'Maaned': 'month', 'Tid': 'year', '08981': 'cpi'}, inplace=True)
# drop ContentsCode
cpi.drop(columns=['ContentsCode'], inplace=True)
# Convert year (int) and month (int) to date
cpi['date'] = pd.to_datetime(cpi['year'].astype(str) + '-' + cpi['month'].astype(str) + '-01')

# Sort by date
cpi.sort_values('date', inplace=True)

# Convert cpi to float (missing is '.')
cpi['cpi'] = cpi['cpi'].replace('.', np.nan).astype(float)

# save to csv in E:/utility as cpi_monthly_1920_2024.csv
# only keep cpi and date
cpi[['date', 'cpi']].to_csv('E:/utility/cpi_monthly_1920_2024.csv', index=False)

# Get yearly CPI
cpi_year_query = {
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

r_year = requests.post(cpi_year_url, json=cpi_year_query)
cpi_year = pd.read_csv(StringIO(r_year.text))
# rename Tid -> year, 08184 -> cpi
cpi_year.rename(columns={'Tid': 'year', '08184': 'cpi'}, inplace=True)
# drop ContentsCode
cpi_year.drop(columns=['ContentsCode'], inplace=True)

# Save to csv in E:/utility as cpi_yearly_1920_2024.csv
cpi_year[['year', 'cpi']].to_csv('E:/utility/cpi_yearly_1920_2024.csv', index=False)