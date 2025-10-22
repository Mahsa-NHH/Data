"""
====================================================================
⚡ ENTSO-E Transparency Data Downloader
====================================================================
Created on: 02.09.2024  
Author: M. Saethre  
Updated on: 15.10.2025 by Mahsa  

📘 DESCRIPTION:
This script downloads power system data from the **ENTSO-E Transparency Platform**
via its RESTful API. It collects two main datasets for the **Nordic bidding zones**:

1️⃣ **Actual Generation per Production Type (A75)**
   → Hourly actual generation data, broken down by production type (e.g., Hydro, Wind, Nuclear)

2️⃣ **Installed Capacity per Production Type (A68)**
   → Year-ahead installed generation capacity by production type

It uses XML parsing to extract time series data for each bidding zone (e.g., DK1, NO3, SE4)
and saves the results as CSV files.

--------------------------------------------------------------------
🧭 HOW TO RUN:
1. Make sure you have the required libraries installed:

--------------------------------------------------------------------
📊 OUTPUT FILES:
Two CSV files are created:
1. `nordic_hourly_gen_prodtype.csv`
→ Hourly actual generation by production type
2. `nordic_installed_capacities.csv`
→ Installed generation capacity by production type

Both files are saved under:
`C:/Users/s15832/Documents/Project/Data/entsoe/`

Columns:
| Column     | Description |
|-------------|-------------|
| bzn         | Bidding zone (e.g., NO1, SE3) |
| prodtype    | Production type (e.g., HydroRes, WindOn) |
| timestamp   | Time in UTC (hourly) |
| quantity    | Value in MW |

--------------------------------------------------------------------
⚙️ PURPOSE:
This script automates ENTSO-E data collection
====================================================================
"""

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
from pathlib import Path
import time  # For future retry or delay handling


# =============================================================================
# API Configuration
# =============================================================================

# Base URL for ENTSO-E Transparency API
base_url = "https://web-api.tp.entsoe.eu/api"

# Personal API key (replace with your own if needed)
api_key = "0e618bc9-1f36-4186-b70c-b2518618dcaf"

# Define XML namespaces used in ENTSO-E responses
namespaces = {
 'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
}


# =============================================================================
# Load Reference Data (Bidding Zones, Document Types, Process Types, etc.)
# =============================================================================

# Load bidding zone codes from CSV
# CSV must contain at least two columns: "BZN" and "code"
areacodes = pd.read_csv('C:/Users/s15832/Documents/Project/Data/entsoe/entsoe_area_codes.csv')

########################################################
# ENTSO-E Parameter Dictionaries
########################################################

# ------------------------------
# DocumentType (defines data type)
# ------------------------------
documenttype = pd.DataFrame({
 'code': [
     'A09', 'A11', 'A15', 'A24', 'A25', 'A26', 'A31', 'A37', 'A38', 'A44',
     'A61', 'A63', 'A65', 'A68', 'A69', 'A70', 'A71', 'A72', 'A73', 'A74',
     'A75', 'A76', 'A77', 'A78', 'A79', 'A80', 'A81', 'A82', 'A83', 'A84',
     'A85', 'A86', 'A87', 'A88', 'A89', 'A90', 'A91', 'A92', 'A93', 'A94',
     'A95', 'B11', 'B17', 'B45'
 ],
 'description': [
     'Finalised schedule', 'Aggregated energy data report', 'Acquiring system operator reserve schedule',
     'Bid document', 'Allocation result document', 'Capacity document', 'Agreed capacity',
     'Reserve bid document', 'Reserve allocation result document', 'Price Document',
     'Estimated Net Transfer Capacity', 'Redispatch notice', 'System total load',
     'Installed generation per type', 'Wind and solar forecast', 'Load forecast margin',
     'Generation forecast', 'Reservoir filling information', 'Actual generation',
     'Wind and solar generation', 'Actual generation per type', 'Load unavailability',
     'Production unavailability', 'Transmission unavailability', 'Offshore grid infrastructure unavailability',
     'Generation unavailability', 'Contracted reserves', 'Accepted offers',
     'Activated balancing quantities', 'Activated balancing prices', 'Imbalance prices',
     'Imbalance volume', 'Financial situation', 'Cross border balancing', 'Contracted reserve prices',
     'Interconnection network expansion', 'Counter trade notice', 'Congestion costs', 'DC link capacity',
     'Non EU allocations', 'Configuration document', 'Flow-based allocations',
     'Aggregated netted external TSO schedule document', 'Bid Availability Document'
 ]
})
# Short names for easier referencing
documenttype['short_name'] = [
 'FinSched', 'AggEnergy', 'AcqRes', 'BidDoc', 'AllocDoc', 'CapDoc', 'AgrCap', 'ResBid', 'ResAlloc',
 'Price', 'ENTSOE', 'Redispatch', 'SysLoad', 'GenType', 'WSForecast', 'LoadMarg', 'GenForecast',
 'ResFill', 'ActGen', 'WSAct', 'ActGenType', 'LoadUnavail', 'ProdUnavail', 'TransUnavail',
 'OffUnavail', 'GenUnavail', 'ContrRes', 'AccOffers', 'ActBalQ', 'ActBalP', 'ImbPrice', 'ImbVol',
 'FinSit', 'CrossBal', 'ContrResP', 'NetExp', 'CountTr', 'CongCost', 'DCLinkCap', 'NonEUAlloc',
 'ConfigDoc', 'FlowAlloc', 'AggTSO', 'BidAvail'
]
documenttype.set_index('short_name', inplace=True)


# ------------------------------
# ProcessType (defines temporal scope)
# ------------------------------
processtype = pd.DataFrame({
 'code': [
     'A01', 'A02', 'A16', 'A18', 'A31', 'A32', 'A33', 'A39', 'A40', 'A46',
     'A47', 'A51', 'A52', 'A56', 'A60', 'A61', 'A67', 'A68'
 ],
 'description': [
     'Day ahead', 'Intra day incremental', 'Realised', 'Intraday total', 'Week ahead', 'Month ahead',
     'Year ahead', 'Synchronisation process', 'Intraday process', 'Replacement reserve',
     'Manual frequency restoration reserve', 'Automatic frequency restoration reserve',
     'Frequency containment reserve', 'Frequency restoration reserve', 'Scheduled activation mFRR',
     'Direct activation mFRR', 'Central Selection aFRR', 'Local Selection aFRR'
 ]
})
processtype['short_name'] = [
 'DA', 'IDI', 'Real', 'IDT', 'WA', 'MA', 'YA', 'Sync', 'ID', 'RR',
 'mFRR_man', 'aFRR', 'FCR', 'FRR', 'mFRR_sched', 'mFRR_dir', 'aFRR_cen', 'aFRR_loc'
]
processtype.set_index('short_name', inplace=True)


# ------------------------------
# PsrType (Production types)
# ------------------------------
psrtype = pd.DataFrame({
 'code': [
     'A03', 'A04', 'A05', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09',
     'B10', 'B11', 'B12', 'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21',
     'B22', 'B23', 'B24'
 ],
 'description': [
     'Mixed', 'Generation', 'Load', 'Biomass', 'Fossil Brown coal/Lignite', 'Fossil Coal-derived gas',
     'Fossil Gas', 'Fossil Hard coal', 'Fossil Oil', 'Fossil Oil shale', 'Fossil Peat', 'Geothermal',
     'Hydro Pumped Storage', 'Hydro Run-of-river and poundage', 'Hydro Water Reservoir', 'Marine',
     'Nuclear', 'Other renewable', 'Solar', 'Waste', 'Wind Offshore', 'Wind Onshore', 'Other',
     'AC Link', 'DC Link', 'Substation', 'Transformer'
 ]
})
psrtype['short_name'] = [
 'Mixed', 'Gen', 'Load', 'Bio', 'CoalBrown', 'CoalGas', 'Gas', 'CoalHard', 'Oil', 'OilShale',
 'Peat', 'Geo', 'HydroPS', 'HydroRoR', 'HydroRes', 'Marine', 'Nuclear', 'OtherRenew', 'Solar',
 'Waste', 'WindOff', 'WindOn', 'Other', 'LinkAC', 'LinkDC', 'Substation', 'Transformer'
]
psrtype.set_index('short_name', inplace=True)


# =============================================================================
# Functions for Data Retrieval and XML Parsing
# =============================================================================

def get_entsoe_data(document_type, process_type, in_domain, period_start, period_end, out_domain=None, bzn=None):
 """Fetch data from ENTSO-E API for given parameters."""
 params = {
     "securityToken": api_key,
     "documentType": document_type,
     "processType": process_type,
     "in_Domain": in_domain,
     "periodStart": period_start,
     "periodEnd": period_end
 }
 if out_domain:
     params["out_Domain"] = out_domain

 response = requests.get(base_url, params=params)

 if response.status_code == 200:
     return parse_xml_response(response.content, bzn=bzn)
 else:
     print(f"Error {response.status_code} for {bzn}")
     print(f"Response: {response.text[:500]}")
     print(f"Request URL: {response.url}")
     return []


def parse_xml_response(xml_content, bzn=None):
 """Parse XML and extract hourly values for production types."""
 root = ET.fromstring(xml_content)
 time_series = root.findall('.//ns:TimeSeries', namespaces=namespaces)

 data = []
 for ts in time_series:
     start_el = ts.find('.//ns:Period/ns:timeInterval/ns:start', namespaces=namespaces)
     if start_el is None or start_el.text is None:
         continue
     start_time = datetime.fromisoformat(start_el.text.replace('Z', '+00:00'))

     psr_el = ts.find('.//ns:MktPSRType/ns:psrType', namespaces=namespaces)
     psr_type = psr_el.text if psr_el is not None else None

     for point in ts.findall('.//ns:Period/ns:Point', namespaces=namespaces):
         pos_el = point.find('ns:position', namespaces=namespaces)
         qty_el = point.find('ns:quantity', namespaces=namespaces)
         if pos_el is None or qty_el is None:
             continue
         position = int(pos_el.text)
         quantity = float(qty_el.text)
         timestamp = start_time + timedelta(hours=position - 1)

         data.append({
             'bzn': bzn,
             'psrtype': psr_type,
             'timestamp': timestamp,
             'quantity': quantity
         })
 return data


# =============================================================================
# Download Actual Generation per Production Type (A75)
# =============================================================================

nordic_bzn = ['DK1', 'DK2', 'FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'SE1', 'SE2', 'SE3', 'SE4']
bznmap = areacodes[areacodes.BZN.notnull()].set_index('BZN').code

data = []
for bzn in nordic_bzn:
 for year in range(2014, 2025):
     print("Fetching", bzn, year)
     data.extend(get_entsoe_data(
         document_type=documenttype.loc['ActGenType', 'code'],
         process_type=processtype.loc['Real', 'code'],
         in_domain=bznmap.loc[bzn],
         period_start=f"{year}01010000",
         period_end=f"{year}12310000",
         bzn=bzn
     ))

data = pd.DataFrame(data)
data['prodtype'] = data.psrtype.map(psrtype.reset_index().set_index('code').short_name)
data[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv(
 'C:/Users/s15832/Documents/Project/Data/entsoe/nordic_hourly_gen_prodtype.csv', index=False
)

# =============================================================================
# Download Installed Capacities per Production Type (A68)
# =============================================================================

cap_data = []
for bzn in nordic_bzn:
 for year in range(2014, 2025):
     print("Fetching", bzn, year)
     cap_data.extend(get_entsoe_data(
         document_type=documenttype.loc['GenType', 'code'],
         process_type=processtype.loc['YA', 'code'],
         in_domain=bznmap.loc[bzn],
         period_start=f"{year}01010000",
         period_end=f"{year}12310000",
     ))

cap_df = pd.DataFrame(cap_data)
cap_df['prodtype'] = cap_df.psrtype.map(psrtype.reset_index().set_index('code').short_name)
cap_df[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv(
 'C:/Users/s15832/Documents/Project/Data/entsoe/nordic_installed_capacities.csv', index=False
)
