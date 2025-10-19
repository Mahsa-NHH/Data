"""
Created on 02.09.2024
Author: M. Saethre
Updated on 10.15.2025 by Mahsa

Description: This script will download data from the ENTSO-E Transparency restful API

"""

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
from pathlib import Path
import time  # ADD THIS


# Define the base URL for the ENTSO-E Transparency API
base_url = "https://web-api.tp.entsoe.eu/api"

# Define the API key
api_key = "0e618bc9-1f36-4186-b70c-b2518618dcaf"

# Define namespaces for xml (response from API)
namespaces = {
    'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'
}

########################################################
# Define lists of parameters and values for the API
# Information from ENTSO-E API guidelines, appendix A
#######################################################
# Load area codes from csv file
areacodes = pd.read_csv('C:/Users/s15832/Documents/Project/Data/entsoe/entsoe_area_codes.csv')

### DocumentType
documenttype = pd.DataFrame({
    'code': ['A09', 'A11', 'A15', 'A24', 'A25', 'A26', 'A31', 'A37', 'A38', 'A44', 'A61', 'A63', 'A65', 'A68', 'A69', 
             'A70', 'A71', 'A72', 'A73', 'A74', 'A75', 'A76', 'A77', 'A78', 'A79', 'A80', 'A81', 'A82', 'A83', 'A84',
             'A85', 'A86', 'A87', 'A88', 'A89', 'A90', 'A91', 'A92', 'A93', 'A94', 'A95', 'B11', 'B17', 'B45'],
    'description': ['Finalised schedule', 'Aggregated energy data report', 'Acquiring system operator reserve schedule',
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
                    'Aggregated netted external TSO schedule document', 'Bid Availability Document']
})

# Create short names
documenttype['short_name'] = ['FinSched', 'AggEnergy', 'AcqRes', 'BidDoc', 'AllocDoc', 'CapDoc', 'AgrCap', 'ResBid', 'ResAlloc',
                              'Price', 'ENTSOE', 'Redispatch', 'SysLoad', 'GenType', 'WSForecast', 'LoadMarg', 'GenForecast',
                              'ResFill', 'ActGen', 'WSAct', 'ActGenType', 'LoadUnavail', 'ProdUnavail', 'TransUnavail',
                              'OffUnavail', 'GenUnavail', 'ContrRes', 'AccOffers', 'ActBalQ', 'ActBalP', 'ImbPrice', 'ImbVol',
                              'FinSit', 'CrossBal', 'ContrResP', 'NetExp', 'CountTr', 'CongCost', 'DCLinkCap', 'NonEUAlloc',
                              'ConfigDoc', 'FlowAlloc', 'AggTSO', 'BidAvail']
documenttype.set_index('short_name', inplace=True)


### ProcessType
processtype = pd.DataFrame({
    'code': ['A01', 'A02', 'A16', 'A18', 'A31', 'A32', 'A33', 'A39', 'A40', 'A46', 'A47', 'A51', 'A52', 'A56', 'A60', 'A61', 'A67', 'A68'],
    'description': ['Day ahead', 'Intra day incremental', 'Realised', 'Intraday total', 'Week ahead', 'Month ahead', 'Year ahead', 
                    'Synchronisation process', 'Intraday process', 'Replacement reserve', 'Manual frequency restoration reserve', 
                    'Automatic frequency restoration reserve', 'Frequency containment reserve', 'Frequency restoration reserve', 
                    'Scheduled activation mFRR', 'Direct activation mFRR', 'Central Selection aFRR', 'Local Selection aFRR']
})

# Create short names
processtype['short_name'] = ['DA', 'IDI', 'Real', 'IDT', 'WA', 'MA', 'YA', 'Sync', 'ID', 'RR', 'mFRR_man', 'aFRR', 'FCR', 'FRR', 'mFRR_sched', 'mFRR_dir', 'aFRR_cen', 'aFRR_loc']
processtype.set_index('short_name', inplace=True)


### PsrType
psrtype = pd.DataFrame({
    'code': ['A03', 'A04', 'A05', 'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B09', 'B10', 'B11',
             'B12', 'B13', 'B14', 'B15', 'B16', 'B17', 'B18', 'B19', 'B20', 'B21', 'B22', 'B23', 'B24'],
    'description': ['Mixed', 'Generation', 'Load', 'Biomass', 'Fossil Brown coal/Lignite', 'Fossil Coal-derived gas',
                    'Fossil Gas', 'Fossil Hard coal', 'Fossil Oil', 'Fossil Oil shale', 'Fossil Peat', 'Geothermal',
                    'Hydro Pumped Storage', 'Hydro Run-of-river and poundage', 'Hydro Water Reservoir', 'Marine',
                    'Nuclear', 'Other renewable', 'Solar', 'Waste', 'Wind Offshore', 'Wind Onshore', 'Other', 'AC Link',
                    'DC Link', 'Substation', 'Transformer']
})

# Create short names
psrtype['short_name'] = ['Mixed', 'Gen', 'Load', 'Bio', 'CoalBrown', 'CoalGas', 'Gas', 'CoalHard', 'Oil', 'OilShale',
                         'Peat', 'Geo', 'HydroPS', 'HydroRoR', 'HydroRes', 'Marine', 'Nuclear', 'OtherRenew', 'Solar',
                         'Waste', 'WindOff', 'WindOn', 'Other', 'LinkAC', 'LinkDC', 'Substation', 'Transformer']
psrtype.set_index('short_name', inplace=True)


#########################################################
# Functions: Request data from API and parse xml response
#########################################################
def get_entsoe_data(document_type, process_type, in_domain, period_start, period_end, out_domain=None, bzn=None):
    """
    Fetches data from the ENTSO-E Transparency API.

    Args:
        document_type (str): The document type code (e.g., 'A75' for Actual generation per type).
        process_type (str): The process type code (e.g., 'A16' for Realised).
        in_domain (str): The bidding zone code (e.g., '10YNO-1--------2' for Norway).
        period_start (str): Start of the period in YYYYMMDDHHMM format.
        period_end (str): End of the period in YYYYMMDDHHMM format.
        out_domain (str, optional): Output domain for load queries.
        bzn (str, optional): Bidding zone name for labeling.

    Returns:
        list: A list of dictionaries containing the extracted data.
    """

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
        print(f"Response: {response.text[:500]}")  # First 500 chars of error
        print(f"Request URL: {response.url}")
        return []

def parse_xml_response(xml_content, bzn=None):
    """
    Args:
        xml_content (str): The XML content to parse.

    Returns:
        list: A list of dictionaries containing the extracted data.
    """

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

        #points = ts.findall('.//ns:Period/ns:Point', namespaces=namespaces)

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

# ##################################################
# # Download actual generation per production type (A75, ActGenType)
# ##################################################

# # Nordic bidding zones
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

# #################################################
# # Download Installed capacities per production type (A68, GenType)
# #################################################

# cap_data = []
# for bzn in nordic_bzn:
#     for year in range(2014, 2025):
#         print ("Fetching", bzn, year)
#         cap_data.extend(get_entsoe_data(
#             document_type=documenttype.loc['GenType', 'code'],  # A68
#             process_type=processtype.loc['YA', 'code'],         # Year-ahead (A33)
#             in_domain=bznmap.loc[bzn],
#             period_start=f"{year}01010000",
#             period_end=f"{year}12310000",
#         ))

# cap_df = pd.DataFrame(cap_data)
# cap_df['prodtype'] = cap_df.psrtype.map(psrtype.reset_index().set_index('code').short_name)

# # Save
# cap_df[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv(
#     'C:/Users/s15832/Documents/Project/Data/entsoe/nordic_installed_capacities.csv', index=False
#       )

# ##################################################
# # Download Actual generation per generation unit (A73, Realised)
# ##################################################

# # A73 
# a73_data = []
# for bzn in nordic_bzn:
#     for year in range(2014, 2025):
#         print("Fetching", bzn, year, "A73 Realised")
#         a73_data.extend(get_entsoe_data(
#             document_type=documenttype.loc['ActGen', 'code'],  # A73
#             process_type=processtype.loc['Real', 'code'],      # A16
#             in_domain=bznmap.loc[bzn],
#             period_start=f"{year}01010000",
#             period_end=f"{year}12310000"
#         ))
# a73 = pd.DataFrame(a73_data)
# (a73 if not a73.empty else pd.DataFrame(columns=['bzn','timestamp','quantity'])).to_csv(output_dir / "nordic_hourly_actual_generation.csv", index=False)


##################################################
# Download Installed capacity per production unit (A71, Year-ahead)
##################################################
# a71_data = []
# for bzn in nordic_bzn:
#     for year in range(2014, 2025):
#         print("Fetching", bzn, year, "A71 Year-ahead")
#         a71_data.extend(get_entsoe_data(
#             document_type=documenttype.loc['GenForecast', 'code'],  # A71
#             process_type=processtype.loc['YA', 'code'],             # A33
#             in_domain=bznmap.loc[bzn],
#             period_start=f"{year}01010000",
#             period_end=f"{year}12310000",
#             bzn=bzn
#         ))
# a71 = pd.DataFrame(a71_data)
# #print(a71.columns.tolist())
# a71['prodtype'] = a71.psrtype.map(psrtype.reset_index().set_index('code').short_name)
# # Save
# a71[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv(
#     'C:/Users/s15832/Documents/Project/Data/entsoe/nordic_generation_forecast.csv', index=False
#       )

##################################################
# Download System total load (A65, Realised)
#per bidding zone and year
##################################################

###################################################
# Download System total load (A65, Realised)
##################################################

# # These zones are known to report load data
# load_supported_bzn = {
#     'DK1': '10YDK-1--------W',  # West Denmark
#     'DK2': '10YDK-2--------M',  # East Denmark
#     'FI': '10YFI-1--------U',   # Finland
#     'SE': '10YSE-1--------K',   # Sweden
# }

# data = []
# for bzn_name, bzn_code in load_supported_bzn.items():
#     for year in range(2015, 2025):
#         # Request data month by month to avoid API limits
#         for month in range(1, 13):
#             # Calculate period end (first day of next month)
#             if month == 12:
#                 period_end_year = year + 1
#                 period_end_month = 1
#             else:
#                 period_end_year = year
#                 period_end_month = month + 1
            
#             print(f"Fetching {bzn_name} {year}-{month:02d}")
            
#             response_data = get_entsoe_data(
#                 document_type=documenttype.loc['SysLoad', 'code'],  # A65
#                 process_type=processtype.loc['Real', 'code'],        # A16
#                 in_domain=bzn_code,
#                 out_domain=bzn_code,
#                 period_start=f"{year}{month:02d}010000",
#                 period_end=f"{period_end_year}{period_end_month:02d}010000",
#                 bzn=bzn_name
#             )
            
#             if response_data:
#                 print(f"  ✓ Got {len(response_data)} records")
#                 data.extend(response_data)
#             else:
#                 print(f"  ✗ No data")
            
#             time.sleep(1)  # Wait 1 second between requests

# real_load = pd.DataFrame(data)
# if not real_load.empty:
#     real_load[['bzn', 'timestamp', 'quantity']].to_csv(
#         'C:/Users/s15832/Documents/Project/Data/entsoe/nordic_hourly_system_load.csv',
#         index=False
#     )
#     print(f"\n✅ Saved {len(real_load)} records to CSV")
#     print(f"Zones with data: {real_load['bzn'].unique()}")
# else:
#     print("\n⚠️ No data collected for System total load.")

# ##################################################
# # Download System total load (A65, day-ahead)
# # per bidding zone and year
# ##################################################

# data = []
# for bzn in nordic_bzn:
#     for year in range(2014, 2025):
#         print("Fetching", bzn, year)
#         response_data = get_entsoe_data(
#             document_type=documenttype.loc['SysLoad', 'code'],
#             process_type=processtype.loc['DA', 'code'],
#             in_domain=bznmap.loc[bzn],
#             out_domain=bznmap.loc[bzn],
#             period_start=f"{year}01010000",
#             period_end=f"{year}12310000"
#         )
#         # Add bidding zone to each data point
#         for record in response_data:
#             record['bidding_zone'] = bzn
#         data.extend(response_data)

# da_load = pd.DataFrame(data)