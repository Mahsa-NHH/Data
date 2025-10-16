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
########################################################
# Load area codes from csv file (ENV first, then fallback)
env_dir = os.getenv("ENTSOE_DATA_DIR")
default_dir = Path(r"C:/Users/s15832/Documents/Project/Data/entsoe")
data_dir = Path(env_dir).expanduser() if env_dir else default_dir
areacodes = pd.read_csv(data_dir / "entsoe_area_codes.csv")

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

# Map document code -> short name for logging
DOC_SHORT_BY_CODE = documenttype.reset_index().set_index('code')['short_name']


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
def get_entsoe_data(document_type, process_type, in_domain, period_start, period_end, bzn_label=None, out_domain=None):
    """
    Fetches data from the ENTSO-E Transparency API.

    Args:
        document_type (str): The document type code (e.g., 'A75' for Actual generation per type).
        process_type (str): The process type code (e.g., 'A16' for Realised).
        in_domain (str): The bidding zone code (e.g., '10YNO-1--------2' for Norway).
        period_start (str): Start of the period in YYYYMMDDHHMM format.
        period_end (str): End of the period in YYYYMMDDHHMM format.
        bzn_label (str, optional): Human-readable BZN label (e.g., 'NO1') attached to rows.
        out_domain (str, optional): Optional out_Domain EIC for queries that require it.

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
        return parse_xml_response(response.content, bzn_label or in_domain)
    else:
        print("Error:", response.status_code)
        return []

def parse_xml_response(xml_content, bzn_label):
    """
    Args:
        xml_content (str): The XML content to parse.
        bzn_label (str): Label of the bidding zone to attach to rows.

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

        points = ts.findall('.//ns:Period/ns:Point', namespaces=namespaces)

        for point in points:
            pos_el = point.find('ns:position', namespaces=namespaces)
            qty_el = point.find('ns:quantity', namespaces=namespaces)
            if pos_el is None or qty_el is None:
                continue
            position = int(pos_el.text)
            quantity = float(qty_el.text)
            timestamp = start_time + timedelta(hours=position - 1)

            data.append({
                'bzn': bzn_label,
                'psrtype': psr_type,
                'timestamp': timestamp,
                'quantity': quantity
            })

    return data

##################################################
# Download actual generation per generation type
##################################################

# Nordic bidding zones
nordic_bzn = ['DK1', 'DK2', 'FI', 'NO1', 'NO2', 'NO3', 'NO4', 'NO5', 'SE1', 'SE2', 'SE3', 'SE4']
bznmap = areacodes[areacodes.BZN.notnull()].set_index('BZN').code

data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        doc_code = documenttype.loc['ActGenType', 'code']  # A75
        print(f"Fetching {bzn} {year} {DOC_SHORT_BY_CODE.get(doc_code, doc_code)} Realised")
        data.extend(get_entsoe_data(
            document_type=doc_code,
            process_type=processtype.loc['Real', 'code'],          # A16
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000",
            bzn_label=bzn
        ))

data = pd.DataFrame(data)
data['prodtype'] = data.psrtype.map(psrtype.reset_index().set_index('code').short_name)

try:
    script_dir = Path(__file__).resolve().parent
except NameError:
    # __file__ is not defined (e.g., interactive mode)
    script_dir = Path.cwd()

# Determine output directory (ENTSOE_DATA_DIR or ./outputs next to this script)
env_dir = os.getenv("ENTSOE_DATA_DIR")
default_dir = script_dir / "outputs"
output_dir = Path(env_dir).expanduser() if env_dir else default_dir

# Ensure the directory exists
output_dir.mkdir(parents=True, exist_ok=True)

# Output file: CSV containing columns ['bzn', 'prodtype', 'timestamp', 'quantity']
output_file = output_dir / "nordic_hourly_gen_prodtype.csv"

# Write CSV (remove duplicate)
data[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv(output_file, index=False)


##################################################
# Download capacities per generation type
##################################################

data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year)
        data.extend(get_entsoe_data(
            document_type=documenttype.loc['GenType', 'code'],
            process_type=processtype.loc['YA', 'code'],
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000"
        ))

# Test
in_domain = bznmap.loc[nordic_bzn[0]]
document_type=documenttype.loc['GenType', 'code']
process_type=processtype.loc['YA', 'code']
year = 2015
period_start=f"{year}01010000"
period_end=f"{year}12310000"

params = {
    "securityToken": api_key,
    "documentType": document_type,
    "processType": process_type,
    "in_Domain": in_domain,
    "periodStart": period_start,
    "periodEnd": period_end
}

response = requests.get(base_url, params=params)
pd.DataFrame(parse_xml_response(response.content)).shape

##################################################
# Download Total load per bidding zone and year
# Document type: 'A65' (System total load)
# Process type: 'A16' (Realised)
##################################################

data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year, "A65 Realised")
        response_data = get_entsoe_data(
            document_type=documenttype.loc['SysLoad', 'code'],    # A65
            process_type=processtype.loc['Real', 'code'],          # A16
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000",
            bzn_label=bzn
        )
        for record in response_data:
            record['bidding_zone'] = bzn
        data.extend(response_data)

real_load = pd.DataFrame(data)

##################################################
# Download capacities per generation type (A68, Year-ahead)
##################################################

data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year, "A68 Year-ahead")
        data.extend(get_entsoe_data(
            document_type=documenttype.loc['GenType', 'code'],     # A68
            process_type=processtype.loc['YA', 'code'],            # A33
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000",
            bzn_label=bzn
        ))

# Test
in_domain = bznmap.loc[nordic_bzn[0]]
document_type = documenttype.loc['GenType', 'code']
process_type = processtype.loc['YA', 'code']  # A33
year = 2015
period_start = f"{year}01010000"
period_end = f"{year}12310000"

params = {
    "securityToken": api_key,
    "documentType": document_type,
    "processType": process_type,
    "in_Domain": in_domain,
    "periodStart": period_start,
    "periodEnd": period_end
}

response = requests.get(base_url, params=params)
pd.DataFrame(parse_xml_response(response.content, bzn_label=nordic_bzn[0])).shape

##################################################
# Download Actual generation (A73, Realised) and Generation forecast (A71, Year-ahead)
##################################################

# A73 
a73_data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year, "A73 Realised")
        a73_data.extend(get_entsoe_data(
            document_type=documenttype.loc['ActGen', 'code'],  # A73
            process_type=processtype.loc['Real', 'code'],      # A16
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000",
            bzn_label=bzn
        ))
a73 = pd.DataFrame(a73_data)
(a73 if not a73.empty else pd.DataFrame(columns=['bzn','timestamp','quantity'])).to_csv(output_dir / "nordic_hourly_actual_generation.csv", index=False)

# A71
a71_data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year, "A71 Year-ahead")
        a71_data.extend(get_entsoe_data(
            document_type=documenttype.loc['GenForecast', 'code'],  # A71
            process_type=processtype.loc['YA', 'code'],             # A33
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000",
            bzn_label=bzn
        ))
a71 = pd.DataFrame(a71_data)
(a71 if not a71.empty else pd.DataFrame(columns=['bzn','timestamp','quantity'])).to_csv(output_dir / "nordic_generation_forecast.csv", index=False) 