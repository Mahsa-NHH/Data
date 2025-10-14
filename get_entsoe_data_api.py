"""
Created on 02.09.2024
Author: M. Saethre

Description: This script will download data from the ENTSO-E Transparency restful API

"""

import requests
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


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
# Load area codes from csv file
areacodes = pd.read_csv('E:/electricity/entsoe/entsoe_area_codes.csv')

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
def get_entsoe_data(document_type, process_type, in_domain, period_start, period_end):
    """
    Fetches data from the ENTSO-E Transparency API.

    Args:
        document_type (str): The document type code (e.g., 'A75' for Actual generation per type).
        process_type (str): The process type code (e.g., 'A16' for Realised).
        in_domain (str): The bidding zone code (e.g., '10YNO-1--------2' for Norway).
        period_start (str): Start of the period in YYYYMMDDHHMM format.
        period_end (str): End of the period in YYYYMMDDHHMM format.

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

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        return parse_xml_response(response.content)
    else:
        print("Error:", response.status_code)
        return []

def parse_xml_response(xml_content):
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
        start = ts.find('.//ns:Period/ns:timeInterval/ns:start', namespaces=namespaces).text
        start_time = datetime.fromisoformat(start.replace('Z', '+00:00'))
        psr_type = ts.find('.//ns:MktPSRType/ns:psrType', namespaces=namespaces).text
        points = ts.findall('.//ns:Period/ns:Point', namespaces=namespaces)

        for point in points:
            position = int(point.find('ns:position', namespaces=namespaces).text)
            quantity = float(point.find('ns:quantity', namespaces=namespaces).text)
            timestamp = start_time + timedelta(hours=position - 1)

            data.append({
                'bzn': bzn,
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
        print("Fetching", bzn, year)
        data.extend(get_entsoe_data(
            document_type=documenttype.loc['ActGenType', 'code'],
            process_type=processtype.loc['Real', 'code'],
            in_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000"
        ))

data = pd.DataFrame(data)
data['prodtype'] = data.psrtype.map(psrtype.reset_index().set_index('code').short_name)

data[['bzn', 'prodtype', 'timestamp', 'quantity']].to_csv('E:/electricity/entsoe/nordic_hourly_gen_prodtype.csv', index=False)


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
        print("Fetching", bzn, year)
        response_data = get_entsoe_data(
            document_type=documenttype.loc['SysLoad', 'code'],
            process_type=processtype.loc['Real', 'code'],
            in_domain=bznmap.loc[bzn],
            out_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000"
        )
        # Add bidding zone to each data point
        for record in response_data:
            record['bidding_zone'] = bzn
        data.extend(response_data)

real_load = pd.DataFrame(data)

# Get day-ahead total load
data = []
for bzn in nordic_bzn:
    for year in range(2014, 2025):
        print("Fetching", bzn, year)
        response_data = get_entsoe_data(
            document_type=documenttype.loc['SysLoad', 'code'],
            process_type=processtype.loc['DA', 'code'],
            in_domain=bznmap.loc[bzn],
            out_domain=bznmap.loc[bzn],
            period_start=f"{year}01010000",
            period_end=f"{year}12310000"
        )
        # Add bidding zone to each data point
        for record in response_data:
            record['bidding_zone'] = bzn
        data.extend(response_data)

da_load = pd.DataFrame(data)
