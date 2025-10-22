"""
====================================================================
⚡ ENTSO-E Total Load Downloader — Day-Ahead vs Actual (A65)
====================================================================
Created on: 15.09.2024  
Author: M. Saethre  
Updated on: 15.10.2025 by Mahsa  

📘 DESCRIPTION:
This script downloads **Total Load (A65)** data from the **ENTSO-E Transparency Platform API**  
for all Nordic bidding zones (DK1, DK2, FI, NO1–NO5, SE1–SE4) for the years **2014–2025**.

It retrieves both:
1️⃣ **Day-Ahead Total Load Forecast** (processType=A01)  
2️⃣ **Actual Total Load** (processType=A16)

The data is combined and saved as a single CSV file with hourly values.

📊 OUTPUT FILE:
➡️ `entsoe_total_load_nordic_2014_2025.csv`

Saved in:
`C:/Users/s15832/Documents/Project/Data/entsoe/`

Columns:
| Column     | Description |
|-------------|-------------|
| bzn         | Bidding zone (e.g., NO1, SE4) |
| datetime    | UTC timestamp (hourly or 15-minute) |
| DayAhead    | Day-ahead forecast load (MW) |
| Actual      | Actual measured load (MW) |

"""

# =============================================================================
# IMPORTS
# =============================================================================
from entsoe import EntsoePandasClient
import pandas as pd
import xml.etree.ElementTree as ET
import os
import time


# =============================================================================
# CONFIGURATION
# =============================================================================

# ENTSO-E API key (replace with your own key if needed)
api_key = "0e618bc9-1f36-4186-b70c-b2518618dcaf"

# Initialize client from entsoe-py library
client = EntsoePandasClient(api_key=api_key)

# Output folder and file path
output_dir = r"C:\Users\s15832\Documents\Project\Data\entsoe"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "entsoe_total_load_nordic.csv")

# Mapping of Nordic bidding zones to their ENTSO-E EIC codes
nordic_bzn_eic = {
 "DK1": "10YDK-1--------W",
 "DK2": "10YDK-2--------M",
 "FI":  "10YFI-1--------U",
 "NO1": "10YNO-1--------2",
 "NO2": "10YNO-2--------T",
 "NO3": "10YNO-3--------J",
 "NO4": "10YNO-4--------9",
 "NO5": "10Y1001A1001A48H",
 "SE1": "10Y1001A1001A44P",
 "SE2": "10Y1001A1001A45N",
 "SE3": "10Y1001A1001A46L",
 "SE4": "10Y1001A1001A47J",
}


# =============================================================================
# XML PARSER FUNCTION
# =============================================================================
def parse_total_load(xml_text):
 """
 Parse ENTSO-E Total Load XML responses, supporting both namespace variants.

 Handles both:
 - urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0
 - urn:iec62325.351:tc57wg16:451-6:publicationdocument:7:0

 Returns:
     pd.DataFrame: datetime-indexed total load (MW)
 """
 ns_candidates = [
     {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"},
     {"ns": "urn:iec62325.351:tc57wg16:451-6:publicationdocument:7:0"},
 ]
 root = ET.fromstring(xml_text)

 for ns in ns_candidates:
     series = []
     for ts in root.findall(".//ns:TimeSeries", ns):
         # Extract time interval start
         start_time = ts.findtext(".//ns:timeInterval/ns:start", namespaces=ns)
         if not start_time:
             continue
         start = pd.Timestamp(start_time)

         # Determine resolution (hourly or 15-min)
         res = ts.findtext(".//ns:Period/ns:resolution", namespaces=ns)
         step = pd.to_timedelta(15, "m") if res == "PT15M" else pd.to_timedelta(1, "h")

         # Extract data points
         for p in ts.findall(".//ns:Point", ns):
             pos = int(p.findtext("ns:position", namespaces=ns))
             val = float(p.findtext("ns:quantity", namespaces=ns))
             series.append((start + (pos - 1) * step, val))

     # Return DataFrame if series found
     if series:
         df = pd.DataFrame(series, columns=["datetime", "MW"]).set_index("datetime").sort_index()
         return df

 raise ValueError("No <TimeSeries> data found in XML (unknown namespace)")


# =============================================================================
# FETCH FUNCTION
# =============================================================================
def fetch_load(eic, process, start, end):
 """
 Fetch Total Load (A65) data for one bidding zone and process type.

 Args:
     eic (str): EIC code of the bidding zone
     process (str): 'A01' = Day Ahead, 'A16' = Actual
     start (pd.Timestamp): Start date
     end (pd.Timestamp): End date

 Returns:
     pd.DataFrame: Time series of load values
 """
 params = {
     "documentType": "A65",
     "processType": process,
     "outBiddingZone_Domain": eic,
 }

 # Use the lower-level entsoe-py API call for full XML access
 response = client._base_request(params=params, start=start, end=end)
 xml_text = response.text

 if "<TimeSeries>" not in xml_text:
     raise ValueError("No <TimeSeries> data in XML")

 return parse_total_load(xml_text)


# =============================================================================
# MAIN EXECUTION LOOP
# =============================================================================
years = range(2014, 2025)
all_data = []

for zone, eic in nordic_bzn_eic.items():
 print(f"\n=== Fetching data for {zone} ({eic}) ===")

 for year in years:
     start = pd.Timestamp(f"{year}-01-01T00:00Z")
     end   = pd.Timestamp(f"{year}-12-31T23:00Z")

     # ---- Fetch Day-Ahead Forecast ----
     try:
         df_fc = fetch_load(eic, "A01", start, end).rename(columns={"MW": "DayAhead"})
     except Exception as e:
         print(f"⚠️  Day-Ahead forecast failed for {zone} {year}: {e}")
         df_fc = pd.DataFrame()

     # ---- Fetch Actual Load ----
     try:
         df_act = fetch_load(eic, "A16", start, end).rename(columns={"MW": "Actual"})
     except Exception as e:
         print(f"⚠️  Actual load failed for {zone} {year}: {e}")
         df_act = pd.DataFrame()

     # ---- Combine Forecast + Actual ----
     if not df_fc.empty or not df_act.empty:
         df = pd.concat([df_fc, df_act], axis=1)
         df["bzn"] = zone
         all_data.append(df)
         print(f"✅ {zone} {year}: {len(df)} rows collected")
     else:
         print(f"⚠️  No data for {zone} {year}")

     time.sleep(1)  # Be polite to API rate limits


# =============================================================================
# COMBINE AND SAVE OUTPUT
# =============================================================================
if all_data:
 combined = pd.concat(all_data)
 combined = combined.reset_index()[["bzn", "datetime", "DayAhead", "Actual"]]
 combined.to_csv(output_file, index=False, encoding="utf-8-sig")

 print(f"\n🎉 All data successfully saved to: {output_file}")
 print(f"📊 Total rows: {len(combined)}")
else:
 print("\n⚠️ No data fetched at all — please check API connection or parameters.")
