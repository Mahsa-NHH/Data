"""
====================================================================
⚡ ENTSO-E Nordic Generation Downloader (2014–2025 CSV Batch)
====================================================================

📘 DESCRIPTION:
This Python script downloads hourly *Actual Generation per Unit* (A73)
data from the ENTSO-E Transparency Platform API for the Nordic countries:
Denmark (DK), Finland (FI), Norway (NO), and Sweden (SE).

The data covers power generation per plant and production type, fetched
for each month between **2014 and 2025**.

The script automatically:
 - Handles known ENTSO-E API issues (e.g. 'Acknowledgement' responses)
 - Retries failed connections with exponential backoff
 - Avoids entsoe-py `UnboundLocalError` crashes
 - Falls back to XML parsing when structured data is missing
 - Saves one clean, tidy CSV file per month

🧾 OUTPUT FILES:
Each month is saved as:
    A73_Nordic_Filled_Month_YYYY-MM.csv
Columns:
    datetime | country | Type | Generation Unit | generation_MW

🧠 AUTHOR:
    Name: Mahsa Gorji
    Date: October 2025
    Project: DEEP
====================================================================
"""

# =============================================================================
# Imports
# =============================================================================

import os
import time
import copy
import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
from entsoe import EntsoePandasClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# =============================================================================
# Configuration
# =============================================================================

# Your ENTSO-E API key
API_KEY = "0e618bc9-1f36-4186-b70c-b2518618dcaf"

# ENTSO-E base API URL
BASE_URL = "https://web-api.tp.entsoe.eu/api"

# Initialize client
client = EntsoePandasClient(api_key=API_KEY)

# Output directory where CSVs are saved
output_dir = r"C:\Users\s15832\Documents\Project\Data\entsoe"
os.makedirs(output_dir, exist_ok=True)

# Logging setup to record warnings/errors in a debug log file
logging.basicConfig(
    filename=os.path.join(output_dir, "entsoe_debug.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Nordic control area EIC codes
nordic_cta = {
    "DK": "10Y1001A1001A796",  # Denmark
    "FI": "10YFI-1--------U",  # Finland
    "NO": "10YNO-0--------C",  # Norway
    "SE": "10YSE-1--------K"   # Sweden
}

# Define the year range to fetch data
YEARS = list(range(2014, 2026))  # <-- Fetch 2014 through 2025 inclusive


# =============================================================================
# Utility Functions
# =============================================================================

def safe_get(url, params, retries=3, backoff=5):
    """
    HTTP GET wrapper with retry & exponential backoff.
    Handles 'Acknowledgement' XML responses (400 errors) from ENTSO-E,
    which indicate no data is available for that date range.
    """
    for i in range(retries):
        try:
            req_params = copy.deepcopy(params)
            # Fix known typo in API responses
            if "perriodEnd" in req_params:
                req_params["periodEnd"] = req_params.pop("perriodEnd")

            r = requests.get(url, params=req_params, timeout=30)
            r.raise_for_status()
            return r

        except requests.exceptions.RequestException as e:
            # Handle 400 'Acknowledgement' responses gracefully
            if hasattr(e, "response") and e.response is not None and e.response.status_code == 400:
                text = e.response.text if hasattr(e.response, "text") else ""
                if "<Acknowledgement_MarketDocument" in text:
                    print("⚠️ ENTSO-E acknowledgement (no data). Skipping retries.")
                    logging.info("ENTSO-E acknowledgement received — no data for this day.")
                    return e.response

            # Retry with exponential delay
            wait = backoff * (2 ** i)
            print(f"⚠️ Connection error: {e} — retrying in {wait}s")
            time.sleep(wait)

    logging.error("Permanent connection failure after retries")
    return None


def strip_ns(tag):
    """Remove XML namespace from a tag name."""
    return tag.split('}', 1)[1] if '}' in tag else tag


def parse_units_from_xml(xml_text):
    """
    Parse XML returned from ENTSO-E and extract generation units and types.
    If the XML contains an 'Acknowledgement' document, return an empty DataFrame.
    """
    if "<Acknowledgement_MarketDocument" in xml_text:
        logging.info("ENTSO-E acknowledgement XML detected — returning empty unit list.")
        return pd.DataFrame(columns=["Type", "Generation Unit"])

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logging.error(f"XML parse error: {e}")
        return pd.DataFrame(columns=["Type", "Generation Unit"])

    units = []
    for rr in root.iter():
        if strip_ns(rr.tag) == "RegisteredResource":
            name, gen_type = None, None
            for c in rr.iter():
                tag = strip_ns(c.tag)
                if tag == "name":
                    name = c.text
                elif tag == "productionType":
                    gen_type = c.text
            if name or gen_type:
                units.append({
                    "Type": gen_type or "Unknown",
                    "Generation Unit": name or "Unknown"
                })
    return pd.DataFrame(units).drop_duplicates().reset_index(drop=True)


def build_filled_frame(df_units, cta, s, e):
    """
    Create an hourly table filled with NaN generation values
    when no data is available for that period.
    """
    hours = pd.date_range(start=s, end=e, freq="h")[:-1]
    if df_units.empty:
        df_units = pd.DataFrame({"Type": [np.nan], "Generation Unit": [np.nan]})
    df_expanded = pd.concat([
        df_units.assign(datetime=h, country=cta, generation_MW=np.nan)
        for h in hours
    ], ignore_index=True)
    return df_expanded


# =============================================================================
# Safe Entsoe Query Wrapper
# =============================================================================

def safe_entsoe_query(client, code, start, end):
    """
    Wrapper for EntsoePandasClient.query_generation_per_plant().
    Ensures that invalid responses (dicts, strings, corrupted DataFrames)
    do not cause crashes.
    """
    try:
        df = client.query_generation_per_plant(code, start=start, end=end)
        if not isinstance(df, pd.DataFrame):
            logging.warning(f"ENTSO-E returned non-DataFrame type: {type(df)}")
            return None
        if any(df.astype(str).apply(lambda x: x.str.contains("UnboundLocalError", case=False, na=False)).any()):
            logging.warning("ENTSO-E DataFrame contains UnboundLocalError text → discarding")
            return None
        if df.empty:
            return None
        return df
    except Exception as e:
        logging.warning(f"safe_entsoe_query failed: {e}")
        return None


# =============================================================================
# Core Data Fetch Logic
# =============================================================================

def fetch_day(cta, code, date, session, unit_cache):
    """
    Fetch one day of A73 generation data for a given country.
    If numeric data retrieval fails, fall back to XML metadata parsing.
    """
    s = date
    e = date + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    # Try numeric data via entsoe-py client first
    df = safe_entsoe_query(client, code, s, e)

    if df is not None:
        try:
            # Flatten multi-index columns if present
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [" | ".join([x for x in col if x]).strip() for col in df.columns.values]
            df_reset = df.reset_index().melt(
                id_vars=["index"], var_name="unit_info", value_name="generation_MW"
            )
            df_reset.rename(columns={"index": "datetime"}, inplace=True)

            # Split "unit_info" into Type and Generation Unit
            split_cols = df_reset["unit_info"].str.split(" \| ", n=1, expand=True)
            df_reset["Type"] = split_cols[0].str.replace("Actual Aggregated", "").str.strip()
            df_reset["Generation Unit"] = split_cols[1].fillna("Unknown")
            df_reset["country"] = cta
            df_reset["generation_MW"] = pd.to_numeric(df_reset["generation_MW"], errors="coerce")
            df_reset["datetime"] = pd.to_datetime(df_reset["datetime"])

            # Cache unique unit list for this country
            unit_cache[cta] = df_reset[["Type", "Generation Unit"]].drop_duplicates().reset_index(drop=True)
            return df_reset
        except Exception as e:
            logging.error(f"{cta} {s.date()} reshaping failed: {e}")

    # Fallback: XML parsing when numeric data missing
    try:
        if cta in unit_cache and not unit_cache[cta].empty:
            df_units = unit_cache[cta].copy()
        else:
            params = {
                "securityToken": API_KEY,
                "documentType": "A73",
                "processType": "A16",
                "in_Domain": code,
                "periodStart": s.strftime("%Y%m%d%H%M"),
                "periodEnd": (e + pd.Timedelta(hours=1)).strftime("%Y%m%d%H%M")
            }
            r = safe_get(BASE_URL, params)
            if r and r.status_code == 200:
                df_units = parse_units_from_xml(r.text)
                if not df_units.empty:
                    unit_cache[cta] = df_units.copy()
            else:
                df_units = pd.DataFrame(columns=["Type", "Generation Unit"])

        return build_filled_frame(df_units, cta, s, e)
    except Exception as e:
        logging.error(f"{cta} {s.date()} XML fallback failed: {e}")
        df_units = pd.DataFrame({"Type": [np.nan], "Generation Unit": [np.nan]})
        return build_filled_frame(df_units, cta, s, e)


def fetch_country(cta, code, start, end):
    """
    Fetch all daily data for a single country between two dates.
    Uses multi-threading to speed up daily requests.
    """
    print(f"\n→ Fetching {cta} ({code})")
    session = requests.Session()
    unit_cache = {}
    all_days = pd.date_range(start=start, end=end, freq="D")
    frames = []

    # Threaded daily fetching
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(fetch_day, cta, code, d, session, unit_cache): d for d in all_days}
        for f in as_completed(futures):
            day = futures[f]
            try:
                df = f.result()
                if isinstance(df, pd.DataFrame):
                    frames.append(df)
                    print(f"   ✅ {cta} {day.date()} — {len(df)} rows")
                else:
                    print(f"   ⚠️ {cta} {day.date()} returned non-DataFrame, skipping")
            except Exception as e:
                print(f"   ❌ {cta} {day.date()} failed: {e}")
                logging.error(f"{cta} {day.date()} failed: {e}")

    session.close()

    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        df_units = pd.DataFrame({"Type": [np.nan], "Generation Unit": [np.nan]})
        return build_filled_frame(df_units, cta, start, end)


# =============================================================================
# Multi-Year and Multi-Month Fetch Loop
# =============================================================================

for year in YEARS:
    for month in range(1, 13):
        # Define the month range (start and end timestamps)
        month_start = pd.Timestamp(f"{year}-{month:02d}-01T00:00Z")
        month_end = month_start + pd.offsets.MonthEnd(0) + pd.Timedelta(hours=23, minutes=59)
        print(f"\n📅 ===== Fetching A73 for {month_start.date()} – {month_end.date()} =====")

        final_frames = []

        # Fetch data for all four Nordic countries concurrently
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_country, cta, code, month_start, month_end): cta for cta, code in nordic_cta.items()}
            for f in as_completed(futures):
                cta = futures[f]
                try:
                    df = f.result()
                    final_frames.append(df)
                    print(f"📊 {cta} done ({len(df)} rows)")
                except Exception as e:
                    print(f"❌ {cta} failed: {e}")
                    logging.error(f"{cta} failed: {e}")

        # Save combined monthly dataset to CSV
        if final_frames:
            combined = pd.concat(final_frames, ignore_index=True)
            combined = combined[["datetime", "country", "Type", "Generation Unit", "generation_MW"]]
            combined.sort_values(by=["datetime", "country", "Type", "Generation Unit"], inplace=True)
            file_path = os.path.join(output_dir, f"A73_Nordic_Filled_Month_{year}-{month:02d}.csv")
            combined.to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"\n💾 Saved monthly CSV file:\n{file_path}")
            print(f"📊 Total rows: {len(combined)}")
        else:
            print(f"\n⚠️ No A73 data fetched for {year}-{month:02d}.")
