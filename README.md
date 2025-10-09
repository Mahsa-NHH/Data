#get_airquality_measures.py
Brief description (what changed & why):

Portable output paths: replaced hard-coded E:/ with pick_store_dir() fallbacks → runs on any machine.

Robust networking: single requests.Session() + default timeout + retry with exponential backoff/jitter → fewer hangs, resilient to transient errors.

Structured logging: swapped print for logging with timestamps; log station id + name and per-station duration → easier to monitor/debug.

Safer parsing: UTC time conversion and defensive drops of fromTime/toTime → consistent timestamps, fewer key errors.

Checkpointing: write raw/measurements__.csv.gz and skip if exists → resumable runs, no duplicate work.

Reliable aggregation: rebuild measurements.csv (chunked) and measurements.pq from all checkpoints at the end → same final outputs, lower memory use.

Docs: added clear header explaining purpose, outputs, and behavior → future maintenance is simpler.

########################################################################

#npra_download.py
This introduces npra_client.py as the shared library that replaces the
duplicated core logic in:
- get_traffic_measures.py
- get_traffic_measures_addparts.py

What’s in:
- Portable storage selection (pick_store_dir) with sensible fallbacks
- Reusable HTTP session and post_with_retries (timeout + backoff + jitter)
- GraphQL builders: gql_all_points(), gql_by_hour()
- Station metadata helpers: fetch_all_stations(), load_stations()
- Row normalizers:
  - normalize_total_row() -> id,time,volume,coverage
  - normalize_length_rows() -> id,time,length,volume,coverage (fills missing bins)
- Time/window helpers: iter_100h_windows(), to_iso_plus0100()
- CSV helpers: ensure_csv_with_header(), append_lines()
- Constants: VV_API, LENGTH_CATEGORIES, DEFAULT_TIMEOUT, MAX_RETRIES

Why:
- Removes copy-paste logic between the two original scripts
- Improves reliability (robust retries), readability, and testability
- Keeps the exact output schemas expected downstream

Notes:
- Includes clear module docstring and inline comments for maintainability
- Designed to be consumed by the new CLI (npra_download.py)

###########################################################################

#npra_download.py
Adds npra_download.py as the command-line entry point that replaces and
unifies the workflows of:
- get_traffic_measures.py  (initial metadata + full download)
- get_traffic_measures_addparts.py  (resume/append)

Commands:
- --fetch-stations     -> writes trafficregpoints.csv (station metadata)
- --download-all       -> iterates all stations, downloads hourly data in 100h windows,
                          appends to aggvol.csv and lengthvol.csv
- --resume --start-index N
                        -> continues from station index N (same semantics as idlist[N:])

Improvements over the originals:
- Single source of truth (shares logic via npra_client.py)
- Robust networking with retries/backoff instead of assert-based checks
- Clear, timestamped logging; progress per station and window
- Append-safe CSV I/O; headers ensured automatically
- Portable output directory (override with --store-dir)
- Exact same output files and schemas:
  - trafficregpoints.csv
  - aggvol.csv (id,time,volume,coverage)
  - lengthvol.csv (id,time,length,volume,coverage)

Why:
- Eliminates divergence between two similar scripts
- Easier to run (one file, clear flags) and easier to resume
- Safer and more observable long-running downloads

Usage examples:
  python npra_download.py --fetch-stations
  python npra_download.py --download-all
  python npra_download.py --resume --start-index 4088
  python npra_download.py --download-all --store-dir "D:/mydata/traffic"

Follow-up:
- Consider deprecating/removing the original scripts and updating README to point to this CLI.
########################################################################################
#get_municipality_population_ssb_api.py
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
########################################################################################
#get_ssb_cpi.py
"""
author: morten
date: 2025-02-01

Get data from SSB API or table downloads

1. Obtain monthly CPI from 1920 to 2024
Table 08981: Konsumprisindeks, historisk serie, etter måned (2015=100) 1920 - 2024

2. Obtain yearly CPI from 1920 to 2024
Table 08184: Konsumprisindeks, historisk serie (2015=100) 1865 - 2024
"""
########################################################################################
#get_ssb_data_api.py
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
########################################################################################