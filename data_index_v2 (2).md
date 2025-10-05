**NPRA**
| **ID**              | **Name**                               | **Definitions**                                                   |
| ------------------- | -------------------------------------- | ----------------------------------------------------------------- |
| DS-1                | npra                                   | Norwegian Public Roads Administration (NPRA) vehicle & aux data.  |
| DS-1.1              | 2015                                   | Earlier extract of registration history + technical files.        |
| DS-1.1.1            | carhistory.csv                         | NPRA vehicle registration/ownership history (event-level).        |
| DS-1.1.2            | cartekn.csv                            | Per-vehicle technical attributes.                                 |
| DS-1.1.3            | cartypg.csv                            | Type-approval specifications (variant-level).                     |
| DS-1.1.4            | carutek.csv                            | Per-vehicle registration snapshot (key dates/IDs).                |
| DS-1.2              | odometer                               | Odometer (mileage) readings.                                      |
| DS-1.2.1–DS-1.2.11  | Kmstand-XXXX-YYYY.csv                  | Odometer readings from roadworthiness tests for years XXXX–YYYY.  |
| DS-1.2.12–DS-1.2.15 | Kmstand-XXXX.csv                       | Single-year odometer files.                                       |
| DS-1.2.16           | odometer.csv                           | Merge of separate odometer files.                                 |
| DS-1.3              | hhs_format_description.odt             | HHS fixed-width format spec for NPRA events.                      |
| DS-1.4–DS-1.6       | hhs0X.txt                              | NPRA event history files; see HHS spec.                           |
| DS-1.7              | hss-format.txt                         | *                                                                 |
| DS-1.8              | hss-kjøretøy gruppe.txt                | Vehicle-group code list (NO).                                     |
| DS-1.9              | Kilometerstand-PKK.zip                 | Odometer readings (archival package).                             |
| DS-1.10             | muni_lr_routedata.gpkg                 | Route polylines per municipality.                                 |
| DS-1.11             | muni_route_tollroadprice.csv           | Monthly toll-road price per municipality.                         |
| DS-1.12             | odontometer_reading_1997_2015.csv      | Odometer readings (row-level).                                    |
| DS-1.13             | ownermuni.csv                          | Owner counts per municipality.                                    |
| DS-1.14             | ownermunimap.csv                       | NPRA→standard municipality code crosswalk.                        |
| DS-1.15             | REG_brandcodes.csv                     | Brand code→name mapping.                                          |
| DS-1.16             | registreringsaker.csv                  | Registration decision events.                                     |
| DS-1.17             | REGTEK_data_06.03.2022.dsv             | Motor-vehicle registry snapshot (as of date).                     |
| DS-1.18             | REGTEK_data_25.02.2025.dsv             | *                                                                 |
| DS-1.19             | REGTEK_data_codes_descriptions_NO.xlsx | Registry code value descriptions (NO).                            |
| DS-1.20             | REGTEK_data_field_descriptions_NO.xlsx | Registry field descriptions (NO).                                 |
| DS-1.21             | tekninfo.txt                           | Technical info (delivered with HHS files).                        |
| DS-1.22             | typginfo.txt                           | Type-approval info (aux).                                         |
| DS-1.23             | utekinfo.txt                           | Vehicle technical info (aux).                                     |
| DS-1.24             | vehicle_group_descriptions.docx        | Vehicle-group descriptions (EN).                                  |


**NPRA — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|

| DS-1.1.1 | npra | carhistory.csv | `npra/2015/carhistory.csv` | NPRA | Buydate: 1911-11-11 to 2015-12-17; Regdate: 1980-01-03 to 2015-12-17 | One vehicle registration event per row (licensenumber + regdate) | CSV, 38,577,882 rows × 13 cols, 3408.63 MB | archived | `scripts\load_hhs_cardata.py` | licensenumber = plate number; oxid = record ID; ownerid = owner ID; regtype = registration type; buydate = date (buy); regdate = date (registration); fnronr = record/event number; ownername = owner name; address = owner address; postalnumber = owner postal code; vehiclegroup = vehicle category; ownertype = owner type; previouslicensenumber = previous plate; NPRA vehicle registration history — one row per registration/ownership event (by licensenumber), with dates (buy/reg), event code, owner info, and vehicle group.|

| DS-1.1.2 | npra | cartekn.csv | `npra/2015/cartekn.csv` | NPRA | Datefirstregnorway: 1901-01-01 to 2015-11-30; Deregisterdate: 1932-02-26 to 2015-11-30; Lasteucontroldate: 1994-09-19 to 2015-11-30; Nexteucontroldate: 1994-12-31 to 2021-10-31; Lastregdate: 1919-05-19 to 2015-11-30 | Vehicle (one row per license number) | CSV, 1,725,207 rows × 52 cols, 324.26 MB | archived | `scripts\load_hhs_cardata.py` | Per-vehicle technical attributes from NPRA — one row per license number/chassis, with make/model codes, engine & displacement/fuel, dimensions/weights, EU control dates, emissions, seats; covers from first registration to last/deregistration.

| DS-1.1.3 | npra | cartypg.csv | `npra/2015/cartypg.csv` | NPRA | Approvalnumber: 1970-01-01 to 2030-01-01; Approvalyear: 1901-01-01 to 2015-01-01 | Type-approval spec (approvalnumber × variant × type) | CSV, 162,836 rows × 53 cols, 49.00 MB | archived | `scripts\load_hhs_cardata.py` | NPRA type-approval specs: one row per vehicle variant with technical details (make/model codes, engine, dimensions/weights, CO₂, seats) and approval dates.

| DS-1.1.4 | npra | carutek.csv | `npra/2015/carutek.csv` | NPRA | Datefirstregnorway: 1901-07-05 to 2015-11-30; Deregisterdate: 1973-01-19 to 2015-11-30; Lasteucontroldate: 1994-11-03 to 2015-11-30; Nexteucontroldate: 1996-02-24 to 2019-10-31; Lastregdate: 1964-01-08 to 2015-11-30; Datefirstregnorwaymonth: TBD(To Be Determined) atefirstregnorwayday: TBD to TBD | Vehicle (one row per license number) | CSV, 4,756,058 rows × 13 cols, 381.27 MB | archived | `scripts\load_hhs_cardata.py` | licensenumber = plate number; Per-vehicle registration snapshot with key fields: license & chassis IDs, model year, type-approval ID, first-registration date, import flag, color, EU control dates, last/deregistration dates. 

| DS-1.2.1 | npra | kmstand-1997-2000.csv | `npra/odometer/kmstand-1997-2000.csv` | NPRA | 1997-2000 | Row-level records | CSV, 1,800,601 rows × 4 cols, 76.45 MB | Raw | TBD | KJENNEMERKE → license plate / registration mark (≈ licensenumber), UNDERSTELLSNUMMER → chassis number / VIN (≈ undercarriagenumber), KILOMETERSTAND → odometer reading in kilometers (≈ kmstand / mileage), KONTROLLDATO → inspection date (roadworthiness test “EU-kontroll”; ≈ kontrolldato) |

| DS-1.2.2| npra | kmstand-2000-2002.csv | `npra/odometer/kmstand-2000-2002.csv` | NPRA | 2000-2002 | Row-level records | CSV, 1,991,830 rows × 4 cols, 85.33 MB | Raw | TBD | * (the same) |

| DS-1.2.3 | npra | kmstand-2002-2004.csv | `npra/odometer/kmstand-2002-2004.csv` | NPRA | 2002-2004 | Row-level records | CSV, 2,141,338 rows × 4 cols, 92.11 MB | Raw | TBD | * |

| DS-1.2.4 | npra | kmstand-2004-2006.csv | `npra/odometer/kmstand-2004-2006.csv` | NPRA | 2004-2006 | Row-level records | CSV, 2,216,547 rows × 4 cols, 95.64 MB | Raw | TBD | * |

| DS-1.2.5 | npra | kmstand-2006-2008.csv | `npra/odometer/kmstand-2006-2008.csv` | NPRA | 2006-2008 | Row-level records | CSV, 2,274,374 rows × 4 cols, 98.20 MB | Raw | TBD | * |

| DS-1.2.6 | npra | kmstand-2008-2010.csv | `npra/odometer/kmstand-2008-2010.csv` | NPRA | 2008-2010 | Row-level records | CSV, 2,351,518 rows × 4 cols, 101.65 MB | Raw | TBD | * |

| DS-1.2.7 | npra | kmstand-2010-2012.csv | `npra/odometer/kmstand-2010-2012.csv` | NPRA | 2010-2012 | Row-level records | CSV, 2,544,989 rows × 4 cols, 110.15 MB | Raw | TBD | * |

| DS-1.2.8 | npra | kmstand-2012-2014.csv | `npra/odometer/kmstand-2012-2014.csv` | NPRA | 2012-2014 | Row-level records | CSV, 2,613,460 rows × 4 cols, 113.08 MB | Raw | TBD | * |

| DS-1.2.9 | npra | kmstand-2014-2016.csv | `npra/odometer/kmstand-2014-2016.csv` | NPRA | 2014-2016 | Row-level records | CSV, 2,698,113 rows × 4 cols, 116.89 MB | Raw | TBD | * |

| DS-1.2.10 | npra | kmstand-2016-2018.csv | `npra/odometer/kmstand-2016-2018.csv` | NPRA | 2016-2018 | Row-level records | CSV, 2,763,447 rows × 4 cols, 119.78 MB | Raw | TBD | * |

| DS-1.2.11 | npra | kmstand-2018-2020.csv | `npra/odometer/kmstand-2018-2020.csv` | NPRA | 2018-2020 | Row-level records | CSV, 2,877,526 rows × 4 cols, 124.76 MB | Raw | TBD | * |

| DS-1.2.12 | npra | kmstand-2020.csv | `npra/odometer/kmstand-2020.csv` | NPRA | 2020 | Row-level records | CSV, 1,467,350 rows × 4 cols, 65.02 MB | Raw | TBD | * |

| DS-1.2.13 | npra | kmstand-2021.csv | `npra/odometer/kmstand-2021.csv` | NPRA | 2021 | Row-level records | CSV, 1,567,655 rows × 4 cols, 69.49 MB | Raw | TBD | * |

| DS-1.2.14 | npra | kmstand-2022.csv | `npra/odometer/kmstand-2022.csv` | NPRA | 2022 | Row-level records | CSV, 1,525,246 rows × 4 cols, 67.59 MB | Raw | TBD | * |

| DS-1.2.15 | npra | kmstand-2023.csv | `npra/odometer/kmstand-2023.csv` | NPRA | 2023 | Row-level records | CSV, 1,611,168 rows × 4 cols, 71.46 MB | Raw | TBD | * |

| DS-1.2.16 | npra | odometer.csv | `npra/odometer/odometer.csv` | NPRA | Date: 1997-01-06 to 1998-05-08 | Row-level records | CSV, 32,993,655 rows × 3 cols, 864.33 MB | Derived/Merged | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\full_odometer.py`, `scripts\load_odometer_data.py/`, `scripts\odometer_load.py/` | regnr = plate; date = reading date; km = odometer (km)

| DS-1.3 | npra | hhs_format_description.odt | `npra/hhs_format_description.odt`| Statens vegvesen (SVV) vehicle registry, converted to HHS format  | - | One record per registration/ownership event per registration number (sequenced by `ut-lnr`)  | ODT, \~36.5 KB  | Specification/Documentation | - | Specification of a fixed-width text file containing Norwegian vehicle registration/ownership events from Statens vegvesen (SVV), adapted to the HHS format. Lists record layout, field names, types, and meanings. |

| DS-1.4 | npra | hhs01.txt | `npra/hhs01.txt` | * | * | Vehicle event (license plate × event code/seq) | TXT, 13,222,035 rows × 1 cols, 1614.02 MB | Raw | - | registration events history over a long period until 2018(?) in three separate text files. Description in hhs_format_description |

| DS-1.5 | npra | hhs02.txt | `npra/hhs02.txt` | * | * | * | TXT, 14,886,157 rows × 1 cols, 1817.16 MB | Raw | - | * |

| DS-1.6 | npra | hhs03.txt | `npra/hhs03.txt` | * | * | * | TXT, 14,927,216 rows × 1 cols, 1822.05 MB | Raw | - | * |

| DS-1.7 | npra | hss-format.txt | `npra/hss-format.txt` | SVV data delivered to HHS | * | * | Plain text (.txt), fixed-width layout (COBOL PIC X/9 notation); 3 KB | Raw | - | Fixed-width schema/spec for SVV→HHS vehicle registration/ownership events—lists field names, lengths (PIC X/9), event codes (e.g., 1G, BI, NY, AV, VR), and date semantics for parsing the data file. |

| DS-1.8 | npra | hss-kjoretøy gruppe.txt | `npra/hss-kjoretøy gruppe.txt` | Internal Norwegian code list mapping group IDs to descriptions  | - | Row-level records | TXT, 44 rows × 1 cols, 4 KB| Reference / Codebook | - | Reference codebook mapping HHS vehicle-group codes to Norwegian descriptions, with notes on applicability; used to interpret ut-kjtgrp in HHS files. |

| DS-1.9 | npra | Kilometerstand-PKK.zip

| DS-1.10 | npra | muni_lr_routedata.gpkg | `npra/muni_lr_routedata.gpkg` | TBD | - | One LINESTRING feature per municipality (`munid`), with route ID and counts | GeoPackage (.gpkg), \~6.8 MB; 1 layer (`muni_lr_routedata`); 356 features; geometry=LINESTRING; CRS=EPSG:25833 (ETRS89/UTM 33N) | Intermediate Data | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_registration_data.py`,`scripts\prepare_datasets.py` | GeoPackage of municipality-level route polylines (one feature per `munid`) with route ID (`lrid`) and counts of toll-road (`ntollroad`) and ferry (`nferry`) segments; CRS EPSG:25833 |

| DS-1.11 | npra | muni_route_tollroadprice.csv | `npra/muni_route_tollroadprice.csv` | TBD | Aug 2000–Dec 2023 (281 months) | One record per municipality per month (205 municipalities) | CSV (\~727 KB), 33,233 rows, 3 cols | Intermediate Data | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_additional_data.py`, `scripts\odometer_load.py`  | Monthly toll-road price time series per municipality, calculated tollroad price in NOK per route traversal for each municipality and month. Route is calculated based on plausible work route for each municipality

| DS-1.12 | npra | odontometer_reading_1997_2015.csv | `npra/odontometer_reading_1997_2015.csv` | NPRA | 1997-12-02 to 1999-09-01 | Row-level records | CSV, 20,172,086 rows × 4 cols, 1250.44 MB | Raw | - | Row-level vehicle odometer readings for Norway; each record includes timestamp, license plate, odometer value (km), and a unique vehicle/owner ID.|

| DS-1.13 | npra | ownermuni.csv | `npra/ownermuni.csv` | NPRA | - | One row per owner municipality code (`EIER_KOMMUNE_NUMMER`) with total `count` | CSV (\~5.6 KB), 444 rows × 2 cols | Raw | - | Aggregated counts of vehicle owners per Norwegian municipality, number of owners in motor vehicle registry for each municipality code. Municipality codes are not all from same version |

| DS-1.14 | npra | ownermunimap.csv |`npra/ownermunimap.csv`| NPRA | - | One row per NPRA municipality code mapped to a standard `munid` | CSV (\~4.3 KB), 438 rows × 2 cols | TBD | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_registration_data.py`, `scripts\odometer_load.py`  | Crosswalk mapping NPRA municipality codes (`munid_npra`) to standardized municipality IDs (`munid`) for harmonizing across merger, mapping from municipality codes in motor vehicle registry to the 2020 municipality codes|

| DS-1.15 | npra | REG_brandcodes.csv | `npra/REG_brandcodes.csv` | NPRA | - | One row per brand code | CSV, 2 cols (`kode`, `navn`); \~29 KB; 2,150 rows | Reference mapping | `scripts\read_regdata.py` | Code list mapping vehicle brand codes (`kode`) to brand names (`navn`), mapping from brand codes in motor vehicle registry to brand names |

| DS-1.16 | npra | registreringsaker.csv | `npra/registreringsaker.csv` | NPRA | - | Row-level records | CSV, 6,392,618 rows × 16 cols, 1070.00 MB | Raw | `scripts\load_hhs_cardata.py` | Vehicle decision events (registration, ownership change, temp dereg.) per plate, with date/type, case, actor (person/org), and vehicle group. |

| DS-1.17 | npra | REGTEK_data_06.03.2022.dsv | `npra/REGTEK_data_06.03.2022.dsv` | NPRA | - | Row-level records | DSV, 3,798,163 rows × 239 cols, 6208.47 MB | Raw | TBD | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\explore_registered_ownership.py` , `scripts\load_registration_data.py`, `scripts\odometer_load.py`, `scripts\read_regdata.py` |motor vehicle registry as of indicated date

| DS-1.18 | npra | REGTEK_data_25.02.2025.dsv | `npra/REGTEK_data_25.02.2025.dsv`| NPRA | - | Row-level records | DSV, 4,073,210 rows × 254 cols, 7046.51 MB | Raw | - | * |

| DS-1.19 | npra | REGTEK_data_codes_descriptions_NO.xlsx | `npra/REGTEK_data_codes_descriptions_NO.xlsx` | NPRA | - | One row per code per sheet | XLSX (\~29.3 KB), 7 sheets: `TEKNISK_KODE` (45×2), `TEKNISK_UNDERKODE` (39×2), `AVGIFT` (60×2), `DRIVSTOFFTYPE` (20×2), `FARGE` (16×3), `KJENNEMERKEFARGE` (9×3), `PABYGG` (65×2) | Reference codebook | - | description of code values for certain fields/columns in the motor vehicle registry (in Norwegian) |

| DS-1.20 | npra | REGTEK_data_field_descriptions_NO.xlsx | `npra/REGTEK_data_field_descriptions_NO.xlsx` | NPRA | - | One row per field (`Felt`) | XLSX (\~15.4 KB); 1 sheet (`Ark1`); 113 rows × 3 cols (Felt, Beskrivelse, Format) | Reference | - | description of the fields/columns in the motor vehicle registry (in Norwegian) |

| DS-1.21 | npra | tekninfo.txt | `npra/tekninfo.txt` | TBD | - | Row-level records | TXT, 2,394,188 rows × 1 cols, 879.06 MB | tech info | `scripts\load_hhs_cardata.py` | technical information on cars (delivered in 2018 together with hhs0X files) |

| DS-1.22 | npra | typginfo.txt | `npra/typginfo.txt` | TBD | - | Row-level records | TXT, 160,054 rows × 1 cols, 93.57 MB | tech info | `scripts\load_hhs_cardata.py` | * |

| DS-1.23 | npra | utekinfo.txt | `npra/utekinfo.txt` | TBD | - | Row-level records | TXT, 7,811,355 rows × 1 cols, 856.69 MB | tech info | `scripts\load_hhs_cardata.py` | * |

| DS-1.24 | npra | vehicle_group_descriptions.docx  | `npra/vehicle_group_descriptions.docx` | TBD  | DOCX, 17 kb| Reference codebook | - | Description of vehicle group codes in the motor vehicle registry (in English)

**electricity**
| **ID**   | **Name**                                             | **Definitions**                              |
| -------- | ---------------------------------------------------- | -------------------------------------------- |
| DS-2     | electricity                                          | Electricity market data.                     |
| DS-2.1   | electricity/entsoe                                   | ENTSO-E Transparency data.                   |
| DS-2.1.1 | electricity/entsoe/entsoe_area_codes.csv             | EIC area codes for Nord Pool bidding zones.  |
| DS-2.1.2 | electricity/entsoe/nordic_hourly_gen_prodtype.csv    | Nordic hourly generation by production type. |
| DS-2.2   | electricity/nordpool                                 | Nord Pool market data.                       |
| DS-2.2.1 | electricity/nordpool/Elspot_capacity                 | Elspot interconnector capacity (weekly).     |
| DS-2.2.2 | electricity/nordpool/Elspot_flow                     | Elspot cross-border flows (weekly).          |
| DS-2.2.3 | electricity/nordpool/Elspot_prices                   | System price (daily).                        |
| DS-2.2.4 | electricity/nordpool/Market_coupling_capacity        | Market-coupling capacity (ATC, weekly).      |
| DS-2.2.5 | electricity/nordpool/Market_coupling_flow            | Market-coupling flow (weekly).               |
| DS-2.2.6 | electricity/nordpool/mcp                             | Market-clearing price files (daily).         |
| DS-2.2.7 | electricity/nordpool/operating                       | Balancing/operating stats (weekly).          |
| DS-2.3   | electricity/NVE                                      | NVE hydrology & wind data.                   |
| DS-2.3.1 | electricity/NVE/hydrologi_magasin_13092024.xlsx      | Nordic hydrology (weekly, area).             |
| DS-2.3.2 | electricity/NVE/magasindata_per_mag.csv              | Reservoir levels per reservoir (weekly).     |
| DS-2.3.3 | electricity/NVE/wind2002_2023_perplant_utcplus1.xlsx | Hourly wind by plant (UTC+1).                |
| DS-2.3.4 | electricity/NVE/windprod_hourly_no_plant.csv         | Hourly wind (aggregated, NO).                |

**electricity — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-2.1.1 | electricity | entsoe_area_codes.csv | `electricity/entsoe/entsoe_area_codes.csv` | ENTSO-E | - | One row per bidding zone (e.g., NO1, SE3, DK2) | CSV, ~5.7 KB (5,795 bytes); 118 rows × 13 columns | Reference mapping | `scripts\create_entsoe_area_info.py`, `scripts\get_entsoe_data_api.py` | A reference CSV mapping Nord Pool bidding zones to their ENTSO-E EIC area codes. Use it to join Nord Pool data to standardized ENTSO-E identifiers for queries and aggregation.|

| DS-2.1.2 | electricity | nordic_hourly_gen_prodtype.csv | `electricity/entsoe/nordic_hourly_gen_prodtype.csv` | ENTSO-E | 2014-12-08 23:00 → 2026-09-12 15:00 | One row per area × production type × hour | CSV, 241.5 MB; 5,890,539 data rows + 1 header = 5,890,540 lines; 4 columns | Processed | `scripts\get_entsoe_data_api.py` | Hourly electricity generation in the Nordic region, broken out by production type (e.g., hydro, wind, nuclear, thermal) per area/country; typical columns: datetime, area/country, production_type, generation_mw (or mwh). |

| DS-2.2.1 | electricity | Elspot_capacity  | `electricity/nordpool/Elspot_capacity/YYYY/scapYYWW.sdv` | Nord Pool | 1999-2022 (weekly) each folder contains around 52 `sdv` files | One record per hour × directional bidding-zone link  | `.sdv` text | Raw  | - | Weekly Nord Pool Elspot “spot capacity” files (`scapYYWW.sdv`) giving hour-by-hour available transfer capacity between bidding areas and any capacity-reduction codes. Each semicolon-separated file contains blocks: ST (file/update meta), BE (area descriptions), UE (hourly capacities, MW), CR (hourly reduction reasons), and AL (line count). Contains folder 1999-2022 and 2014_24h: Daily 24-hour Elspot interconnector capacities & constraints for Nordic–Baltic bidding areas. |

| DS-2.2.1.1 | electricity | SCAP-eng.DOC | `electricity/nordpool/Elspot_capacity/SCAP-eng.DOC` | Nord Pool | - | - | DOC, 27 KB| Reference | - |  Describes governance and processes for cross-border capacity (allocation, nomination, curtailment), operational coordination, participant roles, and standard terms (bidding areas, constraints, interconnectors).|

| DS-2.2.2 | electricity | Elspot_flow | `electricity/nordpool/Elspot_flow/YYYY/sfloYYWW.sdv` | Nord Pool | 1999-2023 (weekly) each folder contains `sdv` files| Hourly values (1–24) per interconnector direction, daily rows within the week | `.sdv` text | Raw | — | File stores Elspot flow between countries with hourly values and a 24-hour sum; units are MWh/h; header includes field explanations (e.g., data type codes and hour columns 1–24). Contains folder 1999-2023 and 2014_24h: Weekly files with hourly cross-border flow (MW) per interconnector across 168 hours (24×7) for Nordic–Baltic bidding areas, semicolon-delimited with ST/BE/FE sections.|

| DS-2.2.2.1 | electricity | SFLO.doc | `electricity/nordpool/Elspot_flow/SFLO.doc` | Nord Pool | - | - | DOC, 45 KB | Reference | - | Record types per spec: ST (file header: Year, Week, Day, Hour, Tot.numb.hours=24, Unit, Clock, Date), BE (areas: Alias, Text), FE (flows: Code D=Elspot, S=Systemprice; Hour1–Hour24 + daily sum), AL (line count).|

| DS-2.2.3 | electricity | Elspot_prices | `electricity/nordpool/Elspot_prices/YYYY/syseurYY.sdv` | Nord Pool | 1999-2023 | daily rows | `.sdv`, 65 KB | Raw | - | Nord Pool Elspot system price, semicolon-delimited file with 24 hourly EUR/MWh values per day, plus daily average and a EUR→NOK reference rate. Contains folder 1999-2023 and 2014_24h: 2014 Nord Pool System Price — hourly EUR/MWh in .sdv; 24×365 table with standard Nord Pool headers (DST 23/25 hrs as applicable).|

| DS-2.2.4| electricity | Market_coupling_capacity | `electricity/nordpool/Market_coupling_capacity/YYYY/mcapYYWW.sdv` | Nord Pool | 2011-2022 | Hourly (24×7) per interconnector & direction | .sdv; semicolon-delimited text | Raw | - |ST = file meta; BE = bidding areas; UE = hourly available transfer capacity by interconnector/direction; CR = capacity reductions/constraints. Each year contans different weeks, Contains folder 2011-2022 and 2014_24h: Link = interconnector code (e.g., DK1_DE); Direction = left→right of code; Capacity = ATC in MW; hours H1–H24 in local time.|

| DS-2.2.5 | electricity | Market_coupling_flow | `electricity/nordpool/Market_coupling_flow/YYYY/mfloYYWW.sdv` | Nord Pool | 2011-2023, each folder contains different numbers of weeks | Link × Direction × Day × time-bin (24×7; interval columns per day)| `.sdv` (semicolon-delimited) | Raw | - | Link: interconnector ID; Direction: signed flow (MW) by link orientation; Hour: CET timestamps (winter, UTC+1). Contains folder 2011-2023 and 2014_24h | 

| DS-2.2.6.1 | electricity | mcp.zip | `electricity/nordpool/mcp/mcp.zip/mcp_YMD` | Nord Pool | 2014-01-01 → 2023-06-30 (CET/CEST) | Hourly (24 rows; 23/25 on DST) | XLS; 1 file/day; cols: Hour, System price, bidding-area prices (EUR/MWh) | Raw | - | MCP = market-clearing price; areas as applicable by date (e.g., DK1, DK2, FI, NO1–NO5, SE1–SE4, EE/LV/LT); timestamps in local market time, hourly bid curves, one excel file per day in zip file. |

| DS-2.2.6.2 | electricity | mcp_2010_2013.zip | `electricity/nordpool/mcp/mcp_2010_2013.zip/YYYY/MM/Bid_ask_curves-YYYY-MM-DD-1-24.xls or MCP_Data_Report_DD-MM-YYYY` | Nord Pool | 2010-01-01 → 2013-12-31 (daily, H1–H24) | Area × Hour (CET/CEST) × price–quantity steps | XLS | Raw | - | Bid/Ask curves: The inputs — aggregated supply and demand curves (price–volume pairs) used to determine the MCP. Aggregated demand (bid) & supply (ask) curves; clearing where bid=ask → MCP & cleared MWh. MCP report: The outcome — cleared market price (and often cleared volume) for each hour after auction, Contains folder 2010_2013 which have different months. |

| DS-2.2.7.1 | electricity | podk | `electricity/nordpool/operating/podk/YYYY/podkYYWW.sdv` | Nord Pool | 1999-2023 | Likely hourly 24×7; zone=DK | `.sdv` | Raw | `scripts\prepare_nordpool_operating_data.py` | Weekly, semicolon-separated Denmark balancing-market file containing hourly values (1–24) for regulation prices (up/down, imbalance), consumption/production, and interconnector exchange, plus metadata (year/week/day, currency, update time) and line count, Contains folder 1999-2023 and 2014_24h |

| DS-2.2.7.1.1 | electricity | Podk_description.doc | `electricity/nordpool/operating/podk/Podk_description.doc` | Nord Pool | - | - | `.doc`, \~28 KB | Reference | - | Describes weekly PODK files `podkYYYYWW.sdv`: ST (meta/time), BE (areas), PR/OM/FB/PS/UT hourly series with codes RN, RO, RC, RP, RS, DD, EH, F, E, P, PE, WE, WS, U; units: DKK/MWh and MWh/h; AL (line count). |

| DS-2.2.7.2 | electricity | pofi | `electricity/nordpool/operating/pofi/YYYY/pofiYYWW.sdv` | Nord Pool | 1998-2023 | Likely hourly 24×7; zone=FI | `.sdv` | Raw | `scripts\prepare_nordpool_operating_data.py` | Weekly, semicolon-separated Finland balancing-market file containing hourly values (1–24) for regulation prices (up/down, imbalance), consumption/production, and interconnector exchange, plus metadata (year/week/day, currency, update time) and line count, Contains folder 1998-2023 and 2014_24h |

| DS-2.2.7.2.1 | electricity | Pofi_description.doc | `electricity/nordpool/operating/pofi/Pofi_description.doc` | Nord Pool | - | - | `.doc`, \~28 KB | Reference | - | Defines the semicolon-separated weekly Finland files pofiYYWW.sdv. Sections: ST (file/time metadata), BE (area alias + text), hourly series PR/OM/FB/PS/UT with codes RN, RO, RC, RP, RS, DD, F, E, P, PE, U (prices in EUR/MWh; others in MWh/h, integers), and AL (total line count).|

| DS-2.2.7.3 | electricity | pono | `electricity/nordpool/operating/pono/YYYY/ponoYYWW.sdv` | Nord Pool | 1996-2023 | Likely hourly 24×7; zone=NO | `.sdv` | Raw | `scripts\prepare_nordpool_operating_data.py` | Weekly, semicolon-separated Norway balancing-market file containing hourly values (1–24) for regulation prices (up/down, imbalance), consumption/production, and interconnector exchange, plus metadata (year/week/day, currency, update time) and line count, Contains folder 1996-2023 and 2014_24h |

| DS-2.2.7.3.1 | electricity | Pono_description.doc | `electricity/nordpool/operating/pono/Pono_description.doc` | Nord Pool | - | - | `.doc`, \~28 KB | Reference | - | Defines the semicolon-separated weekly Norway files ponoYYWW.sdv. Sections: ST (file/time metadata), BE (area alias + text), hourly series PR/OM/FB/PS/UT with codes RN, RO, RC, RP, RS, DD, F, E, P, PE, U (prices in EUR/MWh; others in MWh/h, integers), and AL (total line count).|

| DS-2.2.7.4 | electricity | pose | `electricity/nordpool/operating/pose/YYYY/poseYYWW.sdv` | Nord Pool | 1996-2023 | Likely hourly 24×7; zone=SW | `.sdv` | Raw | `scripts\prepare_nordpool_operating_data.py` | Weekly, semicolon-separated Sweden balancing-market file containing hourly values (1–24) for regulation prices (up/down, imbalance), consumption/production, and interconnector exchange, plus metadata (year/week/day, currency, update time) and line count, Contains folder 1996-2023 and 2014_24h |

| DS-2.2.7.4.1 | electricity | Pose_description.doc | `electricity/nordpool/operating/pose/Pose_description.doc` | Nord Pool | - | - | `.doc`, \~28 KB | Reference | - | Defines the semicolon-separated weekly Sweden files poseYYWW.sdv. Sections: ST (file/time metadata), BE (area alias + text), hourly series PR/OM/FB/PS/UT with codes RN, RO, RC, RP, RS, DD, F, E, P, PE, U (prices in EUR/MWh; others in MWh/h, integers), and AL (total line count).|


| DS-2.3.1 | electricity | hydrologi_magasin_13092024.xlsx |`electricity/NVE/hydrologi_magasin_13092024.xlsx` | NVE | 1958 - 2024 |  Area × Year × Week | `.xlsx` (size 475 KB) | Raw | - | Weekly Nordic hydrology/reservoir status by area (year–week) with precipitation energy, useful inflow (model & statistical), hydrological balance, reservoir deviation, soil/groundwater, snow storage, and related deviations. Columns: Area, Year, Week, Precipitation energy, Useful inflow (HBV model), Useful inflow (statistics), Hydrological balance, Reservoir deviation, Soil & groundwater, Snow storage, Deviation (snow/soil/groundwater). Units: energy quantities (e.g., GWh) and indexes. |

| DS-2.3.2 | electricity | magasindata_per_mag.csv |`electricity/NVE/magasindata_per_mag.csv` | NVE | 1958 - 2024 |  Area × Year × Week | `.xlsx` (size 475 KB) | Raw | - | Weekly reservoir-levels per Norwegian reservoir, magasinid: reservoir ID; MagasinNavn: reservoir name; tidspunkt: timestamp (weekly); maalt\_fylling\_gwh: measured energy content (GWh); maalt\_kapasitet\_gwh: measured capacity (GWh); fyllingsgrad: fill level (%)

| DS-2.3.3 | electricity | wind2002_2023_perplant_utcplus1.xlsx |`electricity/NVE/wind2002_2023_perplant_utcplus1.xlsx` | NVE | 2002-01-01 00:00 → 2023-12-31 23:00 | Hourly    | Excel (.xlsx) • \~27.2 MB | Raw | `scripts\prepare_no_wind_production_data.py` | Hourly wind generation per plant for 2002–2023 (timestamps in UTC+1). First column is the timestamp; subsequent columns are individual wind plants (name and ID), with values giving each plant’s hourly output (blank/zero before commissioning). |

| DS-2.3.4 | electricity | windprod_hourly_no_plant.csv | `electricity/NVE/windprod_hourly_no_plant.csv` | NVE | 2002–2023 | Hourly × aggregated level *(total and/or bidding areas; no plant columns)* | CSV , size 211,640 KB | Intermediate  | `scripts\prepare_no_wind_production_data.py`, `scripts\prepare_nordpool_operating_data.py` | Hourly aggregated wind generation for Norway (total and/or NO1–NO5), timestamped UTC+1, units MWh, ~2002–2023; no plant-level detail. |

**airquality**
| **ID**   |            **Name**                                  | **Definitions**                              |
| -------- | ---------------------------------------------------- | -------------------------------------------- |
| DS-3     | airquality                                           |        air quality measures (Norway)         |
| DS-3.1   | airquality/figures                                   |                                              |
| DS-3.2   | airquality/measurements.csv                          |                                              |
| DS-3.3   | airquality/measurements.pq                           |                                              |
| DS-3.4   | airquality/stations.csv                              |       information on measuring stations      |


**airquality — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-3.1.1 | airquality | emissions_dev_plot.png |`airquality\figures\emissions_dev_plot.png` | - | - | .png | fig | - | TBD |
| DS-3.2 | airquality | measurements.csv |`airquality\measurements.csv` | NILU | 1978-12-01 to 2025-03-19 | Station × component × hour | CSV, 41,867,014 rows × 5 cols, 1797.71 MB | Raw | `scripts\get_airquality_measures.py` | measurement of each component from each station, value = measured value (unit depends on component); qualitycontrolled = QC flag (True/False); component = pollutant code (e.g., O3, NO2, PM10); id = station ID |
| DS-3.3 | airquality | measurements.pq |`airquality\measurements.pq` | NILU | Time: 1978-12-01 to 2025-03-19 | * | .pq, same as measurements.csv in .pq version| Raw | * | * |
| DS-3.4 | airquality | stations.csv | `airquality\stations.csv` | EEA Air Quality e-Reporting (EOI-station list) | 1978-11-30 → 2025-03-19 | One row per monitoring station | CSV, 1,298 rows × 15 cols, 0.04 MB | Raw | * | Air-quality station registry: one row per monitoring site with location (lat/lon), station type, measured pollutants, EOI code, and first/last measurement dates. |

**ofv**
| **ID**       |            **Name**                                  | **Definitions**                               |
| ------------ | ---------------------------------------------------- | --------------------------------------------- |
| DS-4         | ofv                                                  | data from OFV                                 |
| DS-4.1-4.29  | ofv/YYYY.xlsx                                        | information on cars registered in year XXXX   |                                              
| DS-4.30      | ofv/cars_full.pq                                     | parquet file with all the yearly files merged |
| DS-4.31      | ofv/LP_All.csv                                       | list prices and additional information by brand-model-make per year |                                             

**ofv — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-4.1-4.29 | ofv | YYYY.xlsx  | `ofv\YYY.xlsx` | OFV | 1994–2022 | One row per vehicle (registration/config) | XLSX | Raw | - | information on cars registered in year XXXX, including prices, battery, range and segments (additional to the motor vehicle registry). Contains duplicate values for each license number when the information in the motor vehicle registry is not sufficient to identify the exact brand-model-make of the car. All years share the same structure. |
| DS-4.30 | ofv | cars_full.pq | `ofv\cars_full.pq` | * | * | * | .pq, 253 MB | Intermediate Data | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_car_data.py`, `scripts\odometer_load.py` | parquet file with all the yearly files merged. |
| DS-4.31 | ofv | LP_All.csv | `ofv\LP_All.csv` | * | * | One row per timestamp × series ID | CSV, 8,126 KB | Raw | `scripts\driving_analysis_sketchboard.py`, `scripts\read_regdata.py` | list prices and additional information by brand-model-make per year. Difficult to merge with either the yearly OFV files or the motor vehicle registry |

**processed**
| **ID**       |            **Name**                                  | **Definitions**                                    |
| ------------ | ---------------------------------------------------- | -------------------------------------------------- |
| DS-5         | processed                                            | temporary files created during analysis/processing |
| DS-5.1       | processed/carhist_munid.csv                          |                                                    |
| DS-5.2       | processed/diffusekm.csv                              |                                                    |
| DS-5.3       | processed/diffusekm.dta                              |                                                    |
| DS-5.4       | processed/diffusekm.pq                               |                                                    |
| DS-5.5       | processed/drivemodel_sample.pq                       |                                                    |
| DS-5.6       | processed/postalmunidmap.csv                         |                                                    |
| DS-5.7       | processed/vehicle_choices.pq                         |                                                    |
| DS-5.8       | processed/vehicle_choices_with_emissions.pq          |                                                    |
| DS-5.9       | processed/vehicle_demog_choice_obs.pq                |                                                    |
| DS-5.10      | processed/vwp2502_choice_brand_model_make.csv        |                                                    |

**processed — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-5.1 | processed | carhist_munid.csv | `processed/carhist_munid.csv` | - | 1980-01-02 to 2018-09-05 | Vehicle × date (licensenumber × regdate) | CSV, 13,031,969 rows × 5 cols, 382.54 MB | processed | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_hhs_cardata.py`, `scripts\load_odometer_data.py` | Per-vehicle registration events with municipality: one row per licensenumber × date, including registration_type, owner_type, and munid (municipality code). |
| DS-5.2 | processed | diffusekm.csv | `processed/diffusekm.csv` | - | 1996-01-01 to 2022-02-18 | Vehicle × date (licensenumber × regdate) | CSV, 14,321,514 rows × 21 cols, 2581.87 MB | processed | `scripts\driving_analysis_sketchboard.py`, `scripts\odometer_load.py` | Per-vehicle usage periods (startmonth→endmonth) with derived distance and cost metrics. Each row is a license plate’s spell of time showing Dkm (km driven), Ddays, kmprday, costkm (fuel/toll), fuel type & HEV/PHEV flags, munid, vehicle attributes (van/body/segment), and reference dates (regdate, rownerdate). |
| DS-5.3 | processed | diffusekm.dta | `processed/diffusekm.dta` | * | * | * | dta, 1881 MB| * | `scripts\driving_analysis_sketchboard.py` | * | 
| DS-5.4 | processed | diffusekm.pq | `processed/diffusekm.pq` | * | * | * | pq, 13,695,491 rows × 21 cols, 599.88 MB| `scripts\driving_analysis_sketchboard.py`, `scripts\odometer_load.py` | * |
| DS-5.5 | processed | drivemodel_sample.pq | `processed/drivemodel_sample.pq` | - | Date: 2010-01-01 to 2019-09-04; Origdate: 1995-10-27 to 2017-11-30| Row-level records | PARQUET, 2,309,600 rows × 28 cols, 158.53 MB | processed | `scripts\driving_analysis.py`, `scripts\prepare_datasets.py` | Per-vehicle driving-spell records combining registry, odometer, routing, and demographics. For each plate (regnr) and spell (startmonth→endmonth), includes km, Dkm, Ddays, kmprday, fuel/HEV/PHEV/usage, costs (avgprice, costkm, tolls), registration and owner info (registration\_type, owner\_type, munid, origmunid, origdate), route features (lrid, routekm, centrality), and year/income/age. |
| DS-5.6 | processed | postalmunidmap.csv | `processed/postalmunidmap.csv` | TBD | - | One row per postal code → municipality mapping | CSV, 70 KB | reference  | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_hhs_cardata.py`, `scripts\load_registration_data.py` | Crosswalk from Norwegian postal codes to standardized municipality IDs (munid) for joining datasets to a common geography, postalcode = Norwegian post code; munid = standardized municipality ID; (optional fields if present) munname = municipality name; share = share of postcode in munid (if split); valid_from/valid_to = versioning dates. |
| DS-5.7 | processed | vehicle_choices.pq | `processed/vehicle_choices.pq` | TBD | - | Alternative-level (purchase event × vehicle alternative) | pq, 391 rows × 20 cols, 29 KB | processed | `scripts\driving_analysis.py`, `scripts\predict_choice_emissions_vca.py` | Vehicle purchase choice-set dataset—one row per alternative within a purchase event, with attributes and a chosen flag for the selected vehicle.
| DS-5.8 | processed | vehicle_choices_with_emissions.pq | `processed/vehicle_choices_with_emissions.pq` | TBD | - | one row per vehicle choice instance with attached emissions attributes | pq, 35 KB | processed | `scripts\driving_analysis.py`, `scripts\predict_choice_emissions_vca.py`, `scripts\prepare_datasets.py`  | Vehicle-choice dataset augmented with emissions metrics (e.g., per-vehicle CO₂/NOx factors) for each choice record |
| DS-5.9 | processed | vehicle_demog_choice_obs.pq | `processed/vehicle_demog_choice_obs.pq` | TBD | - | Row-level records | pq, 697,760 rows × 35 cols, 13.12 MB | processed | - | vehicle choice sets for discrete-choice modeling: each row is an alternative within a choice occasion (indexed by demogid, choice_id), with demographics (agecat, munid, income), vehicle attributes (segment/body/brand, price, range, weight, size, power, seats, transmission), technology flags (fuelid, hev, phev), operating costs (fuelprice, costkm, taxkm), emissions (co2, thc, nox, pm components), and taxes (regtax, actregtax), including an “outside” option. |
| DS-5.10 | processed | vwp2502_choice_brand_model_make.csv | `processed/vwp2502_choice_brand_model_make.csv` | TBD | - | One row per brand × model | CSV, 1,868 rows × 3 cols, 0.07 MB | processed | - | Alternative-level choice-set CSV for vehicle purchase modeling: each row is a candidate alternative with brand, model, make (and typical specs/prices), grouped by decision maker and choice occasion; includes a chosen-alternative flag. |

**traffic**
| **ID**       |            **Name**                                  | **Definitions**                                                             |
| ------------ | ---------------------------------------------------- | --------------------------------------------------------------------------- |
| DS-6         | traffic                                              |traffic measure files downloaded using NPRA API in 'get_traffic_measures.py' |
| DS-6.1       | traffic/aggvol.csv                                   |                                                                             |
| DS-6.2       | traffic/lengthvol.csv                                |                                                                             |
| DS-6.3       | traffic/trafficregpoints.csv                         |                                                                             |

**traffic — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-6.1 | traffic | aggvol.csv | `traffic/aggvol.csv` | NPRA | 1986-01-01T00:00:00+01:00 — 2022-04-27T22:00:00+02:00 | Hourly | CSV, 7.1 GB, Columns: 4, Rows: 200,405,153 (excluding header) | Raw | `scripts\get_traffic_measures_addparts.py`, `scripts\get_traffic_measures.py` | Hourly traffic per location: ISO-8601 timestamp, vehicle volume (nullable), and observation coverage (0–1). |
| DS-6.2 | traffic | lengthvol.csv | `traffic/lengthvol.csv` | NPRA | 2018-02-27T11:00:00+01:00 — 2021-04-13T13:00:00+02:00 | Hourly by length-bin | CSV, 57.4 GB, Columns: 5 | Raw | `scripts\get_traffic_measures_addparts.py`, `scripts\get_traffic_measures.py` | Hourly traffic per location, split by vehicle length bins: ISO-8601 time, id, length bin (e.g., `[5.6,7.6)`), volume, and coverage. | 
| DS-6.3 | traffic | trafficregpoints.csv | `traffic/trafficregpoints.csv` | NPRA | 1986-01-01 00:00:00+01:00 — 2022-04-23 19:00:00+02:00 | One row per counting point | CSV, 0.7 MB, 4,434 rows × 13 cols | Raw | `scripts\get_traffic_measures_addparts.py`, `scripts\get_traffic_measures_addparts.py` | station registry: one row per road traffic counting point, with IDs (incl. NPRA), location (lat/lon, municipality), status flags (bike/periodic/retired/tempout), and the first/last timestamps where data exists. |

**utility**
| **ID**             |            **Name**                                  | **Definitions**                                                              |
| ------------------ | ---------------------------------------------------- | ---------------------------------------------------------------------------- |
| DS-7               | utility                                              |                                                                              |
| DS-7.1             | utility/vca                                          |                                                                              |
| DS-7.1.1-DS-7.1.8  | utility/vca_YYYY.csv                                 |                                                                              |
| DS-7.1.9           | utility/vca_unique.csv                               |                                                                              |
| DS-7.1.10          | utility/avstand_kommun_km_2_8.dta.csv                |                                                                              |
| DS-7.1.11          | utility/centrality2020.csv                           |                                                                              |
| DS-7.1.12          | utility/cpi_1865_2022.csv                            | CPI for Norway until 2022                                                    |
| DS-7.1.13          | utility/cpi_monthly_1920_2024.csv                    |                                                                              |
| DS-7.1.14          | utility/cpi_yearly_1920_2024.csv                     |                                                                              |
| DS-7.1.15          | utility/gdp_population.csv                           |                                                                              |
| DS-7.1.16          | utility/income_lognormal_params_2022_2015NOK.csv     |                                                                              |
| DS-7.1.17          | utility/income_muni_year.csv                         |                                                                              |
| DS-7.1.18          | utility/matrikkelenAdresse.csv                       |                                                                              |
| DS-7.1.19          | utility/population_muni_year_age.csv                 |                                                                              |
| DS-7.1.20          | utility/postnummer.csv                               |                                                                              |
| DS-7.1.21          | utility/reddays_2013_2022.csv                        |                                                                              |
| DS-7.1.22          | utility/ssb_electricity_prices.csv                   | Average prices of electricity in Norway over time (quarterly)                |
| DS-7.1.23          | utility/ssb_fuel_prices.csv                          | Average prices of fuel (gasoline and diesel) in Norway over time (monthly)   |
| DS-7.1.24          | utility/vehicle_taxes.csv                            |    |

**utility — Detailed Metadata**
| **ID** | **Domain** | **Name** | **Path** | **Source** | **Period** | **Grain** | **Format/Size** | **Status** | **Scripts** | **Definitions** |
|--------|------------|----------|----------|------------|------------|-----------|-----------------|------------|-------------|-----------------|
| DS-7.1.1-DS-7.1.8 | utility | vca_YYYY.csv | utility/vca_YYYY.csv | TBD | 2019-2025/02 | One row per vehicle variant/trim | CSV, 4,140 rows × 45 cols; 44 manufacturers; Testing scheme: WLTP; Fuel types: 8; Powertrains: 7 | Raw | `scripts\predict_emissions_from_vca.py` | (for 2019–2025/02 set):
Annual VCA certification snapshot—one row per UK type-approved car variant/trim with specs (manufacturer/model, fuel type & powertrain, transmission, engine size/power, Euro standard) and WLTP results (CO₂, fuel economy; electric consumption & range for BEV/PHEV, weighted values for PHEV), plus derived running-cost fields. |
| DS-7.1.9 | utility | vca_unique.csv| `utility/vca_unique.csv` | TBD | - | One row per unique Manufacturer–Model–Description |  CSV, 530.4 KB, 10,443 rows × 3 cols | Raw | - | It’s a deduplicated catalog of vehicle variants: 10,443 rows with three fields — Manufacturer, Model, Description — representing unique VCA entries (49 manufacturers, ~1k models). |
| DS-7.1.10 | utility | avstand_kommun_km_2_8.dta.csv | `utility/avstand_kommun_km_2_8.dta.csv` | TBD | - | Origin municipality × destination municipality | Stata .dta | Raw | - | Stata dataset of inter-municipal distances in Norway — likely a pairwise table of municipality A → municipality B with distance in kilometers, origin_munid = from-municipality ID; dest_munid = to-municipality ID; distance_km = distance between the two in km. |
| DS-7.1.11 | utility | centrality2020.csv | `utility/centrality2020.csv` | TBD | 2020 | Municipality (munid/kommunenummer) | CSV, 3 KB | Reference mapping | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\get_municipality_population_ssb_api.py`, `scripts\get_municipality_population_ssb_api.py`, `scripts\prepare_datasets.py` | Norwegian municipality centrality (SSB “Sentralitet”) for 2020—one row per municipality with centrality index/class for linking to other datasets. |
| DS-7.1.12 | utility | cpi_1865_2022.csv | utility/cpi_1865_2022.csv | SSB (Statistics Norway) | 1865–2022 | Yearly (Norway) | CSV, 2 KB | Raw | `scripts\driving_analysis_sketchboard.py` | year = calendar year; cpi = consumer price index (index value; base year per source). |
| DS-7.1.13 | utility | cpi_monthly_1920_2024.csv | utility/cpi_monthly_1920_2024.csv | SSB (Statistics Norway) | 1920–2024 | Monthly (Norway) | CSV, 21 KB | Raw | `scripts\driving_analysis.py`, `scripts\get_ssb_cpi.py`, `scripts\load_additional_data.py` | date = month (YYYY-MM); cpi = CPI index value |
| DS-7.1.14 | utility | cpi_yearly_1920_2024.csv | utility/cpi_yearly_1920_2024.csv | SSB (Statistics Norway) | 1920–2024 | Yearly (Norway) | CSV, 2 KB | Raw | `scripts\get_ssb_cpi.py` | Yearly CPI index for Norway, 1920–2024 |
| DS-7.1.15 | utility | gdp_population.csv | utility/gdp_population.csv | TBD | 1978Q1-2024Q4 | Quarterly | CSV, 7 KB | Raw | `scripts\get_ssb_data_api.py`, `scripts\load_registration_data.py` | GDP and population |
| DS-7.1.16 | utility | income_lognormal_params_2022_2015NOK.csv | utility/income_lognormal_params_2022_2015NOK.csv | TBD | 2022 | Group × year  | CSV, 1 KB | Raw | - | Columns typically include group identifiers (e.g., munid/segment), mu and sigma of log-income, and possibly counts/means. |
| DS-7.1.17 | utility | income_muni_year.csv | utility/income_muni_year.csv | TBD | 2005-2023 | Municipality × year | CSV, 184 KB | Raw | `scripts\get_municipality_population_ssb_api.py` | Typical columns: munid (municipality ID), year, mean_income, median_income, p10, p25, p75, p90, n (population count); currency NOK unless noted. |
| DS-7.1.18 | utility | matrikkelenAdresse.csv | utility/matrikkelenAdresse.csv | Matrikkelen | Oppdateringsdato: 2020-05-27 to 2025-09-26 | Address record (municipality × street × house number) | CSV, 2,888,612 rows × 46 cols, 776.97 MB | Raw | - | lokalid = local id; kommunenummer = municipality code; kommunenavn = municipality name; adressenavn = street name; nummer = house number; bokstav = letter; postnummer = postal code; poststed = postal place, Typical use: geocoding and joining people/vehicles to a municipality/street/house number for analysis. |
| DS-7.1.19 | utility | population_muni_year_age.csv | utility/population_muni_year_age.csv | Matrikkelen |  Year: 1986 to 1993 | Row-level records | CSV, 1,562,596 rows × 4 cols, 23.93 MB | Raw | `scripts\get_municipality_population_ssb_api.py` | A CSV of annual population counts by municipality and single-year age, with columns `year`, `munid`, `age`, and `population`. |
| DS-7.1.20 | utility | postnummer.csv | utility/postnummer.csv | (likely Posten/SSB) | 29 Aug 2022 to 02 Aug 2024 | One row per postal code | CSV, 5,790 rows and 14 columns | Raw | `scripts\load_hhs_cardata.py` | Typical columns: postnummer (postcode), poststed (place name), kommunenr (municipality code), kommune (name), kategori (street/PO box), ev. fylke (county), lat/lon (if present). Used to join addresses to municipalities. |                                                                            
| DS-7.1.21 | utility | reddays_2013_2022.csv | `utility/reddays_2013_2022.csv` | Statistics Norway / calendar ref | 2013-01-01 to 2022-12-31 | Daily dates | CSV, 3653 rows and 10 columns | Raw | - | Norwegian “red days” (public holidays and Sundays). Typical cols: date, holiday_name, is_red_day (1/0), weekday; used to flag non-working days. |                                                                            
| DS-7.1.22 | utility | ssb_electricity_prices.csv | utility/ssb_electricity_prices.csv | SSB | 2003 to 2023 | quarterly | CSV, 84 rows × 2 columns | Raw | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_additional_data.py`, `scripts\odometer_load.py` | Average prices of electricity in Norway over time (quarterly) |
| DS-7.1.23 | utility | ssb_fuel_prices.csv | utility/ssb_fuel_prices.csv |  Statistics Norway (SSB) | 1986 to 2024 | Monthly | CSV, 0.01 MB, 452 rows × 3 columns | Raw | `scripts\driving_analysis_sketchboard.py`, `scripts\driving_analysis.py`, `scripts\load_additional_data.py`, `scripts\odometer_load.py`| Average prices of fuel (gasoline and diesel) in Norway over time (monthly) |    
| DS-7.1.24 | utility | vehicle_taxes.csv | utility/vehicle_taxes.csv | TBD | 1969-2023 | TBD | CSV, 0.01 MB, 555 rows × 5 columns | Raw | `scripts\driving_analysis.py`, `scripts\prepare_datasets.py` | year, name, step, increment, fee | 


