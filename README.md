OGIMET Africa Meteorological Data Ingestion & Processing Pipeline (Azure)

This repository contains a pipeline for ingesting SYNOP and METAR meteorological observations from the OGIMET service for African stations (WMO Blocks 60–69).


Pipeline Architecture
1. Setup Component
Initializes the directory structure and generates shared configuration files used by all downstream steps.
Outputs:

setup_output/ with base_dir.txt and metadata.

2. SYNOP Ingestion Component
Scrapes SYNOP observations from OGIMET for all African WMO blocks
Supports incremental windowed ingestion

3. METAR Ingestion Component
Scrapes METAR observations for all ICAO stations across Africa.
METAR ingestion begins only after SYNOP data is available.

4. Decoder Component
Decodes raw OGIMET text data into structured CSV/Parquet tables.
Includes: Temperature (tmin, tmax, tmed), Precipitation, Humidity, Wind speed, Station metadata (lat, lon, alt)

5. Aggregation & QC Component
Applies quality controls, merges decoded outputs, organizes files by: Date, Station, Observation type

6. Final Dataset Builder
Produces the final analytics-ready dataset used for Climate baselines

Feel free for collaboration, issues, or enhancements
