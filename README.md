OGIMET Africa Meteorological Data Ingestion & Processing Pipeline

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

│
├── components/
│   ├── setup/
│   │   ├── setup.yml
│   │   └── pipeline_setup.py
│   ├── synop_ingest/
│   │   ├── synop_ingest.yml
│   │   └── synop_incremental.py
│   ├── metar_ingest/
│   │   ├── metar_ingest.yml
│   │   └── metar_incremental.py
│   ├── decode/
│   │   ├── decode.yml
│   │   └── decoder_incremental.py
│   ├── aggregate/
│   │   ├── aggregate.yml
│   │   └── aggregation_qc.py
│   └── final/
│       ├── final.yml
│       └── final_builder.py
│
├── azure_pipeline.yml   # Master Azure ML pipeline definition
└── README.md            # Documentation

Feel free for collaboration, issues, or enhancements