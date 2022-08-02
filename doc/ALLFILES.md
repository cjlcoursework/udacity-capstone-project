
```
├── Capstone Project Template.ipynb
├── README.md
├── boneyard
│        ├── __init__.py
│        ├── iata_names.csv
│        ├── load_immigration_pandas.py
│        ├── load_raw_controls.py
│        ├── sas_date_sandbox.py
│        └── schemas.py
├── data
│        ├── prepdata
│        │        ├── immigration
│        │        │        ├── _SUCCESS
│        │        │        └── year=2016.0
│        │        │            └── month=4.0
│        │        │                ├── i94port=5KE
│        │        │                │        ├── part-00005-c75380fa-e974-46a1-802f-c8340ea48a28.c000.csv
│        │        │                │        └── part-00012-c75380fa-e974-46a1-802f-c8340ea48a28.c000.csv
│        │        └── temperature
│        │            ├── _SUCCESS
│        │            ├── year=2005
│        │            │        ├── month=1
│        │            │        │        ├── country_name=Australia
│        │            │        │        │        ├── part-00000-2f1515ef-e3b9-4701-a31f-cad6368f3990.c000.csv
│        │            │        │        │        ├── part-00004-2f1515ef-e3b9-4701-a31f-cad6368f3990.c000.csv
│        │            │        │        │        ├── part-00005-2f1515ef-e3b9-4701-a31f-cad6368f3990.c000.csv
│        │            │        │        │        ├── part-00006-2f1515ef-e3b9-4701-a31f-cad6368f3990.c000.csv
│        │            │        │        │        └── part-00007-2f1515ef-e3b9-4701-a31f-cad6368f3990.c000.csv
│        └── rawdata
│            ├── controls
│            │        ├── airport-codes.csv
│            │        ├── country_codes.csv
│            │        ├── sas_countries.csv
│            │        ├── sas_index_raw.csv
│            │        ├── shit
│            │        └── state_names.csv
│            ├── temperatures
│            │        └── GlobalLandTemperaturesByState.csv
│            └── udacity
│                ├── I94_SAS_Labels_Descriptions.SAS
│                ├── airport-codes_csv.csv
│                ├── sas_data
│                │        ├── part-00000-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet
│                │        ├── part-00001-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet
│                │        ├── part-00002-b9542815-7a8d-45fc-9c67-c9c5007ad0d4-c000.snappy.parquet
│                └── us-cities-demographics.csv
├── data_test
│        ├── immigration
│        │        ├── _SUCCESS
│        │        └── year=2016
│        │            └── month=4
│        │                ├── i94port=5KE
│        │                │        ├── part-00005-ad3b1986-560e-4080-b7e6-a29b045fa246.c000.csv
│        │                │        └── part-00012-ad3b1986-560e-4080-b7e6-a29b045fa246.c000.csv
│        └── temperature
│            ├── _SUCCESS
│            └── year=2016
│                ├── month=1
│                │        ├── Country=Australia
│                │        │        ├── part-00000-c9a6d1c9-28a2-427a-8c97-6cfb697f754f.c000.csv
│                │        │        ├── part-00004-c9a6d1c9-28a2-427a-8c97-6cfb697f754f.c000.csv
│                │        │        ├── part-00005-c9a6d1c9-28a2-427a-8c97-6cfb697f754f.c000.csv
│                │        │        ├── part-00006-c9a6d1c9-28a2-427a-8c97-6cfb697f754f.c000.csv
│                │        │        └── part-00007-c9a6d1c9-28a2-427a-8c97-6cfb697f754f.c000.csv
├── datalake
│        ├── immigration
│        │        ├── controls
│        │        │        ├── load_files
│        │        │        │        └── load_year=2022
│        │        │        │            └── load_month=08
│        │        │        │                └── load_day=02
│        │        │        │                    └── load_hour=02
│        │        │        │                        ├── _SUCCESS
│        │        │        │                        └── part-00000-e186af70-3b3d-4acd-a066-0f1dba9efddb-c000.snappy.parquet
│        │        │        └── load_times
│        │        │            └── load_year=2022
│        │        │                └── load_month=08
│        │        │                    └── load_day=02
│        │        │                        └── load_hour=02
│        │        │                            ├── _SUCCESS
│        │        │                            └── part-00000-c1fa0358-1054-4f6e-9c7e-d7e888f0767f-c000.snappy.parquet
│        │        └── data
│        │            └── load_year=2022
│        │                └── load_month=08
│        │                    └── load_day=02
│        │                        └── load_hour=02
│        │                            ├── _SUCCESS
│        │                            ├── part-00000-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
│        │                            ├── part-00008-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
│        │                            ├── part-00009-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
│        │                            └── part-00010-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
│        └── temperature
│            ├── controls
│            │        ├── load_files
│            │        │        └── load_year=2022
│            │        │            └── load_month=08
│            │        │                └── load_day=02
│            │        │                    └── load_hour=02
│            │        │                        ├── _SUCCESS
│            │        │                        └── part-00000-be4630d0-158c-443f-8179-71d936f56122-c000.snappy.parquet
│            │        └── load_times
│            │            └── load_year=2022
│            │                └── load_month=08
│            │                    └── load_day=02
│            │                        └── load_hour=02
│            │                            ├── _SUCCESS
│            │                            └── part-00000-07342999-7c6d-4a1a-8e60-ba47694d7dd8-c000.snappy.parquet
│            └── data
│                └── load_year=2022
│                    └── load_month=08
│                        └── load_day=02
│                            └── load_hour=02
│                                ├── _SUCCESS
│                                ├── part-00000-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
│                                ├── part-00001-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
│                                ├── part-00002-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
│                                └── part-00017-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
├── doc
│        ├── ALLFILES.md
│        ├── DATALAKE.files.md
│        ├── SRC.files.md
│        ├── boneyard.md
│        ├── data_dictionary.md
│        ├── step1.scope.md
│        ├── step2.explore.md
│        ├── step2.labnotes1.md
│        ├── step2.labnotes2.md
│        ├── step3.define.md
│        ├── step4.architecture.md
│        ├── step4.pipeline.md
│        ├── step4.proof-of-concept.md
│        └── terms.md
└── src
    ├── common
    │        ├── common.py
    │        ├── config.py
    │        ├── ingest_control_data.py
    │        └── ingest_core_data.py
    ├── prepare
    │        ├── from_raw_to_partitioned.py
    │        ├── from_raw_to_postgres.py
    │        └── load_core_data.py
    └── stage
        ├── ingest_immigration.py
        └── ingest_temperatures.py

```

