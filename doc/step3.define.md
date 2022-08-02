
## Step 3: Define the Data Model
- Map out the conceptual data model and explain why you chose that model
- List the steps necessary to pipeline the data into the chosen data model

---
### Best Use
I think that the best use of this data is as a <u>research dataset</u>, residing in a data "lake"
- It does not make sense (to me) as a data warehouse because it does not provide a closed solution that end users would need - It may or may not provide any insights at all
- at this point we don't know what we might want to use in the future, so I am leaving the full data set, and ony adding additional columns to enable joins.  I might not expose all of that to all users, but

### Create a Data Lake dataset 
- As a proof of concept - I am creating a <u>Data-Lake</u> dataset of immigration and temperature data,
- I want to have a "short" lineage - not too much change and not too far from the raw data
- I want to avoid a lot of joins and I don't mind redundancy, so I don't want to chop this data into facts and dimensions .. yet
- This can go into Redshift, Athena, MongoDB, even Cassandra 

### Create my own INPUT data
- For this project - I do not want to use the provided snapshot of the immigration and temperature data provided.
- Instead, I am first transforming those snapshots into periodic data that I can read periodically and update into the "data lake"
- I recognize that the real data will never be provided that way, but since this is an exercise, I want to start with a periodic dataset
- so I have a preparation step where I create a partitioned dataset from this data.  This is not real data, but provided for a more interesting assignment


   Partitioned Global temperature INPUT data
   ```text
   ├── year=2005
   │        ├── month=1
   │        │        ├── Country=Australia
   │        │        │        ├── part-00000-923b4d11-3030-49b7-bbd4-d08b4f8eef99.c000.csv
   │        │        │        ├── part-00004-923b4d11-3030-49b7-bbd4-d08b4f8eef99.c000.csv
   │        │        │        ├── part-00005-923b4d11-3030-49b7-bbd4-d08b4f8eef99.c000.csv
   │        │        │        ├── part-00006-923b4d11-3030-49b7-bbd4-d08b4f8eef99.c000.csv
   
   ```
   
   Partitioned i94immigration dataset INPUT data
   ```text
   └── year=2016
       └── month=4
           ├── i94port=5KE
           │        ├── part-00005-3930b00f-386b-4cda-a27b-ab27106a818e.c000.csv
           │        └── part-00012-3930b00f-386b-4cda-a27b-ab27106a818e.c000.csv
           ├── i94port=5T6
           │        ├── part-00002-3930b00f-386b-4cda-a27b-ab27106a818e.c000.csv
           │        ├── part-00004-3930b00f-386b-4cda-a27b-ab27106a818e.c000.csv
           │        └── part-00010-3930b00f-386b-4cda-a27b-ab27106a818e.c000.csv
   
   ```
   
## Steps
Using the program  `src/prepare/load_raw_data.py` :
1. Create the INPUT: partitioned datasets from the raw immigration and temperature snapshot data
2. PROCESS the partitioned datasets 
3. into a GOLD dataset and a control table

### Step 1. INPUT: partitioned datasets
1. table_name: `immigrations`  <-- `./immigration/year=*/month=*/i94port=*/<csv files>`
2. table_name: `state_temperatures`  <-- `./temperature/year=*/month=*/Country=*/<csv files>`


### Step 2. PROCESS the data in a pipeline job
Once we have the partitioned dataset, I'll create the "Gold" data for the data lake.

Using the programs in `src/stage`:
- load_prepared_data.py
- load_control_data.py
- common.py

This will read the INPUT data and augment the data to make it easier to join the tables and OUTPUT into a GOLD dataset:

### Step 3. OUTPUT: "Gold" datasets
1. table_name: `immigrations`  -- all immigrations data upgraded to support joining with temperatures
2. table_name: `state_temperatures`  -- all temperatures data upgraded to support joining with immigration
3. table_name: `load_files`    -- a record of the files and records ingested
 
Each job integrates the gold data along with a separate load_files control table
This can be used to validate that all records from each input file were processed into the gold dataset


### Open To Do's:




## Schemas

```sql
create table immigration
(
    cicid                text,
    i94yr                text,
    i94mon               text,
    i94cit               text,
    i94res               text,
    i94port              text,
    arrdate              text,
    i94mode              text,
    i94addr              text,
    depdate              text,
    i94bir               text,
    i94visa              text,
    count                text,
    dtadfile             text,
    visapost             text,
    occup                text,
    entdepa              text,
    entdepd              text,
    entdepu              text,
    matflag              text,
    biryear              text,
    dtaddto              text,
    gender               text,
    insnum               text,
    airline              text,
    admnum               text,
    fltno                text,
    visatype             text,
    birth_year           integer,
    ----- I am adding the following columns   -----
    year                 integer,
    month                integer,
    file_id              text,  -- an id for the file that this record came from
    origin_country_name  text,
    origin_country_code  text,
    arrival_country      text,
    arrival_country_name text,
    arrival_state_code   text,
    arrival_city_name    text
);
```

```sql
create table state_temperatures
(
    dt                       timestamp,
    average_temp             double precision,
    average_temp_uncertainty double precision,
    state_name               text,
    country_name             text,
    timestamp                date,
    ----- adding the following columns   ----- 
    year                     integer,
    month                    integer,
    canon_country_name       text,         --- uppercase the country name
    file_id                  text not null --- an id for the file that this record came from
);
```

```sql
create table controls.load_files
(
    input_file text not null,  -- the full file name from the partitioned datasets
    dataset    text not null,  -- the dataset (immigration or temperature ---
    file_id    text not null,  -- the file_id to be used as a foriegn key in the tables
    file_name  text not null   -- the base file name (without directory)
);
```



