### Approach
- Play with a sample set in an IDE (Postgres)
Could do this in Redshift or Athena, but working with sample data in a local Postgres is free to me

  1. Get a sense of the data and points of convergence in the data
  2. Get a sense of the completeness and scope of the data
  3. Start creating some conformed naming for key values - for example iana codes are called different things in different datasets

- [x] Load the core sample data into Postgres
  - load alternative state, city, country temperatures to decide
- [x] Load some helper tables (controls) into Postgres to make the data easier to read
- [ ] Look at what the data says - not what I want (queries and cleanup)


### Controls

---
src/load_controls.py
* input
1. file: city_codes.txt <-- cut.paste  `I94CIT` city codes to name
2. file: iana_names.txt <-- cut.paste  `I94PORT` airport codes to state, country

---
* data
1. table: city_codes from I94_SAS_Labels_Descriptions.SAS  -- transform `city_codes.txt` to table=`controls.city_codes`
2. table: iana_names from I94_SAS_Labels_Descriptions.SAS  -- transform `iana_names.txt` airport codes  table=`controls.iana_names`
3. table: state_names from controls/state_names.csv  -- to  table=`controls.state_names`

---

### DataSets

1. table_name: `temperatures`  <-- `data/global-temperatues/SampleGlobalLandTemperaturesByCity.csv`
2. table_name: `immigrations`  <-- `data/udacity/immigration_data_sample.csv`
3. table_name: `us_cities`     <--  `data/udacity/us-cities-demographics.csv`
4. table_name: `airport_codes` <-- `data/udacity/airport-codes_csv.csv`

