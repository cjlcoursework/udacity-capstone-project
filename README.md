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


### Data

- i94
Form I-94, Arrival/Departure Record, Information for Completing USCIS Forms
The Department of Homeland Security (DHS) issues Form I-94, Arrival/Departure Record, to aliens who are:
- Admitted to the U.S.;
- Adjusting status while in the U.S.; or
- Extending their stay.


### DataSets

country_codes.csv  -- uploaded from https://gist.github.com/radcliff/f09c0f88344a7fcef373
sas_countries_raw.csv  -- original data from file://I94_SAS_Labels_Descriptions.SAS
state_names.csv   --- uploaded from https://worldpopulationreview.com/states/state-abbreviations
sas_countries.csv   -- joined sas_countries_raw.csv and country_codes.csv on country_name
world_cities -- git@github.com:datasets/world-cities.git (Not using until we expand to non-US destinations)
airport-codes.csv  -- modified from udacity airport codes - added 5 'missing' US airports
bad_airports.csv  -- temp table
sas_countries_raw.csv  -- from file://I94_SAS_Labels_Descriptions.SAS

-- core tables
1. table_name: `state_temperatures`  <-- `data/temperatues/SampleGlobalLandTemperaturesByState.csv`
2. table_name: `immigrations`  <-- `data/udacity/immigration_data_sample.csv`

--controls
3. table_name: `airport_codes`  <-- `data/controls/airport_codes.csv`  
4. table_name: `country_codes`  <-- `data/controls/country_codes.csv`
5. table_name: `sas_countries`  <-- `data/controls/sas_countries.csv`
6. table_name: `state_names`  <-- `data/controls/state_names.csv`
7. table_name: `world_cities`  <-- `data/controls/world_cities.csv`


### Missing airport codes
```sql
WITH bad_airports as (
    with immigration_countries as (select distinct i94port, arrival_country_name, arrival_country from immigration )
    select distinct I.i94port, I.arrival_country_name, I.arrival_country, S.iata_code, S.ident
    from immigration_countries I
             left join  controls.airport_codes S on I.i94port in (S.ident, S.iata_code)
    where iata_code is null
 )
select I.i94port, count(*) from immigration I join bad_airports B on B.i94port = I.i94port
group by I.i94port ;

---------
NYC,485953
HHW,142720
CHI,130567
FTL,95977
LVG,89287
WAS,74858
SAI,23628
SAJ,9144
NOL,4409
XXX,3522
X96,2378
YGF,1763
PHU,1653
YHC,772
INP,426
MIL,248
PHR,228
EPI,178
SSM,92
JKM,70
GPM,51
WBE,28
RAY,27
RIF,25
PTL,21
FPT,15
MOR,14
DLB,12
ROM,12
JMZ,7
PAR,6
FER,5
FTC,5
NYL,5
5T6,4
FTF,3
JFA,3
SPO,3
VCB,3
HEF,2
RYY,2
SGJ,2
NC8,1
RIO,1
VNB,1

```

Manually add the following to the airports DB (no elevations)
NYC,485953
HHW,142720
CHI,130567
FTL,95977
LVG,89287
WAS,74858

```sql
WITH bad_airports as (
    with immigration_countries as (select distinct i94port, arrival_country_name, arrival_country from immigration )
    select distinct I.i94port, I.arrival_country_name, I.arrival_country, S.iata_code, S.ident
    from immigration_countries I
             left join  controls.airport_codes S on I.i94port in (S.ident, S.iata_code)
    where iata_code is null
)
select I.i94port, count(*) from immigration I join bad_airports B on B.i94port = I.i94port
group by I.i94port ;
------
SAI,23628
SAJ,9144
NOL,4409
XXX,3522
X96,2378
YGF,1763
PHU,1653
YHC,772
INP,426
MIL,248
PHR,228
EPI,178
SSM,92
JKM,70
GPM,51
WBE,28
RAY,27
RIF,25
PTL,21
FPT,15
MOR,14
DLB,12
ROM,12
JMZ,7
PAR,6
FER,5
FTC,5
NYL,5
FTF,3
JFA,3
SPO,3
VCB,3
HEF,2
RYY,2
SGJ,2
NC8,1
RIO,1
VNB,1



```

### Mo overlap with sample temperature, immigration data
- immigration sample data has only 2016, 4 as year, month 
- to POC the solution bump the state_temperature year +3 = so we have temperatures in 2016



#### Finally - after cleanup and filtering 
```sql
-- we use iata code to get to the arrival GEO data - check that it's complete
with immigration_countries as (select distinct i94port, arrival_country_name, arrival_country from immigration )
select distinct I.i94port, I.arrival_country_name, I.arrival_country, S.iata_code, S.ident
from immigration_countries I
left join  controls.airport_codes S on I.i94port in (S.ident, S.iata_code)
where iata_code is null;
-- perfect - no records found

with immigration_countries as (select distinct origin_country_name from immigration )
select  I.origin_country_name, S.canon_country_name from immigration_countries I
     left join controls.sas_countries S on S.canon_country_name = I.origin_country_name
    where S.canon_country_name is null
-- perfect - no records found


```


### Map immigrations to state temperatures

```sql
--- correlate immigration arrival.country to temperatures.canon_country_name
select   count(*)
from immigration I
    left join state_temperatures C on I.arrival_country_name = C.canon_country_name
where C.canon_country_name is null;
--- perfect - no records found

--- correlate immigration arrival.country to temperatures.canon_country_name
select   count(*)
from immigration I
         left join state_temperatures C on I.origin_country_name = C.canon_country_name
where C.canon_country_name is null;
--- perfect - no records found

```


### Next issues
-- small airports in Brazil have no iata_code - remove those from airport codes - so iata code is unique
  - its a small set - only 68 small airports in Brazil
```sql
select count(*) from (
select iata_code, count(*) as record_count
from controls.airport_codes
group by iata_code
having count(*) > 1 ) A;

--- returns 68
```

- somae ita codes in airports are '0' -- only about 106 and not US, so filter them out
- if the iata_code is null, replace it with the ident number -- gets come additional hits that way -- immigration data uses i94port == airport_codes.ident in a few cases

- closed airport has duplicated becuase 2 records for closed and open records with the same iata code, 
  - can't just remove the
  - causing duplicates - 
  - partition on the iata_code over closed=T/F, and city_name

result= 
```
select
iata_code, count(*)
from controls.airport_codes
where iso_country='US'
group by iata_code
having count(*) > 1;

--- no records --

```


- Double cicid values - can't use as a aky - but these are really duplicates - except that sme have more data than others
- soma have citiy names some do not, so partition by cicid over arrival-city, and take the first
```sql
WITH  DEDUP AS (
    WITH DUPS as (
        select cicid, count(*) as record_count
                            from immigration
                            group by cicid
                            having count(*) > 1
      )
      select *,
             row_number() OVER (PARTITION BY I.cicid
                 ORDER BY arrival_city_name ASC) AS rownumber
      from immigration I
               JOIN DUPS D on I.cicid = D.cicid
      order by I.cicid)
select *
from DEDUP
WHERE rownumber = 1;

```




### Use controls to update immigrations and temperatures by state
immigration sas_data/  --- Udacity parquet
  - cardinaity of destination/arrival data --> country, state, city
  - cardinality of origin --> country

temperatures by state -- https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data
  - blah,

---
## Correlate immigration arrivals and immigration departures (origin)  

--- 

#### correlate immigration arrival.country to temperatures.canon_country_name

```
select count(*)
from pyspark_immigration I
left join state_temperatures C on I.arrival_country_name = C.canon_country_name
where C.canon_country_name is null;

--- 430115 records do not match across country
---1774764 total records - 430115 = 1344649 = 68% completeness on immigration arrival country to state temperatures
```
---

#### correlate immigration origin (departure).state to temperatures.canon_country_name

```
select count(*)
from pyspark_immigration I
left join state_temperatures C on I.origin_country_name = C.canon_country_name
where C.canon_country_name is null;
--- 1395133
-- (1774764 total records - 1395133) / 1774764 = 78% completeness on origin country to state temperatures
```


#### kaggle global temperatures file has only the following countries:

`select distinct canon_country_name from state_temperatures order by canon_country_name;`

```
AUSTRALIA
BRAZIL
CANADA
CHINA
INDIA
RUSSIA
UNITED STATES
```

#### Arrival and departure dates
- The immigration data seems to have "bad" arrival and departure sas date number (i think) they are too small to convert to real dates
  - A sas numeric date should be the epoch + the sas date as seconds
  - There is a dtaddto field that might replace the 'epoch' in the calculation - but the dataes still do not make sense


  ```
  def from_sas_date(sas_date: int) -> datetime:
      return datetime.datetime(1960,1,1) + datetime.timedelta(seconds=sas_date)
  ```

#### Determination
The only interesting correlation is between temperatures in the departure and arrival locations and immigration away from departure cities or to arrival locations
- `Does temperature influence migration?`

#### immigration data
The main drive table has to be immigration dataset
 - comes from (Udacity)
 - The immigration data has two locations: 
   - the origin/departure location, and 
   - the arrival location
 - Use the year and month data as "arrival" dates unless we can get better data or a better understanding of the dates codes

#### State temperatures

 - For this project - just correlate at the country level, using the temperature data we have from kaggle
 - without a better temperature dataset we can only correlate immigration to temperature between the following countries

   - AUSTRALIA
   - BRAZIL
   - CANADA
   - CHINA
   - INDIA
   - RUSSIA
   - UNITED STATES

 - Can't go any denser than country for origin (departure) data
 - Also can't go any denser than country for arrival data  - unless we get a complete world-states-countries database to get the state codes
   - Experiments with US state code shows that with a more complete world-states-countries database we can get down to the state level ONLY for the arrival states, not the origin states (they don't exist in immigration data)
   - Experimented with getting the state codes using the arrival immigration city - it will work if we need that


Time granularity year, month


- all th


### Closure
- what arrival geos (countries, states, cities)  have all temperature data for any given origin
- what origin geos (country) have all temperature data for any given destination 

--- 
Closed sample 
1. filter out any arrivals or origins that are not in the countries having temps 
2. filter out only US destinations/arrivals -- see what getting to the state leve tells us

3. filter out sas_countries that are DEFUNCT - join to the good set of get_sas_countries(spark=spark) which filters out the DEFUNCT
4. filter out any i94port code that don't conform to 

-- normally I would save the whole data and count what was filtered 

```sql
        SELECT I.*,
            upper(S.canon_country_name) as origin_country_name,
            upper(S.country_code2) as origin_country_code,
            A.country_code2 as arrival_country,
            upper(A.country_name) as arrival_country_name,
            A.state_code as arrival_state_code,
            A.city_name as arrival_city_name
        FROM immigration I
        join countries S on S.sas_code = I.i94cit   -- fliter out any countries not in the 
        join airports A on A.iata_code = I.i94port
        where S.canon_country_name in {COUNTRIES_WITH_TEMPERATURE_DATA}
        and  A.country_code2 = 'US'
```

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

### Proof of Concept 
1. is there a correlation between temperature and immigration?  e.g. are immigrants comming from coller to warmer locations or vis versa.
2. Granualrity == BY immigrations year, month, arrival country, origin country
3. Sample scope based on immigration data: 
   * year=2016, 
   * month=4, 
   * origin country == (AUSTRALIA BRAZIL CANADA CHINA INDIA RUSSIA)  
   * arrival country == (UNITED STATES)
4. POC transforms: added 3 years to state temperatures to overlap immigration data - just to check the output

```sql
WITH country_temps as (select canon_country_name, year, month, avg(average_temp) as avg_temp
                       from state_temperatures
                       group by canon_country_name, year, month
)
select DISTINCT
    I.arrival_year as year,
    I.arrival_month as month,
    I.origin_country_name,
    C2.avg_temp as origin_temp,
    I.arrival_country_name,
    C1.avg_temp as arrival_temp,
    case when C2.avg_temp > C1.avg_temp
             then 'to-cooler-temp'
         else 'to-warmer-temp'
        end as direction
from immigration I -- 346627
         left join country_temps C1
                   on (I.arrival_country_name = C1.canon_country_name
                       and C1.year = I.arrival_year
                       and C1.month = I.arrival_month
                       )
         left join country_temps C2 on (I.origin_country_name = C2.canon_country_name
    and C2.year = I.arrival_year
    and C2.month = I.arrival_month
    )
;
```

| year | month | origin\_country\_name | origin\_temp | arrival\_country\_name | arrival\_temp | direction |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2016 | 4 | AUSTRALIA | 18.705875 | UNITED STATES | 12.007411764705878 | to-cooler-temp |
| 2016 | 4 | INDIA | 25.460470588235296 | UNITED STATES | 12.007411764705878 | to-cooler-temp |
| 2016 | 4 | BRAZIL | 25.19325 | UNITED STATES | 12.007411764705878 | to-cooler-temp |
| 2016 | 4 | CHINA | 14.182548387096777 | UNITED STATES | 12.007411764705878 | to-cooler-temp |
| 2016 | 4 | CANADA | -1.04 | UNITED STATES | 12.007411764705878 | to-warmer-temp |




