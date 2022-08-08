# CLEAN UPS and TRANSFORMATIONS DETAILS
- what arrival geos (countries, states, cities)  have all temperature data for any given origin
- what origin geos (country) have all temperature data for any given destination

--- 
### Create a closed sample
The first question is what part of the data can map immigrations to temperatures.
Verify that the data we are filtering does not impact the statistical validity of the data

1. filter out any arrivals or origins that are not in the countries that actually have temperature data
2. filter out only US destinations/arrivals 
3. filter out sas_countries that are DEFUNCT -- these immigrations records can not be assigned to a country or temperature
4. filter out any i94port code that don't conform to


-- normally I would save the whole data and count what was filtered


src/stage folder
- load_perpared_data.py
- load_control_data.py
- common.py

## MISSING AIRPORT CODES
- immigration sample data has only 2016, 4 as year, month
- to POC the solution bump the state_temperature year + 4 = so we have temperatures in 2016

#### find 'airports' that in the immigration data, but not in temperatures data

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

--------- in the immigration data, but not in temperature data
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

### after cleanup

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
- to POC the solution bump the state_temperature year + 4 = so we have temperatures in 2016


#### After cleanup - check immigration to airport consistency:
```sql
-- we use iata code to get to the arrival GEO data - check that it's complete
with immigration_countries as 
    (select distinct i94port, arrival_country_name, arrival_country from immigration )
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


### After cleanup, Map immigrations to state temperatures

```sql
--- correlate immigration arrival.country to temperature.canon_country_name
select   count(*)
from immigration I
    left join state_temperatures C on I.arrival_country_name = C.canon_country_name
where C.canon_country_name is null;
--- perfect - no records found


--- correlate immigration arrival.country to temperature.canon_country_name
select   count(*)
from immigration I
         left join state_temperatures C on I.origin_country_name = C.canon_country_name
where C.canon_country_name is null;
--- perfect - no records found

```


### Bad i94port/iata codes

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

- some ita codes in airports are '0' -- only about 106 and not US, so filter them out

- if the iata_code is null, replace it with the ident number -- gets come additional hits that way -- immigration data uses i94port == airport_codes.ident in a few cases

- closed airports have duplicated because 2 records for closed and open records with the same iata code,
    - can't just remove the
    - causing duplicates -
    - window on the iata_code over closed=T/F, and city_name

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


- Double cicid values - can't use as a key - but these are really duplicates - except that sme have more data than others
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
from immigration I
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



