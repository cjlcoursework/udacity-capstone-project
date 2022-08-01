# CLEAN UPS and TRANSFORMATIONS DETAILS
- what arrival geos (countries, states, cities)  have all temperature data for any given origin
- what origin geos (country) have all temperature data for any given destination

-------------------------------------------------------------
> DETERMINE THE EFFECT OF FILTERING ON IMMIGRATIONS DATA
-------------------------------------------------------------
##### Summary
- 386,189 records in immigration data do not have a corresponding entry in the sas index
- 87% completeness

  - we can't really do much about missing sas index data, so (1- 386,189/3,096,313) == `87% completeness`
  - not all of these are US countries, so we are better than 87% once we remove the non-US destinations

##### Calculations
`select sas_code from controls.sas_index where sas_code;` = '0'

---

`select count(*) from immigration_raw where i94cit is null;`  == 0

---

`select count(*) from controls.sas_index where sas_index.sas_code is null;`  == 0

---

`select count(*) from immigration_raw;`  == 3,096,313

---

`select count(*) from immigration_raw I
join controls.sas_index S on S.sas_code = I.i94cit
;
`
== 2,710,124  (3,096,313 - 2,710,124 = 386,189)

---

`select count(*) from immigration_raw I
join controls.sas_index S on S.sas_code = I.i94cit
;`

== completed joins: 2,710,124

---

`select count(*) from immigration_raw I
left join controls.sas_index S on S.sas_code = I.i94cit
where S.sas_code is null
;`
 == 386,189 records

---

`select distinct i94cit from immigration_raw I
left join controls.sas_index S on S.sas_code = I.i94cit
where S.sas_code is null
;`
== 386,189

---

##### Outcome `87% completeness`

- There are 386,189 records in immigration that do not have a corresponding entry in the sas index (I94_SAS_Labels_Descriptions.SAS)
  - can't really do much about missing sas index data, so (1- 386,189/3,096,313) == `87% completeness`
  - not all of these are US countries, so we are better than 87% once we remove the non-US destinations


---


-------------------------------------------------------------
> DETERMINE THE EFFECT OF FILTERING ON AIRPORTS DATA
-------------------------------------------------------------
##### Summary: 
- 45,525 out of 55,316 i94ports in immigration data are not in the airports data
- 82.7% completeness

##### Calculations

-- immigration i94port maps to iata codes from the airport data
`select count(*) from immigration_raw I
join airports_raw S on S.iata_code = I.i94port
;`
== 2,569,455 

---

`select count(*) from immigration_raw I
left join airports_raw S on S.iata_code = I.i94port
where S.iata_code is null
;`
 == 534,728

- 1 - (534,728/3,096,313) == 82.7% completeness -- so 17% of the immigration i94cit do not map to airport iata codes

---

-- airports
`select count(*) from airports_raw R
left join controls.airport_codes A on A.name = R.name;`
== 55,316 records

---

`select count(*) from airports_raw R
left join controls.airport_codes A on A.name = R.name
where A.name is null;`
== 45,525

---

`select distinct R.iata_code from airports_raw R
left join controls.airport_codes A on A.name = R.name
where A.name is null;`
== 64 distinct airport codes that were filtered out

---

-------------------------------------------------------------
> FINAL IMMIGRATION DATASET 
-------------------------------------------------------------

##### Summary
- 2,242,026 immigration records can be matched to valid countries and airports = 72% completeness
- But we then deliberately scope down to US arrivals from countries that are found in the temperature data
- leaving only 346,627 effective records which is 11% of the total immigration data

##### Calculations

`select I.i94port, I.i94cit, S.iso_country from immigration_raw I
join airports_raw S on S.iata_code = I.i94port;`
== 2,569,455 records

- BUT , only the following countries have temperature data ( 'AU', 'BR',  'CA', 'CN','IN', 'RU',   'US' )
  - so the additional immigrations data
  - does not work for this dataset - we could leave it in there for future use, but I am going to take it out

`select count(*) from immigration_raw I
join airports_raw S on S.iata_code = I.i94port
where iso_country in ( 'AU',
'BR',
'CA',
'CN',
'IN',
'RU',
'US' );`
= 2,028,371  ---  we only want US arrivals


#### final talley
-    (the joins cause some fan-out so I am windowing over cicid to get just the distinct records)
-    select DISTINCT will not work and would be too slow

#### filters off, joins on returns 2,242,026 records
```sql
WITH DUPS as (SELECT row_number() OVER (PARTITION BY I.cicid ORDER BY A.city_name) as rownumber,
I.cicid,
I.i94yr,
I.i94mon,
I.i94cit,
I.i94res,
I.i94port,
I.arrdate,
I.i94mode,
I.i94addr,
I.depdate,
I.i94bir,
I.i94visa,
I.count,
I.dtadfile,
I.visapost,
I.occup,
I.entdepa,
I.entdepd,
I.entdepu,
I.matflag,
I.biryear,
I.dtaddto,
I.gender,
I.insnum,
I.airline,
I.admnum,
I.fltno,
I.visatype,
I.birth_year,
upper(S.canon_country_name)                                   as origin_country_name,
upper(S.country_code2)                                        as origin_country_code,
A.country_code2                                               as arrival_country,
upper(A.country_name)                                         as arrival_country_name,
A.state_code                                                  as arrival_state_code,
A.city_name                                                   as arrival_city_name
FROM immigration_raw I
join controls.sas_countries S on S.sas_code = I.i94cit
join controls.airport_codes A on A.iata_code = I.i94port
--                where A.country_code2 = 'US'
--                and S.canon_country_name in ( 'AUSTRALIA',
--                                               'BRAZIL',
--                                               'CANADA',
--                                               'CHINA',
--                                               'INDIA',
--                                               'RUSSIA',
--                                               'UNITED STATES' )
)
SELECT count(*)
FROM DUPS
WHERE rownumber = 1;
```

- without the country filters we have 2,242,026 records before filtering arrival and origin countries the country filters
-- just the joins are in play here

#### filters off AND joins aff as well (left join instead of join) returns all records == 3,096,313

```sql
WITH DUPS as (SELECT row_number() OVER (PARTITION BY I.cicid ORDER BY A.city_name) as rownumber,
I.cicid,
I.i94yr,
I.i94mon,
I.i94cit,
I.i94res,
I.i94port,
I.arrdate,
I.i94mode,
I.i94addr,
I.depdate,
I.i94bir,
I.i94visa,
I.count,
I.dtadfile,
I.visapost,
I.occup,
I.entdepa,
I.entdepd,
I.entdepu,
I.matflag,
I.biryear,
I.dtaddto,
I.gender,
I.insnum,
I.airline,
I.admnum,
I.fltno,
I.visatype,
I.birth_year,
upper(S.canon_country_name)                                   as origin_country_name,
upper(S.country_code2)                                        as origin_country_code,
A.country_code2                                               as arrival_country,
upper(A.country_name)                                         as arrival_country_name,
A.state_code                                                  as arrival_state_code,
A.city_name                                                   as arrival_city_name
FROM immigration_raw I
left join controls.sas_countries S on S.sas_code = I.i94cit
left join controls.airport_codes A on A.iata_code = I.i94port
--                where A.country_code2 = 'US'
--                and S.canon_country_name in ( 'AUSTRALIA',
--                                               'BRAZIL',
--                                               'CANADA',
--                                               'CHINA',
--                                               'INDIA',
--                                               'RUSSIA',
--                                               'UNITED STATES' )
)
SELECT count(*)
FROM DUPS
WHERE rownumber = 1;
```
---
#### final results

- left joining returns all records == 3,096,313
- `controls.sas_countries` -- this lets in i94cit data that we can't match to a "good" sas index (e.g. 996 =  'No Country Code (996)')
- `controls.airport_codes` -- this lets in airport codes that we cannot map to countries in the airport data
  - these are generally tiny municipal airports

> so in the immigration data we are filtering out 'bad' airports and countries
>     leaving us 2,242,026 out of 3,096,313 records == 72.4% of the data that we can map to country data


but we deliberately filter out some countries leaving only 346,627 immigrations to the US from that we can map to temperature data
-     * US arrivals
-      * origins available in the temperatures data ( 'AU', 'BR',  'CA', 'CN','IN', 'RU',   'US' )


```sql
WITH DUPS as (SELECT row_number() OVER (PARTITION BY I.cicid ORDER BY A.city_name) as rownumber,
I.cicid,
I.i94yr,
I.i94mon,
I.i94cit,
I.i94res,
I.i94port,
I.arrdate,
I.i94mode,
I.i94addr,
I.depdate,
I.i94bir,
I.i94visa,
I.count,
I.dtadfile,
I.visapost,
I.occup,
I.entdepa,
I.entdepd,
I.entdepu,
I.matflag,
I.biryear,
I.dtaddto,
I.gender,
I.insnum,
I.airline,
I.admnum,
I.fltno,
I.visatype,
I.birth_year,
upper(S.canon_country_name)                                   as origin_country_name,
upper(S.country_code2)                                        as origin_country_code,
A.country_code2                                               as arrival_country,
upper(A.country_name)                                         as arrival_country_name,
A.state_code                                                  as arrival_state_code,
A.city_name                                                   as arrival_city_name
FROM immigration_raw I
left join controls.sas_countries S on S.sas_code = I.i94cit
left join controls.airport_codes A on A.iata_code = I.i94port
where A.country_code2 = 'US'
and S.canon_country_name in ( 'AUSTRALIA',
'BRAZIL',
'CANADA',
'CHINA',
'INDIA',
'RUSSIA',
'UNITED STATES' )
)
SELECT count(*)
FROM DUPS
WHERE rownumber = 1
```
==  346,627 records
