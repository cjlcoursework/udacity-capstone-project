---- characterize -------
select 'airport_codes' as table_name, count(*) from airport_codes union all
select 'immigrations', count(*) from immigrations union all
select 'us_cities', count(*) from us_cities union all
select 'temperatures', count(*) from temperatures
;
----------
-- common-data
create table data_columns as
SELECT *
FROM information_schema.columns
WHERE table_schema = 'public'
;

---scope of temperature data
-- by country
select country_name, min(year), max(year), count(*) from state_temperatures group by country_name;
-- by state
select country_name, state_name, min(year), max(year), count(*) from state_temperatures group by country_name, state_name;



---
--- immigrations:
--     country_code: (eg. 209 = Mexico)
--     state_code (CA)
--     iana_code (HHW)
--     birth_year (1955)
--     month (4)
--     year (2016)

-- temperatures (provides average temperature along the following pivots):
--    city (Arhus)
--    country (Denmark)
--    month (4)
--    year (2016)
--    latitude
--    longitude


--- iana_names
        -- iana_code -> city_name-"ANCHORAGE", state_code="AK"

