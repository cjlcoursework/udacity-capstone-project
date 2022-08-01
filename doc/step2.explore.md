### Step 2. Explore and Assess the Data
- Explore the data to identify data quality issues, like missing values, duplicate data, etc.
- Document steps necessary to clean the data


---

#### Approach
1. read and insert a sample of the data into Postgres to understand the completeness and correlation of the data
2. Transform and query to determine completeness, data quality and 
3. Bring in some additional datasets to help fill out geo-location information

---
<br/>


> Create Postgres tables from raw and query, and bring in more data to fill in the gaps

| data                                     | table                                        |
|:-----------------------------------------|:---------------------------------------------|
| <u>GlobalLandTemperaturesByState.csv</u> | temperature                                  |
| <u>sas_data</u>                          | immigration                                  |
| I94_SAS_Labels_Descriptions.SAS          | add origin country to immigration table      |
| airport-codes_csv.csv                    | add country information to immigration table |
| us-cities-demographics.csv               | not used yet                                 |
| country_codes.csv                        | add country information to immigration table |
| state_names.csv                          | not used yet                                 |
| country_codes.csv                        | augment the airport information              |

---

> Create some light transformations

| data                    | from                                                               |
|:------------------------|:-------------------------------------------------------------------|
| sas_countries_raw.csv   | format to csv from  I94_SAS_Labels_Descriptions.SAS                |
| sas_countries.csv       | join sas_countries_raw.csv and country_codes.csv on country_name   |
| airport-codes.csv       | airport-codes_csv.csv with some 'missing' airports                 |

<br/>
<br/>

---

> Document steps necessary to clean the data  

* [immigration cleanup notes](step2.labnotes1.md)

* [defect cleanup details](step2.labnotes2.md)

<br/>
<br/>

---

> Set the SCOPE of the solution based on the data


- The initial solution will focus on <u>just arrivals into the UNITED STATES</u> 
  - the world-cities data I could find would take a lot of work to conform, and we might as well start with one destination as a POC

---

- The departure countries are also limited to a few major cities, because that's all the GlobalLandTemperaturesByState.csv data contains
    - AUSTRALIA
    - BRAZIL
    - CANADA
    - CHINA
    - INDIA
    - RUSSIA
    - UNITED STATES

---

- The granularity is at the <u>country</u> level, because
    - the `i94cit` column is only at the country level
    - the `i94port` arrival data could be down to the city level, but enriching and conforming both datasets to the city or state level would require a lot of additional work - so we leave this as an improvement.

---

- the sas dates provided don't make sense to me -- we should be able to get to a date using EPOCH + sas_date in seconds  == DATE
  - `depdate` does not look like a real sas date (?)
  - `arrdate` ddes not look like a real sas date
  - we are left with `i94yr` and `i94mon` (year and month) for the time axis




