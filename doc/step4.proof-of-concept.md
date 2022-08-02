
### Immigration and Temperature Dataset


> Is there a correlation between temperature and immigration?  
>    Are people migrating from cooler to warmer locations or vis versa?
> 
>    This dataset contains an immigration table and a temperatures' table that can be joined together to find out

<br/>

---

#### Data Characteristics

* Granularity 
  * By year
  * By month
  * By country

<br/>

* Scope
  * i94 immigrations to the US forms from 1885 to present - new data is updated daily
  * The data has in filtered to just arrivals into the United States
  * The data is limited to immigration from the following countries
     - AUSTRALIA
     - BRAZIL
     - CANADA
     - CHINA
     - INDIA
     - RUSSIA
     - UNITED STATES
  
> Because of these limitations the data is not comprehensive.  
>   It can be used to assess the correlations between the countries above, but not all countries.  
>   It can be used to provide some statistical 

<br/>

---

##### Data Dictionary

*  [Data Dictionary](data_dictionary.md)

<br/>

---


#### Data storage structure
This collection contains two parquet databases residing on a public s3 bucket, and partitioned year, month, day, and hour that each record was ingested.

- immigration databbase
```text
├── immigration
       ├── controls
       │       ├── load_files     * --- a record of each file processed
       │       └── load_times     * --- a record of each load event - a file could be ingested more than once
       └── data                   * --- immigration database
           └── load_year=2022
               └── load_month=08
                   └── load_day=02
                       └── load_hour=02
                           ├── _SUCCESS
                           ├── part-00000-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
                           |                          ...
                           ├── part-00001-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
                           ├── part-00009-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
                           └── part-00010-e61fc6a1-c243-4eee-8995-53c68720c112-c000.snappy.parquet
```

- temperatures database

```text
└── temperature
    ├── controls
    │       ├── load_files    * --- a record of each file processed
    │       └── load_times    * --- a record of each load event - a file could be ingested more than once
    └── data                  * --- temperatures database
        └── load_year=2022
            └── load_month=08
                └── load_day=02
                    └── load_hour=02
                        ├── _SUCCESS
                        ├── part-00000-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
                        ├── part-00001-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
                        |                             ...
                        ├── part-00015-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
                        ├── part-00016-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet
                        └── part-00017-5793e75a-8c09-482e-992f-100aa26f60fc-c000.snappy.parquet

```

<br/>

---

### Database 
- The s3/parquet data above can be used with several database management systems:
  - Snowflake
  - Redshift Spectrum
  - Athena
  - Apache Spark / Jupyter
  - Others....

<br/>

---

### BI tools
- Data managed by the databases above can be accessed by almost all BI tools
- The BI tool would bring in the whoe dataset or a query, like the example below
  - Tableau
  - Looker
  - Quicksort
  - Ohers ...

<br/>

---

### Sample query and results

A typical BI tool like Tableau would expose this data as a set of atrributes and metrics.

**Attributes**
- origin_country_name
- destination_country_name
- direction
- ** Any of the additional i94immigration in the database dictionary

**Metrics**
- year
- month
- origin_temperature
- arrval_temperature

An example of a query that can be input into a BI tool or notebook 

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

| year | month  | origin\_country\_name | origin\_temperature   | arrival\_country\_name | arrival\_temperature     | direction         |
| :--- |:-------|:----------------------|:----------------------|:-----------------------|:-------------------------|:------------------|
| 2016 | 4      | AUSTRALIA             | 18.705875             | UNITED STATES          | 12.007411764705878       | to-cooler-temp    |
| 2016 | 4      | INDIA                 | 25.460470588235296    | UNITED STATES          | 12.007411764705878       | to-cooler-temp    |
| 2016 | 4      | BRAZIL                | 25.19325              | UNITED STATES          | 12.007411764705878       | to-cooler-temp    |
| 2016 | 4      | CHINA                 | 14.182548387096777    | UNITED STATES          | 12.007411764705878       | to-cooler-temp    |
| 2016 | 4      | CANADA                | -1.04                 | UNITED STATES          | 12.007411764705878       | to-warmer-temp    |




