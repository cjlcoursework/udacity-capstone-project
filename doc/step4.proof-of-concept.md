
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

| year | month | origin\_country\_name | origin\_temperature | arrival\_country\_name | arrival\_temperature | direction |
| :--- | :--- | :--- |:--------------------| :--- |:---------------------| :--- |
| 2016 | 4 | AUSTRALIA | 18.705875           | UNITED STATES | 12.007411764705878   | to-cooler-temp |
| 2016 | 4 | INDIA | 25.460470588235296  | UNITED STATES | 12.007411764705878   | to-cooler-temp |
| 2016 | 4 | BRAZIL | 25.19325            | UNITED STATES | 12.007411764705878   | to-cooler-temp |
| 2016 | 4 | CHINA | 14.182548387096777  | UNITED STATES | 12.007411764705878   | to-cooler-temp |
| 2016 | 4 | CANADA | -1.04               | UNITED STATES | 12.007411764705878   | to-warmer-temp |




