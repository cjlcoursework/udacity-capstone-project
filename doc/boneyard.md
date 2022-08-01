
### INPUT: partitioned datasets
1. table_name: `immigrations`  <-- `immigration/year=*/month=*/i94port=*/<csv files>`
2. table_name: `state_temperatures`  <-- `temperature/year=*/month=*/Country=*/<csv files>`

### OUTPUT: "Gold" datasets
1. table_name: `immigrations`  <-- `immigration/year=*/month=*/i94port=*/<csv files>`
2. table_name: `state_temperatures`  <-- `temperature/year=*/month=*/Country=*/<csv files>`
2. table_name: `state_temperatures`  <-- `temperature/year=*/month=*/Country=*/<csv files>`

### OUTPUT: "control" datasets
1. Each job integrates the gold data with a load_files control table
    1. validate that all records from each input file was processed into the gold dataset







### Raw data
| data                                     | from                                                                                                                                           |
|:-----------------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------|
| <u>GlobalLandTemperaturesByState.csv</u> | https://www.kaggle.com/datasets/sohelranaccselab/global-climate-change                                                                         |
| <u>sas_data</u>                          | immigration data provided by Udacity from the International Trade Admnistration `the https://www.trade.gov/national-travel-and-tourism-office` |
| I94_SAS_Labels_Descriptions.SAS          | sas_data dictionary provided by Udacity                                                                                                        |
| airport-codes_csv.csv                    | provided by Udacity                                                                                                                            |
| us-cities-demographics.csv               | provided by Udacity (not used yet)                                                                                                             |
| country_codes.csv                        | upload from: https://gist.github.com/radcliff/f09c0f88344a7fcef373                                                                             |
| state_names.csv                          | upload from: https://worldpopulationreview.com/states/state-abbreviations                                                                      |
| country_codes.csv                        | https://gist.github.com/radcliff/f09c0f88344a7fcef373                                                                                          |


#### Light transformations
| data                    | from                                                               |
|:------------------------|:-------------------------------------------------------------------|
| sas_countries_raw.csv   | format to csv from  I94_SAS_Labels_Descriptions.SAS                |
| sas_countries.csv       | join sas_countries_raw.csv and country_codes.csv on country_name   |
| airport-codes.csv       | airport-codes_csv.csv with some 'missing' airports                 |


#### Airport data
The airport data provided is missing some key US iata* airports that unless added would have lost a lot of records.   These weer manually added

| iata-port | immigration records |
|:----------|:--------------------|
| NYC       | 485,953             |
| HHW       | 142,720             |
| CHI       | 130,567             |
| FTL       | 95,977              |
| LVG       | 89,87               |
| WAS       | 74,858              |


#### immigration data

The "sample" immigration data only contains data from April 2016, but the GlobalLandTemperaturesByState data goes only to 2015. To prove the concept - I added 4 years to the dates, so the data would match.
- The sas-codes index can be used to get a country name from the numeric id in the data (e.g. i94cit, i94res) - this gets us the ORIGIN country
- The Airport data can be used to map the i94port to the iata code - then to the country, state, city information - this gets us the geo-location for the ARRIVAL country, state, city



