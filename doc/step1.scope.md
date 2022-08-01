
## Step 1: Scope the Project and Gather Data
- Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows)
- Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

---


<br/>
<br/>



### Gather the data
I spent some time looking at other datasets, and my thinking was that I wanted to spend more time on engineering the data rather than experimenting with different datasets. 
I chose the Udacity Provided Project
I used some Udacity provided data, but added some additional files to help 

---


<br/>
<br/>


#### Raw data
Here are the raw datasets I am using:

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


---

- [ ] The pivotal data is i94immigrations (immigration)
- [ ] The GlobalLandTemperaturesByState (temperature) adds temperature data 
- [ ] Other data is used for augmenting the immigration and temperature data



#### <u>Describe immigration data</u>
The Department of Homeland Security (DHS) issues Form I-94, Arrival/Departure Record, to aliens who are:
- Admitted to the U.S.;
- Adjusting status while in the U.S.; or
- Extending their stay.
  This dataset is provided by Uadcity from the International Trade Admnistration `the https://www.trade.gov/national-travel-and-tourism-office`


#### <u>Describe temperature data</u>
There are a range of organizations that collate climate trends data. The three most cited land and ocean temperature data sets are NOAA’s MLOST, NASA’s GISTEMP and the UK’s HadCrut.
Kaggle has repackaged the data put together by the Berkeley Earth, which is affiliated with Lawrence Berkeley National Laboratory.
The Berkeley Earth Surface Temperature Study combines 1.6 billion temperature reports from 16 pre-existing archives.
It is nicely packaged and allows for slicing into interesting subsets (for example by country).



<br/>
<br/>
<br/>


---

### Usage and Scope

- After reviewing the data, the only viable way to use these two datasets is to try to correlate immigration to temperatures in the departure and arrival locations.

- The GOAL of this project is to be able to find any correlation between immigrations and temperatures in the departure (**origin**) and destination (**arrival**) countries.

- The OBJECTIVE of this project is create a POC (proof of concept) analytics table that proves that this is possible.


##### Questions to answer: 
> is there a correlation between temperature and immigration? e.g. are immigrants consistently coming from cooler to warmer locations or vis versa.

