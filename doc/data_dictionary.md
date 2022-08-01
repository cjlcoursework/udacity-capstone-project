| source       | attribute                | description                                                                              |
|:-------------|:-------------------------|:-----------------------------------------------------------------------------------------|
| immigration  |                          |                                                                                          |  
|              | i94yr                    | The year of the immigration record - probably the arrival year                           |
|              | i94mon                   | The month of the immigration record - probably the arrival year                          |
|              | i94cit                   | The sas code for the origin country                                                      |
|              | i94res                   | The sas code for the residence counrty                                                   |
|              | i94port                  | The iata airport code of the arrival airport                                             |
|              | arrdate                  | Supposedly a sas numeric date of arrival                                                 |
|              | i94mode                  | 1=Air, 2=Sea, 3=Land, 9=Not reported                                                     |
|              | i94addr                  | The sas code for the arrival address                                                     |
|              | depdate                  | Supposedly a sas numeric date of departure                                               |
|              | i94bir                   | Age of Respondent in Years                                                               |
|              | i94visa                  | 1=Business, 2=Pleasure, 3=Student                                                        |
|              | count                    | Garbage                                                                                  |
|              | dtadfile                 | Character Date Field - Date added to I-94 Files - CIC does not use                       |
|              | visapost                 | Department of State where where Visa was issued                                          |
|              | occup                    | Occupation that will be performed in U.S.                                                |
|              | entdepa                  | Arrival Flag - admitted or paroled into the U.S.                                         |
|              | entdepd                  | Departure Flag - Departed, lost I-94 or is deceased                                      |
|              | entdepu                  | Update Flag - Either apprehended, overstayed, adjusted to perm residence                 |
|              | matflag                  | Match flag - Match of arrival and departure records                                      |
|              | biryear                  | 4 digit year of birth                                                                    |
|              | dtaddto                  | Character Date Field - Date to which admitted to U.S.                                    |
|              | gender                   | Male or Female                                                                           |
|              | insnum                   | INS number                                                                               |
|              | airline                  | Airline used to arrive in U.S.                                                           |
|              | admnum                   | Admission Number                                                                         |
|              | fltno                    | Flight number of Airline used to arrive in U.S.                                          |
|              | visatype                 | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.       |
|              | birth_year               | Calculated birth year - renamed                                                          |
|              | year                     | Renamed from i94yr                                                                       |
|              | month                    | Renamed from i94mon                                                                      |
|              | file_id                  | An uuid4 key for the fie this record came from                                           |
|              | origin_country_name      | text country name translated from i94cit                                                 |
|              | origin_country_code      | text country code translated from i94cit                                                 |
|              | arrival_country          | text country code translated from the i94port airport code                               |
|              | arrival_country_name     | text country name translated from the i94port airport code                               |
|              | arrival_state_code       | text state code translated from the i94port airport code                                 |
|              | arrival_city_name        | text state code translated from the i94port airport code                                 |
| temperatures |                          |                                                                                          |
|              | dt                       | date of the temperature reading                                                          |
|              | average_temp             | the average temperature reading for the country over the month                           |
|              | average_temp_uncertainty | not used                                                                                 |
|              | state_name               | text state code                                                                          |
|              | year                     | year of the temperature reading                                                          |
|              | month                    | month of the temperature reading                                                         |
|              | country_name             | country of the temperature reading                                                       |
|              | canon_country_name       | canonical country of the temperature reading                                             |
|              | file_id                  | An uuid4 key for the fie this record came from                                           |
| files        |                          |                                                                                          |  
|              | input_file               | The full path of an input file                                                           |  
|              | dataset                  | either immigration or temperatures data set                                              |  
|              | file_id                  | a uuid4 key for the file                                                                 |  
|              | file_name                | The base files name without the directory or prefix                                      |  
| loads        |                          |                                                                                          |  
|              | date                     | The date a file was loaded - normally the file was loaded once, but there can be reloads |  
|              | file_id                  | a uuid4 key for the file                                                                 |  
|              | load_type                | an optional reason for the load on this date                                             |  
