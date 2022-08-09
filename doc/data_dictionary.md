| source       | attribute                | type      | description                                                                              |
|:-------------|:-------------------------|:----------|:-----------------------------------------------------------------------------------------|
| immigration  |                          |           |                                                                                          |  
|              | cicid                    | integer   | The year of the immigration record - probably the arrival year                           |
|              | i94yr                    | integer   | The year of the immigration record - probably the arrival year                           |
|              | i94mon                   | integer   | The month of the immigration record - probably the arrival year                          |
|              | i94cit                   | integer   | The sas code for the origin country                                                      |
|              | i94res                   | integer   | The sas code for the residence counrty                                                   |
|              | i94port                  | string    | The iata airport code of the arrival airport                                             |
|              | arrdate                  | integer   | Supposedly a sas numeric date of arrival                                                 |
|              | i94mode                  | integer   | 1=Air, 2=Sea, 3=Land, 9=Not reported                                                     |
|              | i94addr                  | string    | The sas code for the arrival address                                                     |
|              | depdate                  | integer   | Supposedly a sas numeric date of departure                                               |
|              | i94bir                   | integer   | Age of Respondent in Years                                                               |
|              | i94visa                  | integer   | 1=Business, 2=Pleasure, 3=Student                                                        |
|              | count                    | integer   | Garbage                                                                                  |
|              | dtadfile                 | string    | Character Date Field - Date added to I-94 Files - CIC does not use                       |
|              | visapost                 | string    | Department of State where where Visa was issued                                          |
|              | occup                    | string    | Occupation that will be performed in U.S.                                                |
|              | entdepa                  | string    | Arrival Flag - admitted or paroled into the U.S.                                         |
|              | entdepd                  | string    | Departure Flag - Departed, lost I-94 or is deceased                                      |
|              | entdepu                  | string    | Update Flag - Either apprehended, overstayed, adjusted to perm residence                 |
|              | matflag                  | string    | Match flag - Match of arrival and departure records                                      |
|              | biryear                  | integer   | 4 digit year of birth                                                                    |
|              | dtaddto                  | string    | Character Date Field - Date to which admitted to U.S.                                    |
|              | gender                   | string    | Male or Female                                                                           |
|              | insnum                   | string    | INS number                                                                               |
|              | airline                  | string    | Airline used to arrive in U.S.                                                           |
|              | admnum                   | number    | Admission Number                                                                         |
|              | fltno                    | string    | Flight number of Airline used to arrive in U.S.                                          |
|              | visatype                 | string    | Class of admission legally admitting the non-immigrant to temporarily stay in U.S.       |
|              | birth_year               | integer   | Calculated birth year - renamed                                                          |
|              | year                     | integer   | Renamed from i94yr                                                                       |
|              | month                    | integer   | Renamed from i94mon                                                                      |
|              | file_id                  | string    | An uuid4 key for the fie this record came from                                           |
|              | origin_country_name      | string    | text country name translated from i94cit                                                 |
|              | origin_country_code      | string    | text country code translated from i94cit                                                 |
|              | arrival_country          | string    | text country code translated from the i94port airport code                               |
|              | arrival_country_name     | string    | text country name translated from the i94port airport code                               |
|              | arrival_state_code       | string    | text state code translated from the i94port airport code                                 |
|              | arrival_city_name        | string    | text state code translated from the i94port airport code                                 |
| temperatures |                          |           |                                                                                          |
|              | dt                       | timestamp | date of the temperature reading                                                          |
|              | average_temp             | double    | the average temperature reading for the country over the month                           |
|              | average_temp_uncertainty | double    | not used                                                                                 |
|              | state_name               | string    | text state code                                                                          |
|              | year                     | integer   | year of the temperature reading                                                          |
|              | month                    | integer   | month of the temperature reading                                                         |
|              | country_name             | string    | country of the temperature reading                                                       |
|              | canon_country_name       | string    | canonical country of the temperature reading                                             |
|              | file_id                  | string    | An uuid4 key for the fie this record came from                                           |
| files        |                          |           |                                                                                          |  
|              | input_file               | string    | The full path of an input file                                                           |  
|              | dataset                  | string    | either immigration or temperatures data set                                              |  
|              | file_id                  | string    | a uuid4 key for the file                                                                 |  
|              | file_name                | string    | The base files name without the directory or prefix                                      |  
| loads        |                          |           |                                                                                          |  
|              | date                     | timestamp | The date a file was loaded - normally the file was loaded once, but there can be reloads |  
|              | file_id                  | string    | a uuid4 key for the file                                                                 |  
|              | load_type                | string    | an optional reason for the load on this date                                             |  
