import datetime
from typing import Tuple, Type, Union, Any

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

LOAD_CONTROLS_POSTGRES = False
LOAD_RAW_POSTGRES = False
LOAD_RAW_IMMIGRATION_PARQUET = False
LOAD_RAW_TEMPERATURE_PARQUET = True

ROOT_FOLDER = '/Users/christopherlomeli/Source/courses/udacity/data-engineer/udacity-capstone-project'
SUPPORTED_LOCATIONS = [
    "AUSTRALIA",
    "BRAZIL",
    "CANADA",
    "CHINA",
    "INDIA",
    "RUSSIA",
    "UNITED STATES"
]

COUNTRIES_WITH_TEMPERATURE_DATA = """( 'AUSTRALIA',
    'BRAZIL',
    'CANADA',
    'CHINA',
    'INDIA',
    'RUSSIA',
    'UNITED STATES' )"""

# --------------------  Global Spark Session ---------------------
from config import PYSPARK_EXTENDED_JARS


#
# def from_sas_date(sas_date: int) -> datetime:
#     return datetime.datetime(1960,1,1) + datetime.timedelta(seconds=sas_date)
#
#
# def from_sas_date(sas_date: int):
#     epoch = datetime.datetime(2016, 4, 22)
#     return epoch + datetime.timedelta(seconds=sas_date)


def load_immigrations(spark: SparkSession, format: str, path: str, table_name: str):
    contry_ndx_df = get_sas_countries(spark=spark)
    contry_ndx_df.createOrReplaceTempView("sas_countries")

    airports_df = get_airports(spark=spark)
    airports_df.createOrReplaceTempView("airport_codes")

    df = spark.read.format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("arrival_year", regexp_extract('i94yr', r'\d*', 0).cast(IntegerType())) \
        .withColumn("arrival_month", regexp_extract('i94mon', r'\d*', 0).cast(IntegerType()))

    df.createOrReplaceTempView("next_immigrations")

    # extract columns to create songs table
    #  exclude immigration countries with no sas code -- none or very few records
    #  exclude immigrations to tiny airports (See bad airports above)
    #  exclude arrivals to non-US -- we'll start with US then add other destinations as needed
    #  So this becomes immigrations to the US from SUPPORTED_LOCATIONS (See variable above)
    # the distinct might not be performant
    # this join causes a fan-out, so dedup using partition on the cicid key
    # todo - improve performance by getting rid of the partition by? (find the fan-out)
    temps_df = spark.sql(f"""
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
             I.arrival_year,
             I.arrival_month,
             upper(S.canon_country_name)                                   as origin_country_name,
             upper(S.country_code2)                                        as origin_country_code,
             A.country_code2                                               as arrival_country,
             upper(A.country_name)                                         as arrival_country_name,
             A.state_code                                                  as arrival_state_code,
             A.city_name                                                   as arrival_city_name
      FROM next_immigrations I
               join sas_countries S on S.sas_code = I.i94cit
               join airport_codes A on A.iata_code = I.i94port
      where A.country_code2 = 'US'
        and S.canon_country_name in {COUNTRIES_WITH_TEMPERATURE_DATA}
    )
    SELECT *
    FROM DUPS 
    WHERE rownumber = 1 """)

    write_to_postgres(temps_df.withColumnRenamed("_c0", "id"), table_name=table_name)
    if LOAD_RAW_POSTGRES:
        write_to_postgres(df.withColumnRenamed("_c0", "id"), table_name=f"raw_{table_name}")
    if LOAD_RAW_IMMIGRATION_PARQUET:
        df.write.partitionBy("arrival_year", "arrival_month", "i94port") \
            .mode("overwrite") \
            .parquet("../data/output/immigration/")


def load_cities(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("sep", ";") \
        .load(path)

    df = df \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("Number of Veterans", "veterans_population") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("Average Household Size", "avg_household_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Count", "count")

    write_to_postgres(df, table_name=table_name)


def load_temperature_data_by_state(spark: SparkSession, format: str, path: str, table_name: str):

    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("year", F.year(F.col("timestamp"))) \
        .withColumn("month", F.month(F.col("timestamp"))) \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumn("canon_country_name", F.upper("country_name"))

    # df.createOrReplaceTempView("temps")

    # extract columns to create songs table
    # temps_df = spark.sql("""
    #     SELECT T.*,
    #       case when S.state_code is not null then
    #        upper(S.state_code)
    #       else 'N/A'
    #       end as canon_state_code
    #     FROM temps T
    #     right join states S on S.state_name = T.state_name
    #      """)

    write_to_postgres(df, table_name=table_name)
    if LOAD_RAW_POSTGRES:
        write_to_postgres(df, table_name=f"raw_{table_name}")
    if LOAD_RAW_TEMPERATURE_PARQUET:
        df.filter("year > 2000")\
            .withColumn("year", df.year + 4)\
            .write.partitionBy("year", "month", "country_name") \
            .mode("overwrite") \
            .parquet("../data/output/temperature")


def load_temperature_data_by_country(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    # dt,AverageTemperature,AverageTemperatureUncertainty,Country

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("arrival_year", F.year(F.col("timestamp"))) \
        .withColumn("arrival_month", F.month(F.col("timestamp"))) \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty")

    write_to_postgres(df, table_name=table_name)


def get_state_names(spark: SparkSession) -> DataFrame:
    path = '../data/controls/state_names.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df


def get_world_cities(spark: SparkSession) -> DataFrame:
    path = '../data/controls/world-cities.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df


def get_sas_countries(spark: SparkSession) -> DataFrame:
    path = '../data/controls/sas_countries.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df.filter(df.sas_status != 'DEFUNCT') \
        .withColumn("canon_country_name", F.upper(regexp_extract('country_name', r'[^\,]*', 0)))


def get_country_codes(spark: SparkSession) -> DataFrame:
    path = '../data/controls/country_codes.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)
    """
    country_name,country_code2,country_code3,country_index,country_iso
    Afghanistan,AF,AFG,004,ISO 3166-2:AF
    """
    return df \
        .withColumn("canon_country_name", F.upper('country_name')) \
        .withColumn("canon_country_code2", F.upper('country_code2')) \
        .withColumn("canon_country_code3", F.upper('country_code3'))


def get_airports(spark: SparkSession) -> DataFrame:
    path = '../data/controls/airport-codes.csv'

    countries_df = get_country_codes(spark=spark)
    countries_df.createOrReplaceTempView("country_lookup")

    df = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    closed_type = F.udf(lambda x: x == 'closed')

    df = df \
        .filter("iata_code != '0'") \
        .withColumn("is_closed", closed_type(df.type)) \
        .withColumn("state_code", F.upper(regexp_extract('iso_region', r'(?<=-)(\w*)', 0))) \
        .withColumn("country_code2", F.upper(regexp_extract('iso_region', r'(\w*)(?=-)', 0))) \
        .withColumn("latitude", regexp_extract('coordinates', r'^[^\,]*', 0).cast(DoubleType())) \
        .withColumn("longitude", regexp_extract('coordinates', r'(?<=,\s).*$', 0).cast(DoubleType())) \
        .withColumnRenamed("municipality", "city_name")

    df.createOrReplaceTempView("airports")

    # extract columns to create songs table
    airports_df = spark.sql("""
        WITH  X AS (
            select
                   row_number() OVER (PARTITION BY A.iata_code
                       ORDER BY is_closed, city_name ) AS rownumber
                        , ident
                        , type
                        , name
                        , elevation_ft
                        , continent
                        , iso_country
                        , iso_region
                        , city_name
                        , gps_code
                        , iata_code
                        , local_code
                        , coordinates
                        , state_code
                        , A.country_code2
                        , latitude
                        , longitude
                    , coalesce(A.iata_code, A.ident) as canon_iata_code
                    , C.country_name as country_name
                    , C.country_code3 as country_code3
            FROM airports A
                JOIN country_lookup C on C.country_code2 = A.country_code2
        )
        select
            ident
             , type
             , name
             , elevation_ft
             , continent
             , iso_country
             , iso_region
             , city_name
             , gps_code
             , iata_code
             , local_code
             , coordinates
             , state_code
             , country_code2
             , latitude
             , longitude
             , canon_iata_code
             , country_name
             , country_code3
        FROM X WHERE rownumber = 1
         """)
    return airports_df


def write_to_postgres(df: DataFrame, table_name: str):
    df.write.format("jdbc").mode("overwrite") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # ---control tables -- insert to postgres for inspection ONLY - we are not really using the postgres tables
    if LOAD_CONTROLS_POSTGRES:
        write_to_postgres(get_airports(spark), table_name="controls.airport_codes")
        write_to_postgres(get_sas_countries(spark), table_name="controls.sas_countries")
        write_to_postgres(get_state_names(spark), table_name="controls.state_names")
        write_to_postgres(get_country_codes(spark), table_name="controls.country_codes")

    # -- core data - these use the control tables internally to extend the tables
    load_temperature_data_by_state(spark=spark, format="csv",
                                   path='../data/temperatures/GlobalLandTemperaturesByState.csv',
                                   table_name="state_temperatures")

    load_immigrations(spark=spark, format="parquet", path='../data/udacity/sas_data',
                      table_name="immigration")

    load_temperature_data_by_country(spark=spark, format="csv",
                                     path=f'../data/temperatures/GlobalLandTemperaturesByCountry.csv',
                                     table_name="country_temperatures")

    # load_cities(spark=spark, format="csv", path='../data/udacity/us-cities-demographics.csv', table_name="us_cities",
    #             header='True', sep=";")

    # load_temperature_data_by_city(spark=spark, format="csv",
    #                               path='../data/temperatures/SampleGlobalLandTemperaturesByCity.csv',
    #                               table_name="city_temperatures")
    #


if __name__ == "__main__":
    load_core_data()
