import datetime
from typing import Tuple, Type, Union, Any

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.stage.common import create_files_log, write_to_postgres
from src.stage.load_control_data import get_sas_countries, get_airports

LOAD_CONTROLS_POSTGRES = False
LOAD_RAW_POSTGRES = False
LOAD_RAW_IMMIGRATION_PARQUET = True
LOAD_RAW_TEMPERATURE_PARQUET = False

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


def load_immigrations(spark: SparkSession, format: str, path: str, table_name: str):
    contry_ndx_df = get_sas_countries(spark=spark)
    contry_ndx_df.createOrReplaceTempView("sas_countries")

    airports_df = get_airports(spark=spark)
    airports_df.createOrReplaceTempView("airport_codes")

    df = spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(path)

    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("input_file", input_file_name()) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType()))

    files_df = create_files_log(df=df, table_name=table_name)
    files_df.createOrReplaceTempView("files")

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
             I.year,
             I.month,
             L.file_id,
             upper(S.canon_country_name)                                   as origin_country_name,
             upper(S.country_code2)                                        as origin_country_code,
             A.country_code2                                               as arrival_country,
             upper(A.country_name)                                         as arrival_country_name,
             A.state_code                                                  as arrival_state_code,
             A.city_name                                                   as arrival_city_name
      FROM next_immigrations I
               join sas_countries S on S.sas_code = I.i94cit
               join airport_codes A on A.iata_code = I.i94port
               left join files L on L.input_file = I.input_file
      where A.country_code2 = 'US'
        and S.canon_country_name in {COUNTRIES_WITH_TEMPERATURE_DATA}
    )
    SELECT *
    FROM DUPS 
    WHERE rownumber = 1 """)

    write_to_postgres(files_df, table_name="controls.load_files", mode="append")
    write_to_postgres(temps_df, table_name=table_name)
    if LOAD_RAW_POSTGRES:
        write_to_postgres(df, table_name=f"raw_{table_name}")


def load_temperature_data_by_state(spark: SparkSession, format: str, path: str, table_name: str):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("input_file", input_file_name()) \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumn("canon_country_name", F.upper("country_name"))
    df.createOrReplaceTempView("temperatures")

    files_df = create_files_log(df=df, table_name=table_name)
    files_df.createOrReplaceTempView("files")

    temps_df = spark.sql(f"""
    SELECT 
        T.dt,
        T.average_temp,
        T.average_temp_uncertainty,
        T.state_name,
        T.timestamp,
        T.year,
        T.month,
        T.country_name,
        T.canon_country_name,    
        L.file_id
    FROM temperatures T 
     join files L on L.input_file = T.input_file
     """)

    write_to_postgres(files_df, table_name="controls.load_files", mode="append")
    write_to_postgres(temps_df, table_name=table_name)
    if LOAD_RAW_POSTGRES:
        write_to_postgres(df, table_name=f"raw_{table_name}")


def load_prep_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # -- core data - these use the control tables internally to extend the tables
    load_temperature_data_by_state(spark=spark, format="csv",
                                   path='../../data_test/temperature',
                                   table_name="state_temperatures")

    load_immigrations(spark=spark, format="parquet", path='../../data_test/immigration',
                      table_name="immigration")


if __name__ == "__main__":
    load_prep_data()
