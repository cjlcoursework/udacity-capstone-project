import datetime
from typing import Tuple, Type, Union, Any

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.stage.common import write_to_postgres
from config import PYSPARK_EXTENDED_JARS


def get_sas_countries(spark: SparkSession) -> DataFrame:
    path = '../../data/rawdata/controls/sas_countries.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df.filter(df.sas_status != 'DEFUNCT') \
        .withColumn("canon_country_name", F.upper(regexp_extract('country_name', r'[^\,]*', 0)))


def get_country_codes(spark: SparkSession) -> DataFrame:
    path = '../../data/rawdata/controls/country_codes.csv'

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
    path = '../../data/rawdata/controls/airport-codes.csv'

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


def load_cities(spark: SparkSession, format: str, path: str, table_name: str):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("sep", ";") \
        .parquet(path)

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


# ---test only --
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # ---control tables -- insert to postgres for inspection ONLY - we are not really using the postgres tables
    write_to_postgres(get_airports(spark), table_name="controls.airport_codes")
    write_to_postgres(get_sas_countries(spark), table_name="controls.sas_countries")
    write_to_postgres(get_country_codes(spark), table_name="controls.country_codes")
