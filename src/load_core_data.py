import datetime
from typing import Tuple, Type, Union, Any

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


ROOT_FOLDER='/Users/christopherlomeli/Source/courses/udacity/data-engineer/udacity-capstone-project'


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


def load_immigrations(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    """
    i94yr = 4 digit year
    i94mon = numeric month
    i94cit = 3 digit code of origin city
    i94port = 3 character code of destination city
    arrdate = arrival date
    i94mode = 1 digit travel code
    depdate = departure date
    i94visa = reason for immigration
    AverageTemperature = average temperature of destination c
    """

    iata_df = load_city_names(spark=spark, path='../data/controls/city_codes.csv')  # todo move to accumulator

    df = spark.read.format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    """
    cit, res, port
    """
    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("destination_iata_code", df.i94port) \
        .withColumn("canon_state_code", F.lower(df.i94addr)) \
        .withColumn("origin_country_index", regexp_extract('i94cit', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("year", regexp_extract('i94yr', r'\d*', 0).cast(IntegerType())) \
        .withColumn("month", regexp_extract('i94mon', r'\d*', 0).cast(IntegerType()))

    df.createOrReplaceTempView("immigration")
    iata_df.createOrReplaceTempView("city")

    # sas_code,country_name,canon_country_name
    # 245,"CHINA, PRC"

    # extract columns to create songs table
    temps_df = spark.sql("""
        SELECT I.*, 
            lower(S.canon_country_name) as origin_canon_country_name
        FROM immigration I
        join city S on S.sas_code = I.i94cit
         """)


    write_to_postgres(temps_df.withColumnRenamed("_c0", "id"), table_name=table_name)


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


def load_airports(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df\
        .withColumn("canon_state_code", F.lower(regexp_extract('iso_region', r'(?<=-)(\w*)', 0)))\
        .withColumn("canon_country_code2", F.lower(regexp_extract('iso_region', r'(\w*)(?=-)', 0))) \
        .withColumnRenamed("municipality", "city_name")

    write_to_postgres(df, table_name=table_name)


def load_temperature_data_by_city(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("year", F.year(F.col("timestamp"))) \
        .withColumn("month", F.month(F.col("timestamp"))) \
        .withColumnRenamed("City", "city_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumnRenamed("Longitude", "longitude") \
        .withColumnRenamed("Latitude", "latitude")

    write_to_postgres(df, table_name=table_name)


def load_temperature_data_by_state(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    state_df = load_state_names(spark=spark, path='../data/controls/state_names.csv')

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
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty")\
        .withColumn("canon_country_name", F.lower("country_name"))

    df.createOrReplaceTempView("temps")
    state_df.createOrReplaceTempView("states")

    # extract columns to create songs table
    temps_df = spark.sql("""
        SELECT T.*, lower(S.state_code) as canon_state_code
        FROM temps T
        join states S on S.state_name = T.state_name
         """)

    write_to_postgres(temps_df, table_name=table_name)


def load_temperature_data_by_country(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    # dt,AverageTemperature,AverageTemperatureUncertainty,Country

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("year", F.year(F.col("timestamp"))) \
        .withColumn("month", F.month(F.col("timestamp"))) \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty")

    write_to_postgres(df, table_name=table_name)


def write_to_postgres(df: DataFrame, table_name: str):
    df.write.format("jdbc").mode("overwrite") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


def load_state_names(spark: SparkSession, path: str) -> DataFrame:
    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true")\
        .csv(path)

    return df\
        .withColumn("canon_state_name", F.lower(df.state_name))\
        .withColumn("canon_state_code", F.lower(df.state_code))


def load_city_names(spark: SparkSession, path: str) -> DataFrame:
    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)
    # sas_code,country_name,canon_country_name
    # 245,"CHINA, PRC"
    return df \
        .withColumn("canon_country_name",  F.lower(regexp_extract('country_name', r'\w*', 0)))


def load_country_codes(spark: SparkSession, path: str) -> DataFrame:
    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)
    """
    country_name,country_code2,country_code3,country_index,country_iso
    Afghanistan,AF,AFG,004,ISO 3166-2:AF
    """
    return df \
        .withColumn("canon_country_name",  F.lower('country_name')) \
        .withColumn("canon_country_code2",  F.lower('country_code2'))


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    df_country = load_country_codes(spark=spark,  path='../data/controls/country_codes.csv')
    df_country.show()


    # states_df = load_state_names(spark, path='../data/controls/state_names.csv')

    # load_temperature_data_by_country(spark=spark, format="csv",
    #                                path=f'../data/temperatures/GlobalLandTemperaturesByCountry.csv',
    #                                table_name="country_temperatures")

    load_temperature_data_by_state(spark=spark, format="csv",
                                  path='../data/temperatures/GlobalLandTemperaturesByState.csv',
                                  table_name="state_temperatures")

    # load_temperature_data_by_city(spark=spark, format="csv",
    #                               path='../data/temperatures/SampleGlobalLandTemperaturesByCity.csv',
    #                               table_name="city_temperatures")
    #
    # load_immigrations(spark=spark, format="csv", path='../data/udacity/immigration_data_sample.csv',
    #                   table_name="immigrations")

    load_immigrations(spark=spark, format="parquet", path='../data/udacity/sas_data',
                      table_name="pyspark_immigration")

    load_cities(spark=spark, format="csv", path='../data/udacity/us-cities-demographics.csv', table_name="us_cities",
                header='True', sep=";")

    load_airports(spark=spark, format="csv", path='../data/udacity/airport-codes_csv.csv', table_name="airport_codes")


if __name__ == "__main__":
    load_core_data()
