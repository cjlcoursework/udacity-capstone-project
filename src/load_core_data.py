from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


# --------------------  Global Spark Session ---------------------
from config import PYSPARK_EXTENDED_JARS


def load_immigrations(spark: SparkSession, format: str, path: str, table_name: str, **kwargs):
    df = spark.read.format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("iana_code", df.i94port) \
        .withColumn("state_code", df.i94addr) \
        .withColumn("country_code", regexp_extract('i94cit', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("year", regexp_extract('i94yr', r'\d*', 0).cast(IntegerType())) \
        .withColumn("month", regexp_extract('i94mon', r'\d*', 0).cast(IntegerType()))

    write_to_postgres(df.withColumnRenamed("_c0", "id"), table_name=table_name)


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
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty")

    write_to_postgres(df, table_name=table_name)


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


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    load_temperature_data_by_country(spark=spark, format="csv",
                                   path='./data/temperatures/SampleGlobalLandTemperaturesByCountry.csv',
                                   table_name="country_temperatures")

    load_temperature_data_by_state(spark=spark, format="csv",
                                  path='./data/temperatures/GlobalLandTemperaturesByState.csv',
                                  table_name="state_temperatures")

    load_temperature_data_by_city(spark=spark, format="csv",
                                  path='./data/temperatures/SampleGlobalLandTemperaturesByCity.csv',
                                  table_name="city_temperatures")

    load_immigrations(spark=spark, format="csv", path='./data/udacity/immigration_data_sample.csv',
                      table_name="immigrations")

    load_cities(spark=spark, format="csv", path='./data/udacity/us-cities-demographics.csv', table_name="us_cities",
                header='True', sep=";")

    load_airports(spark=spark, format="csv", path='./data/udacity/airport-codes_csv.csv', table_name="airport_codes")


if __name__ == "__main__":
    load_core_data()
