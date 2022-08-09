from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from src.common.Configurations import *


def get_raw_immigration_data(spark: SparkSession, format_to_use: str, path: str) -> DataFrame:

    df = spark.read.format(format_to_use) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(path)

    df = df\
        .withColumnRenamed("_c0", "id") \
        .withColumn("year", df.i94yr) \
        .withColumn("month", df.i94mon) \
        .withColumn("input_file", F.input_file_name()) \
        .withColumn("birth_year", F.regexp_extract('biryear', r'\d*', 0).cast(IntegerType()))

    return df


def get_raw_temperature_data(spark: SparkSession, format_to_use: str, path: str, streaming: bool = False, start_year: int = 2000, shift_years: int = 4) -> DataFrame:

    df = spark.read.format(format_to_use) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("id", F.md5(F.concat_ws("||", *df.columns))) \
        .withColumn("timestamp", F.to_date(F.col("dt"), "yyyy-MM-dd")) \
        .withColumn("input_file", F.input_file_name()) \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumn("canon_country_name", F.upper("country_name")) \
        .withColumn("year", F.year("timestamp") + shift_years) \
        .withColumn("month", F.month("timestamp")) \
        .withColumn("original_year", F.year("timestamp")) \
        .filter(f"year > {start_year}")

    return df


def write_immigrations_to_partitioned(df: DataFrame):
    df.write.partitionBy("year", "month", "i94port") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv(Configurations().get_value(IMMIGRATION_INPUT_DATA_TAG))


def write_current_temperatures_to_partitioned(df: DataFrame):
    path = Configurations().get_value(TEMPERATURE_INPUT_DATA_TAG)

    df.filter("year > 2000") \
        .withColumn("year", df.year + 4) \
        .write.partitionBy("year", "month", "country_name") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv(path)


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # --- temperature ----
    temp_df = get_raw_temperature_data(spark=spark,
                                       format_to_use="csv",
                                       path=f'{Configurations().get_value(SOURCE_ROOT_TAG)}/temperature/GlobalLandTemperaturesByState.csv')

    write_current_temperatures_to_partitioned(temp_df)

    # --- immigration ----
    immigration_df = get_raw_immigration_data(spark=spark,
                                              format_to_use="parquet",
                                              path=f'{Configurations().get_value(SOURCE_ROOT_TAG)}/udacity/sas_data')

    write_immigrations_to_partitioned(immigration_df)


if __name__ == "__main__":
    load_core_data()
