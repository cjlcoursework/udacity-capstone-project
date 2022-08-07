from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.common.SchemaFactory import SchemaFactory, IMMIGRATION_TAG, TEMPERATURE_TAG


def get_raw_immigration_data(spark: SparkSession, format_to_use: str, path: str, streaming: bool = False) -> DataFrame:

    if streaming:
        schema = SchemaFactory().get_schema(tag=IMMIGRATION_TAG)
        df = spark.readStream.format(format_to_use) \
            .schema(schema) \
            .option("header", "true") \
            .option("delimiter", ",") \
            .load(path)
    else:
        df = spark.read.format(format_to_use) \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("inferSchema", "true") \
            .load(path)

    df = df \
        .withColumnRenamed("_c0", "id")\
        .withColumn("year", df.i94yr)\
        .withColumn("month", df.i94mon) \
        .withColumn("input_file", input_file_name()) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \

    return df


def get_raw_temperature_data(spark: SparkSession, format_to_use: str, path: str, streaming: bool = False, start_year: int = 2000, shift_years: int = 4) -> DataFrame:
    schema = SchemaFactory().get_schema(tag=TEMPERATURE_TAG)

    if streaming:
        df = spark.readStream.format(format_to_use)\
            .schema(schema) \
            .option("header", "True") \
            .load(path)
    else:
        df = spark.read.format(format_to_use) \
           .option("header", "True") \
            .option("inferSchema", "true") \
            .load(path)

    df = df \
        .withColumn("id", md5(concat_ws("||", *df.columns)))\
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("input_file", input_file_name()) \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumn("canon_country_name", F.upper("country_name"))\
        .withColumn("year", F.year("timestamp") + shift_years)\
        .withColumn("month", F.month("timestamp")) \
        .withColumn("original_year", F.year("timestamp")) \
        .filter(f"year > {start_year}")

    return df


