from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.common.SchemaFactory import SchemaFactory, IMMIGRATION_TAG, TEMPERATURE_TAG


def get_input_immigration_data(spark: SparkSession, format_to_use: str, path: str) -> DataFrame:
    schema = SchemaFactory().get_schema(tag=IMMIGRATION_TAG)
    df = spark.readStream.format(format_to_use) \
        .schema(schema) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    return df


def get_input_temperature_data(spark: SparkSession, format_to_use: str, path: str, start_year: int = 2000, shift_years: int = 4) -> DataFrame:
    schema = SchemaFactory().get_schema(tag=TEMPERATURE_TAG)

    df = spark.readStream.format(format_to_use)\
        .schema(schema) \
        .option("header", "True") \
        .load(path)
    return df


