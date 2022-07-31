from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *


def prep_immigrations(spark: SparkSession, format: str, path: str):
    df = spark.read.format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("year", regexp_extract('i94yr', r'\d*', 0).cast(IntegerType())) \
        .withColumn("month", regexp_extract('i94mon', r'\d*', 0).cast(IntegerType()))

    df.write.partitionBy("year", "month", "i94port") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv("../../data/prepdata/immigration/")


def prep_temperature_data_by_state(spark: SparkSession, format: str, path: str):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("year", F.year(F.col("timestamp"))) \
        .withColumn("month", F.month(F.col("timestamp"))) \

    df.filter("year > 2000") \
        .withColumn("year", df.year + 4) \
        .write.partitionBy("year", "month", "Country") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv("../../data/testdata/temperature")


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # -- core data - these use the control tables internally to extend the tables
    prep_temperature_data_by_state(spark=spark, format="csv",
                                   path='../../data/rawdata/temperatures/GlobalLandTemperaturesByState.csv')

    prep_immigrations(spark=spark, format="parquet", path='../../data/rawdata/udacity/sas_data')


if __name__ == "__main__":
    load_core_data()
