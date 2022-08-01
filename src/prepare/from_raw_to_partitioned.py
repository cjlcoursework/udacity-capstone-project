from pyspark.sql.functions import *

from src.common.ingest_core_data import get_temperature_data_by_state, get_immigration_data


def write_immigrations_to_partitioned(df: DataFrame):
    df.write.partitionBy("year", "month", "i94port") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv("../../data/prepdata/immigration/")


def write_current_temperatures_to_partitioned(df: DataFrame):
    df.filter("year > 2000") \
        .withColumn("year", df.year + 4) \
        .write.partitionBy("year", "month", "Country") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv("../../data/prepdata/temperature")


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    data_path = '../../data/rawdata'

    # --- temperature ----
    temp_df = get_temperature_data_by_state(spark=spark,
                                            format="csv",
                                            path=f'{data_path}/temperatures/GlobalLandTemperaturesByState.csv')

    write_current_temperatures_to_partitioned(temp_df)

    # --- immigration ----
    immigration_df = get_immigration_data(spark=spark,
                                          format="parquet",
                                          path=f'{data_path}/udacity/sas_data')

    write_immigrations_to_partitioned(immigration_df)


if __name__ == "__main__":
    load_core_data()
