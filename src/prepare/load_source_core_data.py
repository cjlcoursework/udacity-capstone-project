from pyspark.sql.functions import *

from src.common.Configurations import *
from src.common.get_raw_core_data import get_raw_temperature_data, get_raw_immigration_data


def write_immigrations_to_partitioned(df: DataFrame):
    df.write.partitionBy("year", "month", "i94port") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv(Configurations().get_value(TEMPERATURE_INPUT_DATA_TAG))


def write_current_temperatures_to_partitioned(df: DataFrame):
    df.filter("year > 2000") \
        .withColumn("year", df.year + 4) \
        .write.partitionBy("year", "month", "country_name") \
        .mode("overwrite") \
        .option("header", "True") \
        .csv(Configurations().get_value(TEMPERATURE_INPUT_DATA_TAG))


def load_core_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # --- temperature ----
    temp_df = get_raw_temperature_data(spark=spark,
                                       format_to_use="csv",
                                       path=f'{Configurations().get_value(SOURCE_ROOT_TAG)}/temperatures'
                                        f'/GlobalLandTemperaturesByState.csv')

    write_current_temperatures_to_partitioned(temp_df)

    # --- immigration ----
    immigration_df = get_raw_immigration_data(spark=spark,
                                              format_to_use="parquet",
                                              path=f'{Configurations().get_value(SOURCE_ROOT_TAG)}/udacity/sas_data')

    write_immigrations_to_partitioned(immigration_df)


if __name__ == "__main__":
    load_core_data()
