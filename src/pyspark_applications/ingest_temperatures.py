import os

from src.common.Configurations import *
from src.pyspark_applications.common import create_files_log, create_load_log, write_table_to_lake

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


def process_temperature_data(spark: SparkSession):

    path = Configurations().get_value(TEMPERATURE_INPUT_DATA_TAG)
    df = spark.read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(path)

    df.createOrReplaceTempView("temperature")

    files_df = create_files_log(df=df, table_name="temperature")
    files_df.createOrReplaceTempView("files")

    logs_df = create_load_log(file_df=files_df)
    logs_df.createOrReplaceTempView("logs")

    temps_df = spark.sql(f"""
    SELECT 
        T.id,
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
    FROM temperature T 
     join files L on L.input_file = T.input_file
     """)

    write_table_to_lake(df=temps_df,
                        folder=Configurations().get_value(TEMPERATURE_LAKE_DATA_TAG),
                        mode="append")

    write_table_to_lake(df=files_df,
                        folder=Configurations().get_value(TEMPERATURE_LAKE_FILES_TAG),
                        mode="append")

    write_table_to_lake(df=logs_df,
                        folder=Configurations().get_value(TEMPERATURE_LAKE_LOADS_TAG), mode="append")


def ingest_temperature_data():
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    process_temperature_data(spark=spark_session)


if __name__ == "__main__":
    ingest_temperature_data()