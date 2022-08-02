import os

from src.common.common import write_to_lake
from src.common.ingest_core_data import get_temperature_data, process_temperature_data

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


def ingest_temperature_data(spark_session: SparkSession, input_files: str,
                            output_files: str):
    dataset = "temperature"
    dataset_input_path = os.path.join(input_files, dataset + "/")
    dataset_output_path = os.path.join(output_files, dataset + "/")

    df = get_temperature_data(spark=spark_session, format="csv", path=dataset_input_path)

    transform_df, file_df, log_df = process_temperature_data(spark=spark_session, df=df)

    write_to_lake(df=transform_df,
                  folder=os.path.join(dataset_output_path, "data" + "/"))

    write_to_lake(df=file_df,
                  folder=os.path.join(dataset_output_path, "controls/load_files" + "/"))

    write_to_lake(df=log_df,
                  folder=os.path.join(dataset_output_path, "controls/load_times" + "/"))


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    input_path = "../../data_test"
    output_path = "../../datalake/"
    checkpoint_path = "../../datalake/checkpoint/"

    ingest_temperature_data(spark_session=spark,
                            input_files=input_path,
                            output_files=output_path)
