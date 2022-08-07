import os

#
# from src.common.get_raw_core_data import get_raw_temperature_data
# from src.common.transform_core_data import process_temperature_data
# from src.stage.ingest_config import configuration
#
# try:
#     from pyspark import SparkConf
#     from pyspark.sql import *
# except ImportError as e:
#     print("Error importing Spark Modules", e)
#
#
# def ingest_temperature_data(spark_session: SparkSession, input_files: str,
#                             output_files: str):
#     dataset = "temperature"
#     dataset_input_path = os.path.join(input_files, dataset + "/")
#     dataset_output_path = os.path.join(output_files, dataset + "/")
#
#     df = get_raw_temperature_data(spark=spark_session, format_to_use="csv", path=dataset_input_path)
#
#     transform_df, file_df, log_df = process_temperature_data(spark=spark_session, df=df)
#
#     write_table_to_lake(df=transform_df,
#                         folder=os.path.join(dataset_output_path, "data" + "/"))
#
#     write_table_to_lake(df=file_df,
#                         folder=os.path.join(dataset_output_path, "controls/load_files" + "/"))
#
#     write_table_to_lake(df=log_df,
#                         folder=os.path.join(dataset_output_path, "controls/load_times" + "/"))
#
#
# def main(profile: str):
#     from pyspark.sql import SparkSession
#
#     spark = SparkSession.builder \
#         .config("spark.sql.debug.maxToStringFields", 1000) \
#         .getOrCreate()
#
#     this_profile = configuration[profile]
#     input_path = this_profile["input_path"]
#     output_path = this_profile["output_path"]
#
#     ingest_temperature_data(spark_session=spark,
#                             input_files=input_path,
#                             output_files=output_path)
#

if __name__ == "__main__":
    pass