import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import *


def create_files_log(df: DataFrame, table_name: str) -> DataFrame:
    df.show(truncate=False)
    files_df = df.select("input_file").distinct() \
        .withColumn("dataset", lit(table_name)) \
        .withColumn("file_id", F.expr("uuid()")) \
        .withColumn("file_name", regexp_extract("input_file", r'([^\/]+).$', 0))
    return files_df


def create_load_log(file_df: DataFrame,  reason: str = "initial") -> DataFrame:
    load_df = file_df.select("file_id").distinct() \
        .withColumn("current_timestamp",current_timestamp()) \
        .withColumn("reason", F.lit(reason))
    return load_df


def write_table_to_lake(df: DataFrame, folder: str, mode: str = "overwrite"):
    x = datetime.datetime.now()
    sub_folder = f"{folder}/load_year={x.year}/load_month={x.month:02}/load_day={x.day:02}/load_hour={x.hour:02}"
    df.write \
        .mode("overwrite") \
        .parquet(sub_folder)


# def write_stream_to_lake(df: DataFrame, folder_path: str, checkpoint_path: str):
#     """write a stream to a file"""
#     x = datetime.datetime.now()
#     sub_folder = f"year={x.year}/month={x.month:02}/day={x.day:02}/hour={x.hour:02}"
#
#     df \
#         .writeStream.trigger(once=True) \
#         .option("path", folder_path + sub_folder) \
#         .option("checkpointLocation", checkpoint_path) \
#         .format('json') \
#         .start()