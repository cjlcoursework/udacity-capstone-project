import datetime
from typing import Tuple, Type, Union, Any

import pandas
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_files_log(df: DataFrame, table_name: str) -> DataFrame:
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


def write_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite"):
    df.write.format("jdbc").mode(mode) \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


def write_to_lake(df: DataFrame, folder: str):
    x = datetime.datetime.now()
    sub_folder = f"{folder}/load_year={x.year}/load_month={x.month:02}/load_day={x.day:02}/load_hour={x.hour:02}"
    df.write \
        .mode("overwrite") \
        .parquet(sub_folder)