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


def write_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite"):
    df.write.format("jdbc").mode(mode) \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


