import datetime

from pyspark.sql import functions as F
from pyspark.sql.functions import *


def write_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite"):
    df.write.format("jdbc").mode(mode) \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


# def write_table_to_lake(df: DataFrame, folder: str):
#     x = datetime.datetime.now()
#     sub_folder = f"{folder}/load_year={x.year}/load_month={x.month:02}/load_day={x.day:02}/load_hour={x.hour:02}"
#     df.write \
#         .mode("overwrite") \
#         .parquet(sub_folder)


