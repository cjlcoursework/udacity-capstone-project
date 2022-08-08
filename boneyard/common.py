from pyspark.sql.functions import *


def write_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite"):
    df.write.format("jdbc").mode(mode) \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "chinois1") \
        .save()


