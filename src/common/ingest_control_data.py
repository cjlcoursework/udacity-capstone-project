from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.common.common import write_to_postgres

# TODO - create a database of raw files and locations - because we are not saving in git
from src.common.config import PYSPARK_EXTENDED_JARS


def get_sas_countries(spark: SparkSession, raw: bool = False) -> DataFrame:
    path = '../../data/rawdata/controls/sas_countries.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    if not raw:
        df = df.filter(df.sas_status != 'DEFUNCT') \
            .withColumn("canon_country_name", F.upper(regexp_extract('country_name', r'[^\,]*', 0)))

    return df


def get_raw_sas_index(spark: SparkSession) -> DataFrame:
    path = '../../data/rawdata/controls/sas_index_raw.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df

def get_country_codes(spark: SparkSession) -> DataFrame:
    path = '../../data/rawdata/controls/country_codes.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)
    """
    country_name,country_code2,country_code3,country_index,country_iso
    Afghanistan,AF,AFG,004,ISO 3166-2:AF
    """
    return df \
        .withColumn("canon_country_name", F.upper('country_name')) \
        .withColumn("canon_country_code2", F.upper('country_code2')) \
        .withColumn("canon_country_code3", F.upper('country_code3'))


def load_cities(spark: SparkSession, format: str, path: str, table_name: str):
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("sep", ";") \
        .parquet(path)

    df = df \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("Number of Veterans", "veterans_population") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("Average Household Size", "avg_household_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Count", "count")

    write_to_postgres(df, table_name=table_name)


# ---test only --
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # ---control tables -- insert to postgres for inspection ONLY - we are not really using the postgres tables
    write_to_postgres(get_sas_countries(spark), table_name="commons.sas_countries")
    write_to_postgres(get_sas_countries(spark, True), table_name="commons.sas_countries_raw")
    write_to_postgres(get_country_codes(spark), table_name="commons.country_codes")
    write_to_postgres(get_raw_sas_index(spark), table_name="commons.sas_index")
