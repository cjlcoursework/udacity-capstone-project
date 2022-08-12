from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.common.prep_configs import *
from boneyard.common import write_to_postgres

# TODO - create a database of raw files and locations - because we are not saving in git
from boneyard.config import PYSPARK_EXTENDED_JARS


def get_control_data(spark: SparkSession, tag: str) -> DataFrame:
    path = PreparationConfigs().get_value(tag)
    return spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .parquet(path)


def write_control_to_lake(df: DataFrame, tag: str):
    path = PreparationConfigs().get_value(tag)
    df.write \
        .mode("overwrite") \
        .parquet(path)


def get_sas_countries(spark: SparkSession, raw: bool = False) -> DataFrame:
    path = '../../source_data/controls/sas_countries.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    if not raw:
        df = df.filter(df.sas_status != 'DEFUNCT') \
            .withColumn("canon_country_name", F.upper(regexp_extract('country_name', r'[^\,]*', 0)))

    return df


def get_raw_sas_index(spark: SparkSession) -> DataFrame:
    path = '../../source_data/controls/sas_index_raw.csv'

    df: DataFrame = spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)

    return df


def get_country_codes(spark: SparkSession) -> DataFrame:
    path = '../../source_data/controls/country_codes.csv'

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


def get_airport_data(spark: SparkSession) -> DataFrame:
    path = '../../source_data/controls/airport-codes.csv'

    return spark.read \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .csv(path)


def process_airport_data(spark: SparkSession, df: DataFrame) -> DataFrame:
    countries_df = get_country_codes(spark=spark)
    countries_df.createOrReplaceTempView("country_lookup")

    closed_type = F.udf(lambda x: x == 'closed')

    df = df \
        .filter("iata_code != '0'") \
        .withColumn("is_closed", closed_type(df.type)) \
        .withColumn("state_code", F.upper(regexp_extract('iso_region', r'(?<=-)(\w*)', 0))) \
        .withColumn("country_code2", F.upper(regexp_extract('iso_region', r'(\w*)(?=-)', 0))) \
        .withColumn("latitude", regexp_extract('coordinates', r'^[^\,]*', 0).cast(DoubleType())) \
        .withColumn("longitude", regexp_extract('coordinates', r'(?<=,\s).*$', 0).cast(DoubleType())) \
        .withColumnRenamed("municipality", "city_name")

    df.createOrReplaceTempView("airports")

    airports_df = spark.sql("""
        WITH  X AS (
            select
                   row_number() OVER (PARTITION BY A.iata_code
                       ORDER BY is_closed, city_name ) AS rownumber
                        , ident
                        , type
                        , name
                        , elevation_ft
                        , continent
                        , iso_country
                        , iso_region
                        , city_name
                        , gps_code
                        , iata_code
                        , local_code
                        , coordinates
                        , state_code
                        , A.country_code2
                        , latitude
                        , longitude
                    , coalesce(A.iata_code, A.ident) as canon_iata_code
                    , C.country_name as country_name
                    , C.country_code3 as country_code3
            FROM airports A
                JOIN country_lookup C on C.country_code2 = A.country_code2
        )
        select
            ident
             , type
             , name
             , elevation_ft
             , continent
             , iso_country
             , iso_region
             , city_name
             , gps_code
             , iata_code
             , local_code
             , coordinates
             , state_code
             , country_code2
             , latitude
             , longitude
             , canon_iata_code
             , country_name
             , country_code3
        FROM X WHERE rownumber = 1
         """)
    return airports_df


def publish_controls_to_portgres(spark: SparkSession):
    write_to_postgres(get_sas_countries(spark), table_name=PreparationConfigs().get_value(SAS_COUNTRIES_TABLE))
    write_to_postgres(get_country_codes(spark), table_name=PreparationConfigs().get_value(COUNTRY_CODES_TABLE))
    write_to_postgres(process_airport_data(spark=spark, df=get_airport_data(spark)),
                      table_name= PreparationConfigs().get_value(AIRPORT_CODES_TABLE))


def publish_controls_to_parquet(spark: SparkSession):
    # ---control tables -- insert to postgres for inspection ONLY - we are not really using the postgres tables
    write_control_to_lake(get_sas_countries(spark), tag=SAS_COUNTRIES_TAG)
    write_control_to_lake(get_country_codes(spark), tag=COUNTRY_CODES_TAG)
    write_control_to_lake(
        process_airport_data(
            spark=spark,
            df=get_airport_data(spark)),
        tag=AIRPORT_CODES)


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    publish_controls_to_parquet(spark=spark)
