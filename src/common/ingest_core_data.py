from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.common.common import create_files_log, write_to_postgres, create_load_log
from src.common.config import PYSPARK_EXTENDED_JARS
from src.common.ingest_control_data import get_sas_countries, get_country_codes

LOAD_CONTROLS_POSTGRES = False
LOAD_RAW_POSTGRES = True
LOAD_RAW_IMMIGRATION_PARQUET = True
LOAD_RAW_TEMPERATURE_PARQUET = False

ROOT_FOLDER = '/Users/christopherlomeli/Source/courses/udacity/data-engineer/udacity-capstone-project'
SUPPORTED_LOCATIONS = [
    "AUSTRALIA",
    "BRAZIL",
    "CANADA",
    "CHINA",
    "INDIA",
    "RUSSIA",
    "UNITED STATES"
]

COUNTRIES_WITH_TEMPERATURE_DATA = """( 'AUSTRALIA',
    'BRAZIL',
    'CANADA',
    'CHINA',
    'INDIA',
    'RUSSIA',
    'UNITED STATES' )"""

# --------------------  Global Spark Session ---------------------


def get_immigration_data(spark: SparkSession, format: str, path: str) -> DataFrame:
    df = spark.read.format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(path)

    df = df \
        .withColumnRenamed("_c0", "id") \
        .withColumn("input_file", input_file_name()) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType())) \
        .withColumn("birth_year", regexp_extract('biryear', r'\d*', 0).cast(IntegerType()))

    return df


def process_immigration_data(spark: SparkSession, df: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    df.createOrReplaceTempView("next_immigrations")

    country_ndx_df = get_sas_countries(spark=spark)
    country_ndx_df.createOrReplaceTempView("sas_countries")

    airports_df = process_airport_data(spark=spark, df=get_airport_data(spark=spark))
    airports_df.createOrReplaceTempView("airport_codes")

    files_df = create_files_log(df=df, table_name="immigration")
    files_df.createOrReplaceTempView("files")

    logs_df = create_load_log(file_df=files_df)
    logs_df.createOrReplaceTempView("logs")

    airports_df.show(truncate=False)

    # extract columns to create songs table
    #  exclude immigration countries with no sas code -- none or very few records
    #  exclude immigrations to tiny airports (See bad airports above)
    #  exclude arrivals to non-US -- we'll start with US then add other destinations as needed
    #  So this becomes immigrations to the US from SUPPORTED_LOCATIONS (See variable above)
    # the distinct might not be performant
    # this join causes a fan-out, so dedup using partition on the cicid key
    # todo - improve performance by getting rid of the de-dyp logic? (find the fan-out)
    migration_df = spark.sql(f"""
    WITH DUPS as (SELECT row_number() OVER (PARTITION BY I.cicid ORDER BY A.city_name) as rownumber,
             I.cicid,
             I.i94yr,
             I.i94mon,
             I.i94cit,
             I.i94res,
             I.i94port,
             I.arrdate,
             I.i94mode,
             I.i94addr,
             I.depdate,
             I.i94bir,
             I.i94visa,
             I.count,
             I.dtadfile,
             I.visapost,
             I.occup,
             I.entdepa,
             I.entdepd,
             I.entdepu,
             I.matflag,
             I.biryear,
             I.dtaddto,
             I.gender,
             I.insnum,
             I.airline,
             I.admnum,
             I.fltno,
             I.visatype,
             I.birth_year,
             I.i94yr as year,
             I.i94mon as month,
             L.file_id,
             upper(S.canon_country_name)                                   as origin_country_name,
             upper(S.country_code2)                                        as origin_country_code,
             A.country_code2                                               as arrival_country,
             upper(A.country_name)                                         as arrival_country_name,
             A.state_code                                                  as arrival_state_code,
             A.city_name                                                   as arrival_city_name
      FROM next_immigrations I
               join sas_countries S on S.sas_code = I.i94cit
               join airport_codes A on A.iata_code = I.i94port
               left join files L on L.input_file = I.input_file
      where A.country_code2 = 'US'
        and S.canon_country_name in {COUNTRIES_WITH_TEMPERATURE_DATA}
    )
    SELECT *
    FROM DUPS 
    WHERE rownumber = 1 """)

    return migration_df, files_df, logs_df


def get_temperature_data_by_state(spark: SparkSession, format: str, path: str, start_year: int = 2000, shift_years: int = 4) -> DataFrame:
    df = spark.read.format(format) \
        .option("header", "True") \
        .option("inferSchema", "true") \
        .load(path)

    df = df \
        .withColumn("timestamp", to_date(col("dt"), "yyyy-MM-dd")) \
        .withColumn("input_file", input_file_name()) \
        .withColumnRenamed("State", "state_name") \
        .withColumnRenamed("Country", "country_name") \
        .withColumnRenamed("AverageTemperature", "average_temp") \
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temp_uncertainty") \
        .withColumn("canon_country_name", F.upper("country_name"))\
        .withColumn("year", F.year("timestamp") + shift_years)\
        .withColumn("month", F.month("timestamp")) \
        .withColumn("original_year", F.year("timestamp")) \
        .filter(f"year > {start_year}")

    return df


def process_temperature_data_by_state(spark: SparkSession, df: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    df.createOrReplaceTempView("temperatures")

    files_df = create_files_log(df=df, table_name="temperature")
    files_df.createOrReplaceTempView("files")

    logs_df = create_load_log(file_df=files_df)
    logs_df.createOrReplaceTempView("logs")

    temps_df = spark.sql(f"""
    SELECT 
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
    FROM temperatures T 
     join files L on L.input_file = T.input_file
     """)

    return temps_df, files_df, logs_df


def get_airport_data(spark: SparkSession) -> DataFrame:
    path = '../../data/rawdata/controls/airport-codes.csv'

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


def load_raw_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    data_path = '../../data/rawdata'

    temp_df = get_temperature_data_by_state(spark=spark,
                                            format="csv",
                                            path=f'{data_path}/temperatures/GlobalLandTemperaturesByState.csv')

    write_to_postgres(temp_df, "temperature_raw")

    immigration_df = get_immigration_data(spark=spark,
                                          format="parquet",
                                          path=f'{data_path}/udacity/sas_data')

    write_to_postgres(immigration_df, "immigration_raw")

    airport_df = get_airport_data(spark=spark)
    write_to_postgres(airport_df, "airports_raw")


if __name__ == "__main__":
    """
    We want to be able to:
    1. from raw versions:
        into postgres processed tables
        into partitioned dataset        
    2. from partitioned data 
        into parquet
        
    """
    load_raw_data()
