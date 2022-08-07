from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from src.common.config import PYSPARK_EXTENDED_JARS
from src.common.get_raw_core_data import get_raw_temperature_data, get_raw_immigration_data
from src.prepare.load_source_control_data import get_sas_countries, process_airport_data

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


def process_temperature_data(spark: SparkSession, df: DataFrame) -> (DataFrame, DataFrame, DataFrame):

    df.createOrReplaceTempView("temperatures")

    files_df = create_files_log(df=df, table_name="temperature")
    files_df.createOrReplaceTempView("files")

    logs_df = create_load_log(file_df=files_df)
    logs_df.createOrReplaceTempView("logs")

    temps_df = spark.sql(f"""
    SELECT 
        T.id,
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


def load_raw_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    data_path = '../../data/source_data'

    temp_df = get_raw_temperature_data(spark=spark,
                                       format_to_use="csv",
                                       path=f'{data_path}/temperatures/GlobalLandTemperaturesByState.csv')

    write_to_postgres(temp_df, "temperature_raw")

    immigration_df = get_raw_immigration_data(spark=spark,
                                              format_to_use="parquet",
                                              path=f'{data_path}/udacity/sas_data')

    write_to_postgres(immigration_df, "immigration_raw")

    # airport_df = get_airport_data(spark=spark)
    # write_to_postgres(airport_df, "airports_raw")


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
