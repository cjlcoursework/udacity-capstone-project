from src.common.prep_configs import *
from src.pyspark_applications.common import create_files_log, create_load_log, write_table_to_lake

try:
    from pyspark import SparkConf
    from pyspark.sql import *
except ImportError as e:
    print("Error importing Spark Modules", e)


def get_control_data(spark: SparkSession, tag: str) -> DataFrame:
    path = PreparationConfigs().get_value(tag)
    return spark.read \
        .option("header", "true") \
        .option("delimiter", ",") \
        .parquet(path)


def process_immigration_data(spark: SparkSession) -> (DataFrame, DataFrame, DataFrame):
    path = PreparationConfigs().get_value(IMMIGRATION_INPUT_DATA_TAG)
    df = spark.read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .csv(path)

    df.createOrReplaceTempView("next_immigrations")

    country_ndx_df = get_control_data(spark=spark, tag=SAS_COUNTRIES_TAG)
    country_ndx_df.createOrReplaceTempView("sas_countries")

    airports_df = get_control_data(spark=spark, tag=AIRPORT_CODES)
    airports_df.createOrReplaceTempView("airport_codes")

    files_df = create_files_log(df=df, table_name="immigration")
    files_df.createOrReplaceTempView("files")

    logs_df = create_load_log(file_df=files_df)
    logs_df.createOrReplaceTempView("logs")

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

    write_table_to_lake(df=migration_df,
                        folder=PreparationConfigs().get_value(IMMIGRATION_LAKE_DATA_TAG),
                        mode="append")

    write_table_to_lake(df=files_df,
                        folder=PreparationConfigs().get_value(IMMIGRATION_LAKE_FILES_TAG),
                        mode="append")

    write_table_to_lake(df=logs_df,
                        folder=PreparationConfigs().get_value(IMMIGRATION_LAKE_LOADS_TAG), mode="append")


def ingest_migration_data():
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    process_immigration_data(spark=spark_session)


if __name__ == "__main__":
    ingest_migration_data()
