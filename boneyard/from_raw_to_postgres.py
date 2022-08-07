from src.common.common import write_to_postgres
from src.common.config import PYSPARK_EXTENDED_JARS
from src.common.get_raw_core_data import get_raw_temperature_data, get_raw_immigration_data, get_airport_data


def load_raw_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    data_path = '../data/source_data'

    temp_df = get_raw_temperature_data(spark=spark,
                                       format_to_use="csv",
                                       path=f'{data_path}/temperatures/GlobalLandTemperaturesByState.csv')

    write_to_postgres(temp_df, "temperature")

    immigration_df = get_raw_immigration_data(spark=spark,
                                              format_to_use="parquet",
                                              path=f'{data_path}/udacity/sas_data')

    write_to_postgres(immigration_df, "immigration")

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
