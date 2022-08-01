from src.common.common import write_to_postgres
from src.common.config import PYSPARK_EXTENDED_JARS
from src.common.ingest_core_data import get_temperature_data_by_state, get_immigration_data, get_airport_data


def load_raw_data():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
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
