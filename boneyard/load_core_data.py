from boneyard.common import write_to_postgres
from boneyard.config import PYSPARK_EXTENDED_JARS
from src.common.get_raw_core_data import process_temperature_data, get_raw_temperature_data, \
    get_raw_immigration_data, process_airport_data, get_airport_data, process_immigration_data


def load_processed_data_to_postgres():
    data_path = '../source_data'

    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath', PYSPARK_EXTENDED_JARS) \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

    # -- temperature --
    temp_df, temp_files_df, temp_logs_df = process_temperature_data(
        spark=spark,
        df=get_raw_temperature_data(spark=spark,
                                    format_to_use="csv",
                                    path=f'{data_path}/temperatures/GlobalLandTemperaturesByState.csv')
    )

    write_to_postgres(temp_df, "temperature_raw")
    write_to_postgres(temp_files_df, "controls.load_files", "append")
    write_to_postgres(temp_logs_df, "controls.load_times", "append")

    # -- immigration --
    immigration_df, immigration_files_df, immigration_logs_df = process_immigration_data(
        spark=spark,
        df = get_raw_immigration_data(spark=spark,
                                      format_to_use="parquet",
                                      path=f'{data_path}/udacity/sas_data')
    )
    write_to_postgres(immigration_df, "immigration_raw")

    # -- postgres --
    airport_df = process_airport_data(
        spark=spark,
        df=get_airport_data(spark=spark)
    )

    write_to_postgres(airport_df, "airports")
    write_to_postgres(immigration_files_df, "controls.load_files", "append")
    write_to_postgres(immigration_logs_df, "controls.load_times", "append")


if __name__ == "__main__":
    load_processed_data_to_postgres()
