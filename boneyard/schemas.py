from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType, BooleanType, DoubleType, \
    IntegerType, TimestampType

migration_schema = StructType([
    StructField('cicid', DoubleType(), True),
    StructField('i94yr', DoubleType(), True),
    StructField('i94mon', DoubleType(), True),
    StructField('i94cit', DoubleType(), True),
    StructField('i94res', DoubleType(), True),
    StructField('i94port', StringType(), True),
    StructField('arrdate', DoubleType(), True),
    StructField('i94mode', DoubleType(), True),
    StructField('i94addr', StringType(), True),
    StructField('depdate', DoubleType(), True),
    StructField('i94bir', DoubleType(), True),
    StructField('i94visa', DoubleType(), True),
    StructField('count', DoubleType(), True),
    StructField('dtadfile', StringType(), True),
    StructField('visapost', StringType(), True),
    StructField('occup', StringType(), True),
    StructField('entdepa', StringType(), True),
    StructField('entdepd', StringType(), True),
    StructField('entdepu', StringType(), True),
    StructField('matflag', StringType(), True),
    StructField('biryear', DoubleType(), True),
    StructField('dtaddto', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('insnum', StringType(), True),
    StructField('airline', StringType(), True),
    StructField('admnum', DoubleType(), True),
    StructField('fltno', StringType(), True),
    StructField('visatype', StringType(), True),
    StructField('birth_year', IntegerType(), True),
    StructField('year', DoubleType(), True),
    StructField('month', DoubleType(), True),
    StructField('file_id', StringType(), True),
    StructField('origin_country_name', StringType(), True),
    StructField('origin_country_code', StringType(), True),
    StructField('arrival_country', StringType(), True),
    StructField('arrival_country_name', StringType(), True),
    StructField('arrival_state_code', StringType(), True),
    StructField('arrival_city_name', StringType(), True)
])

temperature_schema = StructType([
    StructField('dt', TimestampType(), True),
    StructField('average_temp ', DoubleType(), True),
    StructField('average_temp_uncertainty', DoubleType(), True),
    StructField('state_name', StringType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('country_name', StringType(), True),
    StructField('canon_country_name', StringType(), True),
    StructField('file_id', StringType(), True)
])

files_schema = StructType([
    StructField('input_file', StringType(), True),
    StructField('dataset', StringType(), True),
    StructField('file_id', StringType(), True),
    StructField('file_name', StringType(), True)
])

log_schema = StructType([
    StructField('current_timestamp', TimestampType(), True),
    StructField('file_id', StringType(), True),
    StructField('reason', StringType(), True)
])





