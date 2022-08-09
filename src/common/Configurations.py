from typing import Optional

# --- TAGS/IDS ---
PROCESSED_ROOT_TAG = '/udacity/dev/bucket/processed_data'
RAW_ROOT_TAG = '/udacity/dev/bucket/raw_data'
APPLICATION_ROOT_TAG = '/udacity/dev/bucket/application_data'

SAS_COUNTRIES_TAG = "sas_countries_path"
COUNTRY_CODES_TAG = "country_codes_path"
SAS_INDEX_TAG = "sas_index_path"
AIRPORT_CODES = "airport_codes"
DATALAKE_ROOT_TAG = "datalake_path"
IMMIGRATION_INPUT_DATA_TAG = "immigration_input_path"
IMMIGRATION_LAKE_DATA_TAG = "immigration_path"
IMMIGRATION_LAKE_FILES_TAG = "immigration_files"
IMMIGRATION_LAKE_LOADS_TAG = "immigration_loads"
TEMPERATURE_INPUT_DATA_TAG = "temperature_input_path"
TEMPERATURE_LAKE_DATA_TAG = "temperature_path"
TEMPERATURE_LAKE_FILES_TAG = "temperature_files"
TEMPERATURE_LAKE_LOADS_TAG = "temperature_loads"
SOURCE_ROOT_TAG = "sourcedata_path"

COUNTRIES_WITH_TEMPERATURE_DATA_TAG = "/config/immigration/supported-countries"
COUNTRIES_WITH_TEMPERATURE_DATA = """( 'AUSTRALIA',
    'BRAZIL',
    'CANADA',
    'CHINA',
    'INDIA',
    'RUSSIA',
    'UNITED STATES' )"""

# -- used for test only
SAS_COUNTRIES_TABLE = "/dev/sink/postgres/sas_countries_table"
COUNTRY_CODES_TABLE = "/dev/sink/postgres/country_codes_table"
SAS_INDEX_TABLE = "/dev/sink/postgres/sas_index_table"
AIRPORT_CODES_TABLE = "/dev/sink/postgres/airport_codes_table"


class Configurations2:

    def __init__(self, params: dict = None):

        processed_root = params[PROCESSED_ROOT_TAG]
        raw_root = params[RAW_ROOT_TAG]
        application_root = params[APPLICATION_ROOT_TAG]
        source_root = "../../source_data"

        self.configs: dict = {
            SOURCE_ROOT_TAG: source_root,

            DATALAKE_ROOT_TAG: processed_root,
            COUNTRIES_WITH_TEMPERATURE_DATA_TAG: COUNTRIES_WITH_TEMPERATURE_DATA,

            IMMIGRATION_INPUT_DATA_TAG: f"{raw_root}/immigration",
            IMMIGRATION_LAKE_DATA_TAG: f"{processed_root}/immigration/data",
            IMMIGRATION_LAKE_FILES_TAG: f"{processed_root}/immigration/controls/files",
            IMMIGRATION_LAKE_LOADS_TAG: f"{processed_root}/immigration/controls/loads",

            TEMPERATURE_INPUT_DATA_TAG: f"{raw_root}/temperature",
            TEMPERATURE_LAKE_DATA_TAG: f"{processed_root}/temperature",
            TEMPERATURE_LAKE_FILES_TAG: f"{processed_root}/controls/files",
            TEMPERATURE_LAKE_LOADS_TAG: f"{processed_root}/controls/loads",

            SAS_COUNTRIES_TAG: f"{processed_root}/commons/sas_countries",
            COUNTRY_CODES_TAG: f"{processed_root}/commons/country_codes",
            SAS_INDEX_TAG: f"{processed_root}/commons/sas_index",
            AIRPORT_CODES: f"{processed_root}/commons/airport_codes",
            SAS_COUNTRIES_TABLE: "commons.sas_countries",
            COUNTRY_CODES_TABLE: "commons.country_codes",
            SAS_INDEX_TABLE: "commons.sas_index",
            AIRPORT_CODES_TABLE: "commons.airport_codes",
        }

    def get_value(self, name: str, default_value: str = None) -> Optional[str]:
        if name in self.configs:
            return self.configs[name]
        elif default_value is not None:
            return default_value
        else:
            return None


def display_value(config: Configurations2, name: str):
    value = config.get_value(name)
    print(f"{name}\t== {value}")


if __name__ == "__main__":
    # --- todo move to json ---
    parameter_store = {
        '/udacity/dev/bucket/application_data': 'udacity-s3-applicationdata59764528-1cfss4k5she6z',
        '/udacity/dev/bucket/processed_data': 'udacity-s3-processeddata9c71eb6b-6mykt7l2d10j',
        '/udacity/dev/bucket/raw_data': 'udacity-s3-rawdata96bd17dc-xxg1i5n5o7qa',
        '/udacity/dev/emr/vpcId': 'vpc-0a0ea39a06f04a1a9',
        '/udacity/emr/public-subnet/1': 'subnet-0866e5073066e65eb',
        '/udacity/emr/public-subnet/2': 'subnet-06d45ff339723d127'
    }

    local_store = {
        '/udacity/dev/bucket/processed_data': "../../data/processed",
        '/udacity/dev/bucket/raw_data': "../../data/raw",
        '/udacity/dev/bucket/application_data': 'udacity-s3-applicationdata59764528-1cfss4k5she6z',
        '/udacity/dev/emr/vpcId': 'vpc-0a0ea39a06f04a1a9',
        '/udacity/emr/public-subnet/1': 'subnet-0866e5073066e65eb',
        '/udacity/emr/public-subnet/2': 'subnet-06d45ff339723d127'
    }

    config = Configurations2(params=parameter_store)

    display_value(config, RAW_ROOT_TAG)
    display_value(config, APPLICATION_ROOT_TAG)
    display_value(config, SAS_COUNTRIES_TAG)
    display_value(config, COUNTRY_CODES_TAG)
    display_value(config, SAS_INDEX_TAG)
    display_value(config, AIRPORT_CODES)
    display_value(config, DATALAKE_ROOT_TAG)
    display_value(config, IMMIGRATION_INPUT_DATA_TAG)
    display_value(config, IMMIGRATION_LAKE_DATA_TAG)
    display_value(config, IMMIGRATION_LAKE_FILES_TAG)
    display_value(config, IMMIGRATION_LAKE_LOADS_TAG)
    display_value(config, TEMPERATURE_INPUT_DATA_TAG)
    display_value(config, TEMPERATURE_LAKE_DATA_TAG)
    display_value(config, TEMPERATURE_LAKE_FILES_TAG)
    display_value(config, TEMPERATURE_LAKE_LOADS_TAG)
