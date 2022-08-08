from typing import Optional

SAS_COUNTRIES_TAG =             "/dev/sink/stage/fs/gold/sas_countries_path"
COUNTRY_CODES_TAG =             "/dev/sink/stage/fs/gold/country_codes_path"
SAS_INDEX_TAG =                 "/dev/sink/fs/gold/sas_index_path"
AIRPORT_CODES =                 "/dev/sink/fs/gold/airport_codes"
DATALAKE_ROOT_TAG =             "/dev/sink/fs/gold/datalake_path"

SOURCE_ROOT_TAG =               "/dev/source/fs/source/sourcedata_path"
IMMIGRATION_INPUT_DATA_TAG =    "/dev/sink/fs/input/immigration_path"
IMMIGRATION_LAKE_DATA_TAG =    "/dev/sink/fs/lake/immigration_path"
IMMIGRATION_LAKE_FILES_TAG = "/dev/sink/fs/lake/immigration_files"
IMMIGRATION_LAKE_LOADS_TAG = "/dev/sink/fs/lake/immigration_loads"
IMMIGRATION_CHECKPOINTS_TAG = "/dev/checkpoint/immigration"

TEMPERATURE_INPUT_DATA_TAG =    "/dev/sink/fs/input/temperature"
TEMPERATURE_LAKE_DATA_TAG =    "/dev/sink/fs/lake/temperature_path"
TEMPERATURE_LAKE_FILES_TAG = "/dev/sink/fs/lake/temperature_files"
TEMPERATURE_LAKE_LOADS_TAG = "/dev/sink/fs/lake/temperature_loads"


# -- used for test only
SAS_COUNTRIES_TABLE = "/dev/sink/postgres/sas_countries_table"
COUNTRY_CODES_TABLE = "/dev/sink/postgres/country_codes_table"
SAS_INDEX_TABLE = "/dev/sink/postgres/sas_index_table"
AIRPORT_CODES_TABLE = "/dev/sink/postgres/airport_codes_table"
COUNTRIES_WITH_TEMPERATURE_DATA_TAG = "/config/immigration/supported-countries"
COUNTRIES_WITH_TEMPERATURE_DATA = """( 'AUSTRALIA',
    'BRAZIL',
    'CANADA',
    'CHINA',
    'INDIA',
    'RUSSIA',
    'UNITED STATES' )"""


class Configurations:
    PROCESSED_ROOT = "../../data/processed"
    SOURCE_ROOT = "../../source_data"
    RAW_ROOT = "../../data/raw"

    configs: dict = {
        DATALAKE_ROOT_TAG: PROCESSED_ROOT,
        SOURCE_ROOT_TAG: SOURCE_ROOT,
        COUNTRIES_WITH_TEMPERATURE_DATA_TAG: COUNTRIES_WITH_TEMPERATURE_DATA,

        IMMIGRATION_INPUT_DATA_TAG: f"{RAW_ROOT}/immigration",
        IMMIGRATION_LAKE_DATA_TAG: f"{PROCESSED_ROOT}/immigration/data",
        IMMIGRATION_CHECKPOINTS_TAG: f"{PROCESSED_ROOT}/immigration/checkpoints",
        IMMIGRATION_LAKE_FILES_TAG: f"{PROCESSED_ROOT}/immigration/controls/files",
        IMMIGRATION_LAKE_LOADS_TAG: f"{PROCESSED_ROOT}/immigration/controls/loads",

        TEMPERATURE_INPUT_DATA_TAG: f"{RAW_ROOT}/temperature",
        TEMPERATURE_LAKE_DATA_TAG: f"{PROCESSED_ROOT}/temperature",
        TEMPERATURE_LAKE_FILES_TAG: f"{PROCESSED_ROOT}/controls/files",
        TEMPERATURE_LAKE_LOADS_TAG: f"{PROCESSED_ROOT}/controls/loads",

        SAS_COUNTRIES_TAG: f"{PROCESSED_ROOT}/commons/sas_countries",
        COUNTRY_CODES_TAG: f"{PROCESSED_ROOT}/commons/country_codes",
        SAS_INDEX_TAG: f"{PROCESSED_ROOT}/commons/sas_index",
        AIRPORT_CODES: f"{PROCESSED_ROOT}/commons/airport_codes",
        SAS_COUNTRIES_TABLE: "commons.sas_countries",
        COUNTRY_CODES_TABLE: "commons.country_codes",
        SAS_INDEX_TABLE: "commons.sas_index",
        AIRPORT_CODES_TABLE: "commons.airport_codes",
    }

    def __init__(self):
        pass

    def get_value(self, name: str, default_value: str = None) -> Optional[str]:
        if name in self.configs:
            return self.configs[name]
        elif default_value is not None:
            return default_value
        else:
            return None
