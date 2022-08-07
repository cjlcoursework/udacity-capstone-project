configuration = {
    'test': {
        "input_path": "../../data_test",
        "input_format": "parquet",
        "output_path": "../../datalake/"
    },
    'local': {
        "input_path": "../../data/input_data/",
        "input_format": "parquet",
        "output_path": "../../datalake/"
    },
    'prod': {
        "input_path": "../../data/input_data/",
        "input_format": "com.github.saurfang.sas.spark",
        "output_path": "../../datalake/"
    }
}
