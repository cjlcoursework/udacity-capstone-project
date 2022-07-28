import pandas as pd
import re
from sqlalchemy import create_engine
from typing import Callable

engine = create_engine('postgresql://postgres:chinois1@localhost:5432/postgres')


def load_padas_parquet():
    df = pd.read_parquet("../data/udacity/sas_data", engine='auto', columns=None, storage_options=None, use_nullable_dtypes=False)
    df.to_sql(name='pandas_immigration', schema="public", con=engine,  if_exists='replace')


if __name__ == "__main__":
    load_padas_parquet()
