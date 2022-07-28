import pandas as pd
import re
from sqlalchemy import create_engine
from typing import Callable

engine = create_engine('postgresql://postgres:chinois1@localhost:5432/postgres')

code_pattern = r"(?P<id>\d*)(\s*=\s*\')(?P<value>[^\']*)"
iana_pattern = r"(?P<id>^[^=]*)(?P<sep>\s*=\s*\')(?P<value>[^\']*)"


def transform_city_codes(line: str):
    try:
        regexp_1 = re.compile(code_pattern)
        re_match = regexp_1.match(line.strip())
        id = re_match.group('id')
        value = re_match.group('value')
        segments = value.split(":")
        status = 'VALID'
        location = "N/A"
        if len(segments) > 1:
            status = segments[0]
            location = segments[1]
        elif value.startswith('No Country Code') > 0:
            status = "UNKNOWN"
            location = "NONE"
        else:
            location = segments[0]
        return dict(city_code=int(id), country=location, status=status, line=line.strip())

    except Exception as e:
        return dict(city_code=None, status="FAILED", country="", line=line.strip())


def transform_iana_names(line: str):
    try:
        regexp_1 = re.compile(iana_pattern)
        re_match = regexp_1.match(line.strip())
        id = re_match.group('id')
        value = re_match.group('value')

        value = value.strip("'")
        code = id.strip().strip("'")
        country="UNKNOWN"
        state = "UNKNOWN"
        comma = str(value).rfind(",")
        if comma > 0:
            country = value[comma+1:].strip()
            state = value[0:comma].strip()
        else:
            state = value
        return dict(iana_code=code,     city_name=state,          state_code=country, status='VALID', line=line.strip())

    except Exception as e:
        return dict(iana_code='', city_name='', state_code='',     status='FAILED', line=line.strip())


def load_state_names(file_name: str, table_name: str, schema: str):
    df = pd.read_csv(file_name)
    df.to_sql(name=table_name, schema=schema, con=engine,  if_exists='replace')


def load_column_defs(file_name: str, table_name: str, schema: str, func: Callable):
    data = []
    with open(file_name, "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            if len(line.strip()) == 0:
                continue
            data.append(func(line))

    df = pd.DataFrame(data)
    df.to_sql(name=table_name, schema=schema, con=engine,  if_exists='replace')


if __name__ == "__main__":
    load_column_defs(file_name='../data/controls/city_codes.txt', table_name='city_codes', schema="controls", func=transform_city_codes)
    load_column_defs(file_name='../data/controls/iana_names.txt', table_name='iana_names', schema="controls", func=transform_iana_names)
    load_state_names(file_name='../data/controls/state_names.csv', table_name='state_names', schema='controls')