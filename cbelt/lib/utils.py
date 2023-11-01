"""Contains some utils used over the project."""
import os
import pandas as pd
from datetime import datetime, date



def get_dict_env(dict_obj: dict, key: str, strict: bool = True):
    """Return value defined by key if exists or key-named end variable."""
    if key in dict_obj:
        return dict_obj.get(key)
    elif return_value := os.getenv(key):
        return return_value
    elif not strict:
        return None
    else:
        raise Exception(f"Mandatory parameter {key} not defined")


def timstamp_columns_to_string(df):
    """Convert timestamp columns in Dataframe to string."""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    return df

def dict_timestamp_to_string(dc):
    """Convert timestamp columns in dict to string."""
    for k, v in dc.items():
        if isinstance(dc[k], datetime) or isinstance(dc[k], date):
            dc[k] = dc[k].strftime("%d.%m.%Y, %H:%M:%S")

    return dc
