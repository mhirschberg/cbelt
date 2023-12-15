"""Contains some utils used over the project."""
import os
import pandas as pd

def nvl_dict(dict_to_check, key_to_check, value_if_null):
    """Return dict_to_check[key_to_check] if set, else value_if_null"""
    try:
        return dict_to_check[key_to_check]
    except Exception:
        return value_if_null

def nvl(value_to_check, value_if_null):
    """Return 'value_to_check' if it is set and 'value_if_null' else"""
    if value_to_check:
        return value_to_check
    else:
        return value_if_null

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
