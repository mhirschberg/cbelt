"""Contains some utils used over the project."""
import os


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
