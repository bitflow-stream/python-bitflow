import os


def get_env(key):
    var = os.getenv(key)
    if not var:
        raise IOError("Required environment variable {} is not set.".format(key))
    return var


def get_env_var_map(key):
    headers_str = get_env(key)
    parts = headers_str.split(",")
    result = {}
    for part in parts:
        key, value = part.split("=", 1)
        if not key or not value:
            raise IOError("Environment variable {} cannot be split into comma-separated key=value pairs: {}. "
                          "Problem occurs in {}."
                          .format(key, headers_str, part))
        else:
            result[key] = value
    return result
