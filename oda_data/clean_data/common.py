import json
import pathlib
from functools import partial

import pandas as pd
from pydeflate import exchange, deflate

from oda_data.logger import logger


def clean_column_name(column_name: str) -> str:
    """Clean column names by removing spaces, special characters, and lowercasing"""

    # Check for all caps convention
    if column_name.isupper():
        column_name = column_name.lower() + "_code"

    single_string = ""

    # split the string into substrings when the case changes
    for i, char in enumerate(column_name):
        if char.isupper() and i != 0:
            if single_string[-1].isupper():
                single_string += char
            else:
                single_string += "_" + char
        else:
            single_string += char

    return (
        single_string.strip()
        .lower()
        .replace(" ", "_")
        .replace("__", "_")
        .replace("-", "")
    )


def read_settings(settings_file_path: pathlib.Path) -> dict:
    """Read the settings file. Each DAC source has a specific settings file"""

    with open(settings_file_path, "r") as f:
        settings = json.load(f)

    return settings


def _validate_columns(df: pd.DataFrame, dtypes: dict) -> dict:
    """Check that all columns in the DataFrame are in the dtypes dictionary"""

    clean_types = {}

    for col in df.columns:
        if col not in dtypes.keys():
            logger.warn(f"Column {col} not in dtypes dictionary")

    for col in dtypes.keys():
        if col not in df.columns:
            logger.warn(f"Column {col} not in DataFrame")
        else:
            clean_types[col] = dtypes[col]

    return clean_types


def clean_raw_df(
    df: pd.DataFrame, settings_dict: dict, small_version: bool
) -> pd.DataFrame:
    """Rename columns, set correct data types, and optionally drop columns"""

    df = df.rename(columns=lambda c: clean_column_name(c))

    # Extract data types and columns to keep
    dtypes = {c: t["type"] for c, t in settings_dict.items()}
    keep_cols = [c for c, t in settings_dict.items() if t["keep"]]

    # check that all columns are in the dtypes dictionary
    dtypes = _validate_columns(df, dtypes)

    # convert the columns to the correct type
    df = df.replace("\x1a", pd.NA).astype(dtypes, errors="ignore")

    # Optionally keep only the columns that are in the settings file
    if small_version:
        df = df.filter(keep_cols, axis=1)

    return df


# Create a helper function to consistently exchange data
dac_exchange = partial(
    exchange,
    source_currency="USA",
    rates_source="oecd_dac",
    id_column="donor_code",
    id_type="DAC",
    value_column="value",
    target_column="value",
    date_column="year",
)

# Create a helper function to consistently deflate data
dac_deflate = partial(
    deflate,
    source="oecd_dac",
    source_currency="USA",
    id_column="donor_code",
    id_type="DAC",
    source_col="value",
    target_col="value",
    date_column="year",
)
