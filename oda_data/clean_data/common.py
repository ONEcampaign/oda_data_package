import json
import pathlib
from functools import partial

import pandas as pd
from pydeflate import deflate, exchange, set_pydeflate_path

from oda_data.config import OdaPATHS
from oda_data.logger import logger

set_pydeflate_path(OdaPATHS.raw_data)


def clean_column_name(column_name: str) -> str:
    """Clean a column name by removing spaces, special characters, and lowercasing.

    Args:
        column_name (str): The column name to clean.

    Returns:
        str: The cleaned column name.

    Examples:
        >>> clean_column_name("Donor Code")
        "donor_code"
        >>> clean_column_name("recipientName")
        "recipient_name"
        >>> clean_column_name("sector-code")
        "sector_code"
    """

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


def read_settings(settings_file_path: pathlib.Path | str) -> dict:
    """Read the settings file for a DAC source.

    Args:
        settings_file_path: The path to the settings file.

    Returns:
        dict: A dictionary containing the settings.
    """

    with open(settings_file_path, "r") as f:
        settings = json.load(f)

    return settings


def _validate_columns(df: pd.DataFrame, dtypes: dict) -> dict:
    """Check that all columns in the DataFrame are in the dtypes dictionary"""

    clean_types = {}

    for col in df.columns:
        if col not in dtypes.keys():
            logger.warning(f"Column {col} not in dtypes dictionary")

    for col in dtypes.keys():
        if col not in df.columns:
            logger.warning(f"Column {col} not in DataFrame")
        else:
            clean_types[col] = dtypes[col]

    return clean_types


def clean_raw_df(
    df: pd.DataFrame, settings_dict: dict, small_version: bool
) -> pd.DataFrame:
    """Clean a raw dataframe by renaming columns, setting correct data types,
    and optionally dropping columns.

    Args:
        df (pd.DataFrame): The raw dataframe to clean.
        settings_dict (dict): A dictionary containing the settings for cleaning the dataframe.
        small_version (bool): A flag indicating whether to keep only the columns
            specified in the settings.

    Returns:
        pd.DataFrame: The cleaned dataframe.
    """

    df = df.rename(columns=lambda c: clean_column_name(c))

    # Extract data types and columns to keep
    dtypes = {c: t["type"] for c, t in settings_dict.items()}
    keep_cols = [c for c, t in settings_dict.items() if t["keep"]]

    # check that all columns are in the dtypes dictionary
    dtypes = _validate_columns(df, dtypes)

    # convert the columns to the correct type
    try:
        df = df.replace("\x1a", pd.NA).astype(dtypes, errors="ignore")
    except TypeError:
        df = df.astype(dtypes, errors="ignore")

    # Optionally keep only the columns that are in the settings file
    if small_version:
        df = df.filter(keep_cols, axis=1)

    return df


def _cols_in_list(all_columns: list, cols_list: list) -> list:
    """Return a list of columns that are in the all_columns list"""
    return [c for c in cols_list if c in all_columns]


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder the columns of a dataframe to have a more predictable output.

    Args:
        df (pd.DataFrame): The dataframe to reorder.

    Returns:
        pd.DataFrame: The reordered dataframe.
    """

    # Get all columns
    all_columns = df.columns.tolist()

    # Columns to appear first
    reorder_b = _cols_in_list(
        all_columns,
        [
            "year",
            "indicator",
            "donor_code",
            "donor_name",
            "recipient_code",
            "recipient_name",
        ],
    )

    # Columns to appear last
    reorder_l = _cols_in_list(
        all_columns, ["currency", "prices", "value", "share", "total_of", "gni_share"]
    )

    new_order = (
        reorder_b
        + [c for c in all_columns if c not in reorder_b + reorder_l]
        + reorder_l
    )

    return df.filter(new_order, axis=1).sort_values(by=new_order).reset_index(drop=True)


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
    deflator_source="oecd_dac",
    deflator_method="dac_deflator",
    exchange_source="oecd_dac",
    exchange_method="implied",
    source_currency="USA",
    id_column="donor_code",
    id_type="DAC",
    source_column="value",
    target_column="value",
    date_column="year",
)
