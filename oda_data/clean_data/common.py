import json
import pathlib
from functools import partial

import pandas as pd
from pydeflate import set_pydeflate_path, oecd_dac_deflate, oecd_dac_exchange

from oda_data.clean_data.dtypes import set_default_types
from oda_data.clean_data.schema import CRS_MAPPING, OdaSchema
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


def clean_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean a raw dataframe by renaming columns, setting correct data types,
    and optionally dropping columns.

    Args:
        df (pd.DataFrame): The raw dataframe to clean.

    Returns:
        pd.DataFrame: The cleaned dataframe.
    """

    df = df.rename(columns=lambda c: clean_column_name(c))

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").astype(
            "float64[pyarrow]"
        )

    df = df.pipe(map_column_schema).replace("\x1a", pd.NA).pipe(set_default_types)

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
            OdaSchema.YEAR,
            OdaSchema.INDICATOR,
            OdaSchema.PROVIDER_CODE,
            OdaSchema.PROVIDER_NAME,
            OdaSchema.RECIPIENT_CODE,
            OdaSchema.RECIPIENT_NAME,
        ],
    )

    # Columns to appear last
    reorder_l = _cols_in_list(
        all_columns,
        [
            OdaSchema.CURRENCY,
            OdaSchema.PRICES,
            OdaSchema.VALUE,
            OdaSchema.SHARE,
            "total_of",
            "gni_share",
        ],
    )

    new_order = (
        reorder_b
        + [c for c in all_columns if c not in reorder_b + reorder_l]
        + reorder_l
    )

    new_order = list(dict.fromkeys(new_order))

    # Keep columns in right order
    df = df.filter(new_order, axis=1)

    # Try sorting
    try:
        df = df.sort_values(by=new_order).reset_index(drop=True)
    except:
        pass

    return df


# Create a helper function to consistently exchange data
dac_exchange = partial(
    oecd_dac_exchange,
    source_currency="USA",
    id_column=OdaSchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)

# Create a helper function to consistently deflate data
dac_deflate = partial(
    oecd_dac_deflate,
    source_currency="USA",
    id_column=OdaSchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)


def map_column_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Map the column names to the schema"""
    return df.rename(columns=CRS_MAPPING)


def keep_multi_donors_only(df: pd.DataFrame) -> pd.DataFrame:
    from oda_data import donor_groupings

    bilateral = donor_groupings()["all_bilateral"]
    df = df.loc[lambda d: ~d[OdaSchema.PROVIDER_CODE].isin(bilateral)]

    return df
