import json
import pathlib
from functools import partial
from typing import Optional

import pandas as pd
from pydeflate import set_pydeflate_path, oecd_dac_deflate, oecd_dac_exchange

from oda_data.clean_data.dtypes import set_default_types
from oda_data.clean_data.schema import CRS_MAPPING, ODASchema
from oda_data.clean_data.validation import validate_currency
from oda_data.config import ODAPaths
from oda_data.logger import logger

set_pydeflate_path(ODAPaths.raw_data)


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


# Create a helper function to consistently exchange data
dac_exchange = partial(
    oecd_dac_exchange,
    source_currency="USA",
    id_column=ODASchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)

# Create a helper function to consistently deflate data
dac_deflate = partial(
    oecd_dac_deflate,
    source_currency="USA",
    id_column=ODASchema.PROVIDER_CODE,
    use_source_codes=True,
    value_column="value",
    target_value_column="value",
    year_column="year",
)


def map_column_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Map the column names to the schema"""
    return df.rename(columns=CRS_MAPPING)


def keep_multi_donors_only(df: pd.DataFrame) -> pd.DataFrame:
    from oda_data import provider_groupings

    multilateral = provider_groupings()["multilateral"]
    df = df.loc[lambda d: d[ODASchema.PROVIDER_CODE].isin(multilateral)]

    return df


def convert_units(
    data,
    indicator: Optional[str] = None,
    currency: str = "USD",
    base_year: Optional[int] = None,
):
    if indicator is None:
        indicator = ""

    if ((".40." in indicator) and ("DAC1.40.1" != indicator)) or (
        currency == "USD" and base_year is None
    ):
        return data.assign(currency=currency, prices="current")

    elif base_year is None:
        return dac_exchange(
            data=data, target_currency=validate_currency(currency)
        ).assign(currency=currency, prices="current")

    else:
        return dac_deflate(
            data=data,
            base_year=base_year,
            target_currency=validate_currency(currency),
        ).assign(currency=currency, prices="constant")


def convert_dot_stat_to_data_explorer_codes(codes: list) -> list:
    from oda_reader.schemas.crs_translation import area_code_mapping

    mapping = area_code_mapping()

    result = []
    for code in codes:
        if code not in mapping:
            logger.warning(f"Code {code} does not have a new Data Explorer code.")
        else:
            result.append(mapping[code])

    return result
