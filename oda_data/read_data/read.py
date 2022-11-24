import pandas as pd

from oda_data import config
from oda_data.get_data import common


def __read_table(years: int | list | range, file_name: str) -> pd.DataFrame:
    # Check that list of years is valid
    years = common.check_integers(years)

    # Read the table
    df = pd.read_feather(config.OdaPATHS.raw_data / file_name)

    return df.loc[lambda d: d.year.isin(years)].reset_index(drop=True)


def read_crs(years: int | list | range) -> pd.DataFrame:
    """Read the CRS data for the specified years."""
    # Check that list of years is valid
    years = common.check_integers(years)

    # Create an empty dataframe
    df = pd.DataFrame()

    # Loop over years
    for year in years:
        df = pd.concat(
            [df, pd.read_feather(config.OdaPATHS.raw_data / f"crs_{year}_raw.feather")]
        )
    return df


def read_dac1(years: int | list | range) -> pd.DataFrame:
    """Read the DAC1 data for the specified years."""
    # Check that list of years is valid
    return __read_table(years=years, file_name="table1_raw.feather")


def read_dac2a(years: int | list | range) -> pd.DataFrame:
    """Read the DAC2a data for the specified years."""
    # Check that list of years is valid
    return __read_table(years=years, file_name="table2a_raw.feather")


def read_multisystem(years: int | list | range) -> pd.DataFrame:
    """Read the Multisystem data for the specified years."""
    # Check that list of years is valid
    return __read_table(years=years, file_name="multisystem_raw.feather")
