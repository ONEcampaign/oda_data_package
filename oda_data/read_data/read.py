import pandas as pd

from oda_data import config
from oda_data.clean_data.dtypes import set_default_types
from oda_data.get_data import common
from oda_data.get_data.common import resolve_crs_year_name
from oda_data.get_data.crs import download_crs
from oda_data.get_data.dac1 import download_dac1
from oda_data.get_data.dac2a import download_dac2a
from oda_data.get_data.multisystem import download_multisystem
from oda_data.logger import logger


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

    # check that all years are available. If not, download the missing years
    for year in years:
        year_, name = resolve_crs_year_name(year)
        if not (config.OdaPATHS.raw_data / f"crs_{name}_raw.feather").exists():
            logger.info(f"CRS data for {year} not found. Downloading...")
            download_crs(years=year)

    # Create an empty dataframe
    df = pd.DataFrame()

    # Loop over years
    for year in years:
        year_, name = resolve_crs_year_name(year)
        file = pd.read_feather(config.OdaPATHS.raw_data / f"crs_{name}_raw.feather")
        file = file.pipe(set_default_types)
        if len(df) > 0:
            df = pd.concat(
                [df.dropna(axis=1, how="all"), file.dropna(axis=1, how="all")],
                ignore_index=True,
            )
        else:
            df = file.copy()
    return df


def read_dac1(years: int | list | range) -> pd.DataFrame:
    """Read the DAC1 data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(years=years, file_name="table1_raw.feather")
    except FileNotFoundError:
        logger.info("DAC1 data not found. Downloading...")
        download_dac1()
        return __read_table(years=years, file_name="table1_raw.feather")


def read_dac2a(years: int | list | range) -> pd.DataFrame:
    """Read the DAC2a data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(years=years, file_name="table2a_raw.feather")
    except FileNotFoundError:
        logger.info("DAC2a data not found. Downloading...")
        download_dac2a()
        return __read_table(years=years, file_name="table2a_raw.feather")


def read_multisystem(years: int | list | range) -> pd.DataFrame:
    """Read the Multisystem data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(years=years, file_name="multisystem_raw.feather")
    except FileNotFoundError:
        logger.info("Multisystem data not found. Downloading...")
        download_multisystem()
        return __read_table(years=years, file_name="multisystem_raw.feather")
