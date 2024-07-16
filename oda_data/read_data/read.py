import pandas as pd

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.clean_data.dtypes import set_default_types
from oda_data.get_data import common
from oda_data.get_data.common import resolve_crs_year_name
from oda_data.get_data.crs import download_crs
from oda_data.get_data.dac1 import download_dac1
from oda_data.get_data.dac2a import download_dac2a
from oda_data.get_data.multisystem import download_multisystem
from oda_data.logger import logger


def __read_table(
    years: int | list | range, file_name: str, check_years: bool = False
) -> pd.DataFrame:
    # Check that list of years is valid
    years = common.check_integers(years)

    if check_years:
        start_year = min(years)
        end_year = max(years)
        file_name = f"{file_name.split('.')[0]}_{start_year}_{end_year}.feather"

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
        if not (
            (config.OdaPATHS.raw_data / f"crs_{name}_raw.feather").exists()
            or (config.OdaPATHS.raw_data / "fullCRS.parquet").exists()
        ):
            logger.info(f"CRS data for {year} not found. Downloading...")
            download_crs(years=year)

    # If using the parquet file, use predicate pushdown to filter the data by year
    # and avoid reading too much data into memory
    if (config.OdaPATHS.raw_data / "fullCRS.parquet").exists():
        filters = [("year", "in", years)]

        df = pd.read_parquet(
            config.OdaPATHS.raw_data / "fullCRS.parquet",
            filters=filters,
            engine="pyarrow",
        ).pipe(clean_raw_df)

        return df.pipe(set_default_types)

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
        return __read_table(
            years=years, file_name="table1_raw.feather", check_years=True
        )
    except FileNotFoundError:
        years = common.check_integers(years)
        start_year = min(years)
        end_year = max(years)
        download_dac1(start_year=start_year, end_year=end_year)
        return __read_table(
            years=years, file_name="table1_raw.feather", check_years=True
        )


def read_dac2a(years: int | list | range) -> pd.DataFrame:
    """Read the DAC2a data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(
            years=years, file_name="table2a_raw.feather", check_years=True
        )
    except FileNotFoundError:
        years = common.check_integers(years)
        start_year = min(years)
        end_year = max(years)
        download_dac2a(start_year=start_year, end_year=end_year)
        return __read_table(
            years=years, file_name="table2a_raw.feather", check_years=True
        )


def read_multisystem(years: int | list | range) -> pd.DataFrame:
    """Read the Multisystem data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(years=years, file_name="multisystem_raw.feather")
    except FileNotFoundError:
        logger.info("Multisystem data not found. Downloading...")
        download_multisystem()
        return __read_table(years=years, file_name="multisystem_raw.feather")
