import pandas as pd

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.clean_data.dtypes import set_default_types
from oda_data.get_data import common
from oda_data.get_data.crs import download_crs
from oda_data.get_data.dac1 import download_dac1
from oda_data.get_data.dac2a import download_dac2a
from oda_data.get_data.multisystem import download_multisystem
from oda_data.logger import logger


def add_years_to_filter(
    filters: list[tuple] | None, years: int | list | range
) -> list[tuple]:
    if filters:
        if not any([f[0] == "year" for f in filters]):
            filters.append(("year", "in", years))
    else:
        filters = [("year", "in", years)]
    return filters


def __read_table(
    years: int | list | range,
    file_name: str,
    check_years: bool = False,
    filters: list[tuple] | None = None,
) -> pd.DataFrame:
    # Check that list of years is valid
    years = common.check_integers(years)

    if check_years:
        start_year = min(years)
        end_year = max(years)
        file_name = f"{file_name.split('.')[0]}_{start_year}_{end_year}.feather"

    filters = add_years_to_filter(filters, years)

    # Read the table
    df = pd.read_parquet(
        config.OdaPATHS.raw_data / file_name, engine="pyarrow", filters=filters
    )

    return df


def read_crs(
    years: int | list | range, filters: list[tuple] | None = None
) -> pd.DataFrame:
    """Read the CRS data for the specified years."""
    # Check that list of years is valid
    years = common.check_integers(years)

    # check that all years are available. If not, download the missing years
    if not (config.OdaPATHS.raw_data / "fullCRS.parquet").exists():
        logger.info(f"CRS data not found. Downloading...")
        download_crs()

    # If using the parquet file, use predicate pushdown to filter the data by year
    # and avoid reading too much data into memory
    if (config.OdaPATHS.raw_data / "fullCRS.parquet").exists():
        filters = add_years_to_filter(filters, years)

        df = pd.read_parquet(
            config.OdaPATHS.raw_data / "fullCRS.parquet",
            filters=filters,
            engine="pyarrow",
        ).pipe(clean_raw_df)

        return df.pipe(set_default_types)


def read_dac1(
    years: int | list | range, filters: list[tuple] | None = None
) -> pd.DataFrame:
    """Read the DAC1 data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(
            years=years,
            file_name="table1_raw.parquet",
            check_years=True,
            filters=filters,
        )
    except FileNotFoundError:
        years = common.check_integers(years)
        start_year = min(years)
        end_year = max(years)
        download_dac1(start_year=start_year, end_year=end_year)
        return __read_table(
            years=years,
            file_name="table1_raw.parquet",
            check_years=True,
            filters=filters,
        )


def read_dac2a(
    years: int | list | range, filters: list[tuple] | None = None
) -> pd.DataFrame:
    """Read the DAC2a data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(
            years=years,
            file_name="table2a_raw.parquet",
            check_years=True,
            filters=filters,
        )
    except FileNotFoundError:
        years = common.check_integers(years)
        start_year = min(years)
        end_year = max(years)
        download_dac2a(start_year=start_year, end_year=end_year)
        return __read_table(
            years=years,
            file_name="table2a_raw.parquet",
            check_years=True,
            filters=filters,
        )


def read_multisystem(
    years: int | list | range, filters: list[tuple] | None = None
) -> pd.DataFrame:
    """Read the Multisystem data for the specified years."""
    # Check that list of years is valid
    try:
        return __read_table(
            years=years, file_name="multisystem_raw.parquet", filters=filters
        )
    except FileNotFoundError:
        logger.info("Multisystem data not found. Downloading...")
        download_multisystem()
        return __read_table(
            years=years, file_name="multisystem_raw.parquet", filters=filters
        )
