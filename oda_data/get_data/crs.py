import pathlib

import pandas as pd
import requests

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.get_data import common
from oda_data.get_data.common import resolve_crs_year_name
from oda_data.logger import logger


def _save(df: pd.DataFrame, year: int, save_path: pathlib.Path) -> None:
    # Clean the DataFrame
    df = clean_raw_df(df)

    # Save the DataFrame
    df.to_feather(save_path / f"crs_{year}_raw.feather")
    logger.info(f"CRS {year} data downloaded and saved.")


def _download(file_url: str, year: int) -> pd.DataFrame:
    logger.info(f"Downloading CRS data for {year}... This may take a while.")

    # Get file content
    file_content: requests.Response = common.get_zip(file_url)

    return common.read_zip_content(
        request_content=file_content, file_name=f"CRS {year} data.txt"
    )


def _years(years: int | list | range, crs_dict: dict) -> list:
    years = common.check_integers(years)

    for year in years:
        if year not in crs_dict and (year not in range(1973, 2005)):
            logger.info(f"CRS data for {year} is not available.")

    return [y for y in years if y in crs_dict or y in range(1973, 2005)]


def download_crs(years: int | list | range, small_version: bool = False) -> None:
    """Download CRS files for the specified year(s) from the OECD and store
     them as feather files.


    Args:
        years: The year(s) for which to download the CRS data.
        small_version: If True, only save a small version of the CRS data (default is False).
    """

    crs_dict = common.extract_file_link_multiple(config.CRS_URL)

    for year in _years(years, crs_dict):
        year, name = resolve_crs_year_name(year)
        df = _download(file_url=crs_dict[year], year=name)
        _save(df=df, year=name, save_path=config.OdaPATHS.raw_data)
