import pathlib

from oda_data import config
from oda_data.clean_data.common import read_settings, clean_raw_df
from oda_data.get_data import common
from oda_data.logger import logger


def _download_save(
    file_url: str,
    year: int,
    config_file_path: pathlib.Path,
    small_version: bool = False,
) -> None:
    # Get file content
    file_content = common.get_zip(file_url)

    # Extract file as dataframe
    file_name = f"CRS {year} data.txt"
    df = common.read_zip_content(request_content=file_content, file_name=file_name)

    # settings
    settings = read_settings(config_file_path)

    # Clean the DataFrame
    df = clean_raw_df(df, settings, small_version)

    # Save the DataFrame
    df.to_feather(config.OdaPATHS.raw_data / f"crs_{year}_raw.feather")
    logger.info(f"CRS {year} data downloaded and saved.")


def download_crs(years: int | list | range, small_version: bool = False) -> None:
    """Download the CRS file for the specified year from the OECD. Store.
    This function stores the raw data as a feather file in the raw data folder."""

    years = common.check_integers(years)

    crs_dict = common.extract_file_link_multiple(config.CRS_URL)

    for year in years:
        if year not in crs_dict:
            logger.info(f"CRS data for {year} is not available.")
            continue

        _download_save(
            file_url=crs_dict[year],
            year=year,
            config_file_path=config.OdaPATHS.cleaning_config / "crs_config.json",
            small_version=small_version,
        )
