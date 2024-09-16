import io
import pathlib

import pandas as pd
import requests

from oda_data import config
from oda_data.clean_data.common import clean_raw_df
from oda_data.get_data import common
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


def cloud_crs_download():
    file_content = common.fetch_file_from_url_selenium(config.FULL_CRS_URL)

    if isinstance(file_content, io.BytesIO):
        file_content = file_content.getvalue()

    # Open the local file in write-binary mode
    with open(config.OdaPATHS.raw_data / "fullCRS.parquet", "wb") as file:
        file.write(file_content)

        logger.info("Full CRS data downloaded successfully.")


def alternative_crs_download() -> None:
    """Download the CRS data from an alternative source."""

    from bs4 import BeautifulSoup

    logger.info("Downloading CRS data from the alternative source...")

    # Initial URL to the file on Google Drive
    file_id = "1bgiSE5_bHPIjGuGggITNPScWgg2V4j2M"
    file_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    # Session to persist cookies
    session = requests.Session()

    # Make a request to get the download warning page
    response = session.get(file_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the confirmation token
    confirm_token = soup.find("input", {"name": "confirm"})["value"]
    uuid_token = soup.find("input", {"name": "uuid"})["value"]

    # Generate the final download URL
    download_url = (
        f"https://drive.usercontent.google.com/download?id={file_id}&"
        f"export=download&confirm={confirm_token}&uuid={uuid_token}"
    )

    # Make a request to download the file using the final download URL
    response = session.get(download_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Open the local file in write-binary mode
        with open(config.OdaPATHS.raw_data / "fullCRS.parquet", "wb") as file:
            file.write(response.content)

        logger.info("Full CRS data downloaded successfully.")
    else:
        logger.error("Failed to download the CRS data from the alternative source.")


def download_crs(years: int | list | range = None, small_version: bool = False) -> None:
    """Download full CRS file.

    The years parameter is no longer in use.


    Args:
        years: The year(s) for which to download the CRS data.
        small_version: If True, only save a small version of the CRS data (default is False).
    """

    try:
        logger.info(
            "The full, detailed CRS is only available as a large file (>1GB). "
            "The package will now download the data, but it may take a while."
        )
        cloud_crs_download()

    except ConnectionError:
        logger.warning(
            "Could not connect to the OECD website. The OECD"
            "have released a new site which broke many of the data download"
            "tools.\n The package will now try to fetch the data from a different repository"
            "but this will take a long time. The provided file includes the full CRS and it is"
            "almost 1GB in size."
        )
        alternative_crs_download()
