import csv
import io
import os
import pathlib
import tempfile
import time
import zipfile as zf

import bs4
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from oda_data.clean_data.common import clean_raw_df, read_settings
from oda_data.logger import logger


def _get_driver() -> webdriver.chrome:
    """Get driver for Chrome. A folder name must be provided to save the files to.

    Returns:
        A webdriver.chrome object.

    """

    # Create options
    options = webdriver.ChromeOptions()

    # Add arguments and options to options
    options.add_argument("--no-sandbox")
    options.add_argument("headless")

    # Get driver
    chrome = ChromeDriverManager().install()

    # Return driver with the options
    return webdriver.Chrome(service=Service(chrome), options=options)


def get_url_selenium(url: str) -> webdriver.chrome:
    """
    Get the url using selenium

    Args:
        url: The URL to fetch the file from.

    Returns:
        A webdriver.chrome object.

    """
    # get driver
    driver = _get_driver()

    # Get page
    driver.get(url)

    return driver


def fetch_file_from_url_selenium(url: str) -> io.BytesIO:
    """
    Downloads a file from a specified URL using Selenium in headless mode
    and reads it into memory.

    Args:
        url: The URL to fetch the file from.

    Returns:
        A bytes object containing the file data.
    """
    # Create a temporary directory for downloads
    download_dir = tempfile.mkdtemp()

    # Set up Chrome options
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_experimental_option(
        name="prefs",
        value={
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        },
    )

    # Get driver
    chrome = ChromeDriverManager().install()

    # Return driver with the options
    driver = webdriver.Chrome(service=Service(chrome), options=options)

    # define downloaded file
    downloaded_file = None
    try:
        driver.get(url)
        time.sleep(4)

        # Check for the completion of the download by monitoring the absence
        # of .crdownload files
        while any(
            file_name.endswith(".crdownload") for file_name in os.listdir(download_dir)
        ):
            time.sleep(1)  # Check every second

        # Once downloaded, read the file into memory
        downloaded_file = os.path.join(download_dir, os.listdir(download_dir)[0])
        with open(downloaded_file, "rb") as file:
            file_data = io.BytesIO(file.read())
    finally:
        driver.quit()
        if downloaded_file is not None:
            os.remove(downloaded_file)
        os.rmdir(download_dir)

    return file_data


def _checktype(values: list | int | float, type_: type) -> list:
    """Take a list, int or float and return a list of integers."""

    if isinstance(values, list):
        return [type_(d) for d in values]
    elif isinstance(values, str):
        return [type_(values)]
    elif isinstance(values, float):
        return [type_(values)]
    elif isinstance(values, int):
        return [type_(values)]
    else:
        raise ValueError("Invalid values passed. Please check the type and try again.")


def check_integers(values: list | int) -> list[int]:
    """Take a list or int and return a list of integers."""
    if isinstance(values, range):
        return list(values)

    return _checktype(values, int)


def get_zip(url) -> requests.Response | io.BytesIO:
    """Download the zip file from the web"""

    try:
        return requests.get(url)
    except requests.exceptions.SSLError:
        return fetch_file_from_url_selenium(url)
    except ConnectionError:
        raise ConnectionError(f"Could not download file from {url}")


def _serialise_content(response: requests.Response) -> io.BytesIO:
    """Serialise the content of the response object"""
    return io.BytesIO(response.content)


def _extract_zip(serialised_content: io.BytesIO, file_name: str) -> csv:
    """Extract a file from a zip file. This creates a deep copy of the content"""
    return zf.ZipFile(serialised_content).open(file_name)


def _raw2df(csv_file: csv, sep: str, encoding: str) -> pd.DataFrame:
    """Convert a raw csv to a DataFrame. Check the result if requested"""

    try:
        _ = pd.read_csv(
            csv_file, sep=sep, dtype="str", encoding=encoding, low_memory=False
        )
    except UnicodeError:
        raise pd.errors.ParserError

    unnamed_cols = [c for c in _.columns if "unnamed" in c]

    if len(unnamed_cols) > 3:
        raise pd.errors.ParserError

    if len(_.columns) < 3:
        raise pd.errors.ParserError

    return _


def _extract_df(request_content, file_name: str, separator: str) -> pd.DataFrame:
    import copy

    for encoding in ["utf_16", "ISO-8859-1"]:
        try:
            if not isinstance(request_content, io.BytesIO):
                rc = copy.deepcopy(request_content)
                # serialise the content
                serialised_content = _serialise_content(rc)

            else:
                serialised_content = request_content

            # extract the file
            raw_csv = _extract_zip(serialised_content, file_name)

            # convert to a dataframe
            return _raw2df(csv_file=raw_csv, sep=separator, encoding=encoding)
        except pd.errors.ParserError:
            logger.debug(f"{encoding} not valid")

    raise pd.errors.ParserError


def read_zip_content(
    request_content, file_name: str, priority_sep: str = "|"
) -> pd.DataFrame:
    """Read a zip file and return a DataFrame"""

    try:
        return _extract_df(
            request_content=request_content, file_name=file_name, separator=priority_sep
        )

    except UnicodeDecodeError:
        return _extract_df(
            request_content=request_content, file_name=file_name, separator=","
        )
    except pd.errors.ParserError:
        return _extract_df(
            request_content=request_content, file_name=file_name, separator=","
        )


def extract_file_link_single(url: str) -> str:
    """Parse the OECD website to find the download link"""
    try:
        response = requests.get(url, verify=True).text
    except requests.exceptions.SSLError:
        response = get_url_selenium(url).page_source

    # Get page content
    soup = BeautifulSoup(response, "html.parser")
    link = list(soup.find_all("a"))[0].attrs["onclick"][15:-3].replace("_", "-")
    return f"https://stats.oecd.org/FileView2.aspx?IDFile={link}"


def _fetch_page_multiple_links(url: str) -> bs4.ResultSet:
    """
    Fetches multiple links from a specified URL.

    Args:
        url: The URL to fetch the links from.

    Returns:
        A ResultSet containing the links found on the page.
    """

    try:
        response = requests.get(url, verify=True).text
    except requests.exceptions.SSLError:
        response = get_url_selenium(url).page_source

    soup = BeautifulSoup(response, "html.parser")
    return soup.find_all("a")


def _links_dict(results_set: bs4.ResultSet) -> dict[int, str]:
    """Creates a dictionary of links and their corresponding years.

    Args:
        results_set: The ResultSet containing the links to be processed.

    Returns:
        A dictionary mapping each link's year to its URL.
    """
    # empty dictionary to hold files
    links_dict = {}

    # build links
    for _ in results_set:
        link = _.attrs["onclick"][15:-3].replace("_", "-")
        url = f"https://stats.oecd.org/FileView2.aspx?IDFile={link}"
        try:
            year = int(_.text[4:8])
        except ValueError:
            continue
        links_dict[year] = url

    return links_dict


def extract_file_link_multiple(url: str) -> dict[int, str]:
    """Parse the OECD website to find the download links for all years"""
    results_set = _fetch_page_multiple_links(url)
    return _links_dict(results_set)


def download_single_table(
    bulk_url: str,
    raw_file_name: str,
    output_file_name: str,
    save_path: pathlib.Path,
    config_file_path: pathlib.Path,
    small_version: bool = False,
) -> None:
    """Download a single table from the DAC bulk download website and save it as a feather file

    Args:
        bulk_url: The URL of the DAC bulk download page from which to download the table.
        raw_file_name: The name of the file to be downloaded.
        output_file_name: The name to use when saving the processed file.
        save_path: The path to save the processed file to.
        config_file_path: The path to the configuration file to use when processing the file.
        small_version: Whether to save a smaller version of the file.
    """

    # Get the file download link from website
    file_url = extract_file_link_single(bulk_url)

    # download the zip file from the website
    file_content = get_zip(file_url)

    # Load the file into a DataFrame
    df = read_zip_content(request_content=file_content, file_name=raw_file_name)

    # settings
    settings = read_settings(config_file_path)

    # Clean the DataFrame
    df = clean_raw_df(df, settings, small_version)

    # save the file
    df.to_feather(save_path / f"{output_file_name}.feather")

    # log a message confirming the operation
    logger.info(f"{raw_file_name} data downloaded and saved.")
