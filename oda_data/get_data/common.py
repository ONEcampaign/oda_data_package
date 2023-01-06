import csv
import io
import pathlib
import zipfile as zf

import bs4
import pandas as pd
import requests
from bs4 import BeautifulSoup

from oda_data.clean_data.common import read_settings, clean_raw_df
from oda_data.logger import logger


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


def get_zip(url) -> requests.Response:
    """Download the zip file from the web"""

    try:
        return requests.get(url)
    except requests.exceptions.SSLError:
        return requests.get(url, verify=False)
    except ConnectionError:
        raise ConnectionError(f"Could not download file from {url}")


def _serialise_content(response: requests.Response) -> io.BytesIO:
    """Serialise the content of the response object"""
    return io.BytesIO(response.content)


def _extract_zip(serialised_content: io.BytesIO, file_name: str) -> csv:
    """Extract a file from a zip file. This creates a deep copy of the content"""
    return zf.ZipFile(serialised_content).open(file_name)


def _raw2df(csv_file: csv, sep: str) -> pd.DataFrame:
    """Convert a raw csv to a DataFrame. Check the result if requested"""

    _ = pd.read_csv(
        csv_file, sep=sep, dtype="str", encoding="ISO-8859-1", low_memory=False
    )

    if len(_.columns) < 3:
        raise pd.errors.ParserError
    return _


def read_zip_content(
    request_content, file_name: str, priority_sep: str = "|"
) -> pd.DataFrame:
    """Read a zip file and return a DataFrame"""
    import copy

    rc = copy.deepcopy(request_content)

    try:
        # serialise the content
        serialised_content = _serialise_content(rc)

        # extract the file
        raw_csv = _extract_zip(serialised_content, file_name)

        # convert to a dataframe
        return _raw2df(csv_file=raw_csv, sep=priority_sep)

    except UnicodeDecodeError:
        return read_zip_content(rc, file_name=file_name, priority_sep=",")
    except pd.errors.ParserError:
        return read_zip_content(rc, file_name=file_name, priority_sep=",")


def extract_file_link_single(url: str) -> str:
    """Parse the OECD website to find the download link"""
    try:
        response = requests.get(url, verify=True)
    except requests.exceptions.SSLError:
        response = requests.get(url, verify=False)
    soup = BeautifulSoup(response.text, "html.parser")
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
    # Get page data
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
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
        year = int(_.text[4:8])
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
