import json
from lxml import etree as et

import pandas as pd
import requests

from oda_data import ODAData, config


def add_name(df: pd.DataFrame, name_id: str | list) -> pd.DataFrame:
    """Add the donor name to the dataframe.

    Args:
        df: a dataframe containing columns like donor_code or recipient_code
        name_id: the name of the column(s) to add the name to (e.g. donor_code)
    """

    # if a single column is passed, convert to a list
    if isinstance(name_id, str):
        name_id = [name_id]

    # get the name of the column to add the name to
    ids = [f"{i.split('_')[0]}_name" for i in name_id] + name_id

    # fetch a dataframe with codes and names
    names = (
        ODAData(years=2020)
        .load_indicator("crs_bilateral_flow_disbursement_gross")
        .get_data()
        .drop_duplicates(subset=ids)
        .dropna(subset=name_id, how="any")
        .filter(ids, axis=1)
        .reset_index(drop=True)
    )

    # merge the names to the dataframe
    return df.merge(names, on=name_id, how="left")


def _fetch_codes_xml(url: str = config.CODES_URL) -> et.Element:
    return et.XML(requests.get(url).content)


def _extract_crs_elements(xml: et.Element) -> dict:

    # dictionary for data
    data = {}

    # Loop through all the codes in the list to extract the type, code, name, and description
    for code_list in xml:
        list_name_ = code_list.get("name")  # get the list name
        data[list_name_] = {}  # create an empty dictionary for that list

        # for every item in the list, extract the name and description (if not inactive)
        for item in code_list.iter("codelist-item"):

            # if inactive, skip
            if item.attrib.get("status") not in ["active", "voluntary basis", None]:
                continue

            # Store data inside the dictionary
            code_ = item.findall(".//code")[0].text
            name_ = item.findall(".//name/narrative")[0].text
            desc_ = item.findall(".//description/narrative")[0].text

            data[list_name_][code_] = {"name": name_, "description": desc_}

    return data


def download_crs_codes() -> None:
    """Download the CRS codes from the OECD website"""

    data = _fetch_codes_xml()
    codes_db = _extract_crs_elements(data)

    with open(config.OdaPATHS.raw_data / "crs_codes.json", "w") as f:
        json.dump(codes_db, f, indent=4)


def read_crs_codes() -> dict:
    """Read the CRS codes from the saved json file. If file not available, download it."""

    if not (config.OdaPATHS.raw_data / "crs_codes.json").exists():
        download_crs_codes()

    with open(config.OdaPATHS.raw_data / "crs_codes.json", "r") as f:
        return json.load(f)
