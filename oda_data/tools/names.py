import json

import pandas as pd
import requests
from lxml import etree as et

from oda_data import config
from oda_data.clean_data.common import clean_column_name
from oda_data.get_data.common import get_url_selenium
from oda_data.logger import logger
from oda_data.read_data.read import read_crs
from oda_data.tools.groupings import donor_groupings, recipient_groupings


def _fetch_codes_xml(url: str = config.CODES_URL) -> et.Element:
    """Fetch the CRS codes from the OECD website"""
    try:
        requested_content = requests.get(url).content
    except requests.exceptions.SSLError:
        requested_content = get_url_selenium(url).page_source

    return et.XML(requested_content)


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

    clean_codes = {}
    clean_names = {
        "channelcode": "channel_code",
        "incomegroup": "income_group_code",
        "sector": "purpose_code",
        "sector_category": "sector_code",
        "flow_type": "flow_code",
        "finance_type": "finance_type_code",
        "aid_type": "aid_type_code",
    }
    with open(config.OdaPATHS.raw_data / "crs_codes.json", "r") as f:
        codes = json.load(f)

        for k, v in codes.items():
            clean_key = clean_column_name(k)
            if clean_key in clean_names:
                clean_key = clean_names[clean_key]
            clean_codes[clean_key] = v

    return clean_codes


def read_crs_names():
    codes = read_crs_codes()
    new_dict = {}
    for k, inner_d in codes.items():
        for code, v in inner_d.items():
            new_dict[k] = {**new_dict.get(k, {}), **{code: v["name"]}}

    return new_dict


def donor_names() -> dict:
    d = donor_groupings()
    return {**d["all_official"], **d["dac1_aggregates"]}


def _return_donor_names(df, col, loc) -> tuple:
    return loc, "donor_name", df[col].map(donor_names())


def recipient_names() -> dict:
    d = recipient_groupings()
    return {**d["all_recipients"], **d["dac2a_aggregates"]}


def _return_recipient_names(df, col, loc) -> tuple:
    return loc, "recipient_name", df[col].map(recipient_names())


def agency_names() -> pd.DataFrame:
    """Return a dictionary with the agency codes and their names"""

    return (
        read_crs(years=[2020])
        .filter(["donor_code", "agency_code", "agency_name"], axis=1)
        .drop_duplicates()
    )


def _return_agency_names(df, col, loc) -> tuple:
    """Merge the agency names to the dataframe"""
    agency = agency_names()

    series = (
        df.filter(["donor_code", col], axis=1)
        .astype("Int16")
        .merge(agency, how="left", on=["donor_code", col])["agency_name"]
    )

    return loc, "agency_name", series


def _return_crs_names(df, col, loc) -> tuple:
    series = df[col].astype(str).map(read_crs_names()[col])

    col = col.replace("_code", "")

    return loc, f"{col}_name", series


def add_name(df: pd.DataFrame, name_id: str | list | None = None) -> pd.DataFrame:
    """Add names to the dataframe.

    Args:
        df: a dataframe containing columns like donor_code or recipient_code
        name_id: the name of the column(s) to add the name to (e.g. donor_code). A list
            of columns can be passed. If none, it will look for columns containing
            `_code' in the name and add their respective name columns if available.
    """

    # if a single column is passed, convert to a list
    if isinstance(name_id, str):
        name_id = [name_id]

    # Get the code columns
    if name_id is None:
        name_id = [col for col in df.columns if "_code" in col]

    for col in name_id:
        if col not in df.columns:
            logger.warning(f"Column {col} not found in dataframe")

    # Get their index positions
    code_col_idx = {col: df.columns.get_loc(col) for col in name_id}

    names = []

    # Build the name columns
    for idx, col in enumerate(name_id):
        loc = code_col_idx[col] + idx
        if "donor" in col:
            names.append(_return_donor_names(df=df, col=col, loc=loc))
        elif "recipient" in col:
            names.append(_return_recipient_names(df=df, col=col, loc=loc))
        elif "agency" in col:
            names.append(_return_agency_names(df=df, col=col, loc=loc))
        elif col in read_crs_codes().keys():
            names.append(_return_crs_names(df=df, col=col, loc=loc))

    for new_col in names:
        if new_col[1] not in df.columns:
            df.insert(loc=new_col[0], column=new_col[1], value=new_col[2])

    return df
