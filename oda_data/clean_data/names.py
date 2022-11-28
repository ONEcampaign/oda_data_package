import json
import xml.etree.ElementTree as et

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
    data = requests.get(url).content
    return et.XML(data)


def _extract_el_name(item: et.Element) -> str:
    # Fetch the name from the metadata
    if item.tag == "metadata":
        for name in item:
            if name.tag == "name":
                for element in name:
                    return element.text


def _check_active(code: et.Element) -> bool:
    if code.tag == "codelist-item":
        try:
            if code.attrib["status"] != "active":
                return False
        except KeyError:
            pass


def _extract_name(code: et.Element) -> str:
    if code.tag == "name":
        for narrative in code:
            if narrative.tag == "narrative" and len(narrative.attrib) < 1:
                return narrative.text


def _extract_description(code: et.Element) -> str:
    if code.tag == "description":
        for desc in code:
            if desc.tag == "narrative" and len(desc.attrib) < 1:
                return desc.text


def _extract_crs_codes(data: et.Element) -> dict:
    """Download the CRS codes from the OECD website"""

    # Create empty variables to hold the data
    codes_db = {}
    name = ""
    code_n = ""

    # find all top-level code lists
    for code_group in data.findall(".//codelist"):
        # unpack each of the lists
        for code in code_group:
            # Fetch the name from the metadata
            if (_c := _extract_el_name(code)) is not None:
                name = _c
                codes_db[name] = {}
            # Loop through all the codes in the list and add them to the dictionary
            for code_item in code:
                # Check if the code is active
                if _check_active(code_item) is False:
                    continue
                # Fetch the code, name, and description
                for code_id in code_item:
                    if code_id.tag == "code":
                        code_n = code_id.text
                        codes_db[name][code_n] = {}
                    if (_n := _extract_name(code_id)) is not None:
                        codes_db[name][code_n]["name"] = _n
                    if (_d := _extract_description(code_id)) is not None:
                        codes_db[name][code_n]["description"] = _d

    return codes_db


def download_crs_codes() -> None:
    """Download the CRS codes from the OECD website"""

    data = _fetch_codes_xml()
    codes_db = _extract_crs_codes(data)

    with open(config.OdaPATHS.raw_data / "crs_codes.json", "w") as f:
        json.dump(codes_db, f, indent=4)


def read_crs_codes() -> dict:
    """Read the CRS codes from the saved json file. If file not available, download it."""

    if not (config.OdaPATHS.raw_data / "crs_codes.json").exists():
        download_crs_codes()

    with open(config.OdaPATHS.raw_data / "crs_codes.json", "r") as f:
        return json.load(f)
