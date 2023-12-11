from unittest.mock import patch

import lxml.etree as et
import pandas as pd

from oda_data import set_data_path, config
from oda_data.tools import names
from oda_data.tools.names import (
    read_crs_names,
    recipient_names,
)

set_data_path(config.OdaPATHS.test_files)


def test_add_name():
    test_df = pd.DataFrame(
        {
            "donor_code": [5, 12],
            "agency_code": [2, 1],
            "recipient_code": [189, 189],
            "purpose_code": [11110, 11120],
            "value": [2000, 3000],
        }
    )

    result = names.add_name(test_df, "donor_code")

    assert all(
        result.donor_name.values
        == [
            "Germany",
            "United Kingdom",
        ]
    )

    result = names.add_name(test_df, ["donor_code", "agency_code"])

    assert all(
        result.agency_name.values
        == [
            "Kreditanstalt für Wiederaufbau",
            "Department for International Development",
        ]
    )

    result = names.add_name(test_df, ["recipient_code"])

    assert result.recipient_name.unique()[0] == "North of Sahara, regional"

    result = names.add_name(test_df, ["purpose_code"])

    assert list(result.purpose_name.unique()) == [
        "Education policy and administrative management",
        "Education facilities and training",
    ]


def test_read_crs_codes():
    with open(config.OdaPATHS.test_files / "crs_codes_raw", "rb") as f:
        raw_data = f.read()

    data = et.XML(raw_data)

    result = names._extract_crs_elements(data)

    expected = names.read_crs_codes()


def test_read_crs_names():
    # Create a mock read_crs_codes function that returns a known dictionary
    def mock_read_crs_codes():
        return {
            "aid_type_code": {
                "A01": {
                    "name": "General budget",
                    "description": "General budget support",
                },
                "A02": {
                    "name": "Sector budget",
                    "description": "Sector budget support",
                },
            },
            "channel_code": {"11001": {"name": "Central Government"}},
        }

    # Patch the read_crs_codes function with the mock function
    with patch("oda_data.tools.names.read_crs_codes", mock_read_crs_codes):
        # Call the read_crs_names function
        crs_names = read_crs_names()

        # Verify that the function returns the expected dictionary
        assert crs_names == {
            "aid_type_code": {"A01": "General budget", "A02": "Sector budget"},
            "channel_code": {"11001": "Central Government"},
        }


def test_recipient_names():
    result = recipient_names()
    assert isinstance(result, dict)


def test_return_recipient_names():
    loc_ = 4
    col_ = "recipient_code"
    s_ = pd.DataFrame({"recipient_code": [163, 189], "value": [2000, 3000]})

    l, c, s = names._return_recipient_names(s_, col_, loc_)

    assert l == loc_
    assert c == "recipient_name"


def test_return_crs_names():
    loc_ = 4
    col_ = "purpose_code"
    s_ = pd.DataFrame({"purpose_code": [16001, 17001], "value": [2000, 3000]})

    l, c, s = names._return_crs_names(s_, col_, loc_)

    assert l == loc_
    assert c == "purpose_name"
