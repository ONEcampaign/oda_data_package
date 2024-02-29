from unittest.mock import patch

import lxml.etree as et
import pandas as pd

from oda_data import set_data_path, config
from oda_data.clean_data.schema import OdaSchema
from oda_data.tools import names
from oda_data.tools.names import (
    read_crs_names,
    recipient_names,
)

set_data_path(config.OdaPATHS.test_files)


def test_add_name():
    test_df = pd.DataFrame(
        {
            OdaSchema.PROVIDER_CODE: [5, 12],
            OdaSchema.AGENCY_CODE: [2, 10],
            OdaSchema.RECIPIENT_CODE: [189, 189],
            OdaSchema.PURPOSE_CODE: [11110, 11120],
            OdaSchema.VALUE: [2000, 3000],
        }
    )

    result = names.add_name(test_df, OdaSchema.PROVIDER_CODE)

    assert all(
        result[OdaSchema.PROVIDER_NAME].values
        == [
            "Germany",
            "United Kingdom",
        ]
    )

    result = names.add_name(test_df, [OdaSchema.PROVIDER_CODE, OdaSchema.AGENCY_CODE])

    assert all(
        result[OdaSchema.AGENCY_NAME].values
        == [
            "Kreditanstalt für Wiederaufbau",
            "Department of Health  and Social Care",
        ]
    )

    result = names.add_name(test_df, [OdaSchema.RECIPIENT_CODE])

    assert result[OdaSchema.RECIPIENT_NAME].unique()[0] == "North of Sahara, regional"

    result = names.add_name(test_df, [OdaSchema.PURPOSE_CODE])

    assert list(result[OdaSchema.PURPOSE_NAME].unique()) == [
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
