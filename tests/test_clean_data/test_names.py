import pandas as pd

from oda_data.clean_data import names
import lxml.etree as et

from oda_data import set_data_path
from oda_data import config

set_data_path(config.OdaPATHS.test_files)


def test_add_name():
    test_df = pd.DataFrame(
        {
            "donor_code": [5, 12],
            "agency_code": [2, 1],
            "recipient_code": [400, 320],
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


def test_read_crs_codes():

    with open(config.OdaPATHS.test_files / "crs_codes_raw", "rb") as f:
        raw_data = f.read()

    data = et.XML(raw_data)

    result = names._extract_crs_elements(data)

    expected = names.read_crs_codes()

    assert set(result) == set(expected)
    assert result == expected
