import pandas as pd

from oda_data.clean_data.names import add_name


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

    result = add_name(test_df, "donor_code")

    assert all(
        result.donor_name.values
        == [
            "Germany",
            "United Kingdom",
        ]
    )

    result = add_name(test_df, ["donor_code", "agency_code"])

    assert all(
        result.agency_name.values
        == [
            "Kreditanstalt für Wiederaufbau",
            "Department for International Development",
        ]
    )
