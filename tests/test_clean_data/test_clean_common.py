import pandas as pd


from oda_data import config
from oda_data.clean_data import common
from oda_data.logger import logger


def test_clean_column_name():
    text_upper = "DONOR"
    expected_upper = "donor_code"

    text_camel = "DonoRName"
    expected_camel = "dono_rname"

    text_snake = "donor_Name"
    expected_snake = "donor_name"

    assert common.clean_column_name(text_upper) == expected_upper
    assert common.clean_column_name(text_camel) == expected_camel
    assert common.clean_column_name(text_snake) == expected_snake


def test_clean_raw_df():
    df = pd.read_feather(config.OdaPATHS.test_files / "crs_2019_raw.feather")

    logger.setLevel("CRITICAL")
    result = common.clean_raw_df(df)

    assert isinstance(result, pd.DataFrame)
