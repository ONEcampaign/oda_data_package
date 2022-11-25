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


def test_read_settings():
    file = common.read_settings(config.OdaPATHS.cleaning_config / "dac1_config.json")
    assert isinstance(file, dict)
    assert "donor_code" in file.keys()
    assert file["donor"]["keep"]


def test__validate_columns():
    file = common.read_settings(config.OdaPATHS.cleaning_config / "dac1_config.json")
    df = pd.read_feather(config.OdaPATHS.test_files / "table1_raw.feather")

    dtypes = {c: t["type"] for c, t in file.items()}
    dtypes["fake_column"] = "Int64"
    dtypes.pop("donor_code")

    logger.setLevel("CRITICAL")

    clean = common._validate_columns(df, dtypes)
    assert len(clean) == len(dtypes) - 1


def test_clean_raw_df():
    df = pd.read_feather(config.OdaPATHS.test_files / "crs_2019_raw.feather")
    settings = common.read_settings(config.OdaPATHS.cleaning_config / "crs_config.json")

    logger.setLevel("CRITICAL")
    result = common.clean_raw_df(df, settings, small_version=True)

    assert isinstance(result, pd.DataFrame)
