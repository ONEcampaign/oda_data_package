import os

import pandas as pd

from oda_data.get_data import crs
from oda_data.config import OdaPATHS as path
from oda_data.logger import logger


def test__download():
    df = pd.read_feather(path.test_files / "crs_2019_raw.feather")

    logger.setLevel("CRITICAL")

    new_path = path.test_files / "crs_2019_raw.feather"
    before = os.path.getmtime(new_path)

    crs._save(
        df,
        config_file_path=path.settings / "crs_config.json",
        save_path=path.test_files,
        year=2019,
        small_version=True,
    )

    after = os.path.getmtime(new_path)

    assert after > before


def test__years():
    dictionary = {2020: "value", 2021: "value"}

    logger.setLevel("CRITICAL")

    test_list = [2022, 2021, 2020]
    expected = [2021, 2020]

    assert crs._years(test_list, dictionary) == expected
