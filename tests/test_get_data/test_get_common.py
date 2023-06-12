import io
import zipfile

import pandas as pd
import pytest
import requests
from bs4 import BeautifulSoup

from oda_data import config
from oda_data.get_data import common


def test__checktype():
    expected_int = [1, 2, 3]
    expected_float = [1.0, 2.0, 3.0]

    tests = [[1, 2, 3], ["1", "2", "3"], [1.0, 2.0, 3.0]]

    for test in tests:
        assert common._checktype(test, int) == expected_int
        assert common._checktype(test, float) == expected_float

    assert common._checktype("1", int) == [1]
    assert common._checktype(1, int) == [1]
    assert common._checktype(1.0, int) == [1]
    with pytest.raises(ValueError):
        common._checktype((1, 2, 3), int)


def test_check_integers():
    tests = [[1, 2, 3], ["1", "2", "3"], [1.0, 2.0, 3.0]]
    for test in tests:
        assert common.check_integers(test) == [1, 2, 3]

    assert common.check_integers(range(2020, 2022)) == [2020, 2021]


def test_get_zip():
    """This function is essentially just a wrapper around requests.get"""
    # TODO: work on a test
    assert True


def test__serialise_content():
    with open(config.OdaPATHS.test_files / "zip_response", "rb") as f:
        response = f.read()

    mock_response = requests.Response()
    mock_response._content = response

    assert isinstance(common._serialise_content(mock_response), io.BytesIO)


def test__extract_zip():
    with open(config.OdaPATHS.test_files / "zip_response", "rb") as f:
        response = f.read()

    mock_response = requests.Response()
    mock_response._content = response

    serialised = common._serialise_content(mock_response)

    file = common._extract_zip(serialised, "Table1_Data.csv")

    assert isinstance(file, zipfile.ZipExtFile)


def test__raw2df():
    file_comma = config.OdaPATHS.test_files / "comma_df_raw.csv"
    file_tab = config.OdaPATHS.test_files / "tab_df_raw.csv"

    df_comma = common._raw2df(file_comma, sep=",", encoding="ISO-8859-1")

    with pytest.raises(pd.errors.ParserError):
        common._raw2df(file_tab, sep="|", encoding="ISO-8859-1")

    assert isinstance(df_comma, pd.DataFrame)
    assert len(df_comma.columns) > 3


def test_read_zip_content():
    with open(config.OdaPATHS.test_files / "zip_response", "rb") as f:
        response = f.read()

    mock_response = requests.Response()
    mock_response._content = response

    result = common.read_zip_content(
        request_content=mock_response, file_name="Table1_Data.csv", priority_sep=","
    )

    assert isinstance(result, pd.DataFrame)


def test__links_dict():
    with open(config.OdaPATHS.test_files / "response_multi", "r") as f:
        response = f.read()

    soup = BeautifulSoup(response, "html.parser")
    results_set = soup.find_all("a")

    links = common._links_dict(results_set)

    assert isinstance(links, dict)
    assert len(links) == 21
    assert links.get(2021) is not None
