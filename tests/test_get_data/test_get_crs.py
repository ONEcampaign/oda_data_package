import zipfile

import pytest

from oda_data.get_data.crs import _download


def test_download():
    # Test valid input
    file_url = "https://example.com/crs/data.zip"
    year = 2020
    with pytest.raises(zipfile.BadZipFile):
        _download(file_url, year)
