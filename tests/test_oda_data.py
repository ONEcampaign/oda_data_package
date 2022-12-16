from oda_data import __version__


def test_version():
    current_version = "0.2.1"
    assert __version__ == current_version
