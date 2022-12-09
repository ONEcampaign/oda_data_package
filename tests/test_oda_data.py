from oda_data import __version__


def test_version():
    current_version = "0.1.11"
    assert __version__ == current_version
