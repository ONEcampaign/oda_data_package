from pathlib import Path


class ODAPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_data"
    raw_data = scripts / ".raw_data"
    pydeflate = raw_data / ".pydeflate"
    indicators = scripts / "indicators"
    names = scripts / "tools" / "names"
    cleaning = scripts / "clean_data"
    settings = scripts / "settings"
    sectors = indicators / "sectors"
    tests = project / "tests"
    test_files = tests / "files"
