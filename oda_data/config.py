from pathlib import Path


class OdaPATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_data"
    raw_data = scripts / ".raw_data"
    pydeflate = raw_data / ".pydeflate"
    indicators = scripts / "indicators"
    cleaning = scripts / "clean_data"
    settings = scripts / "settings"
    sectors = indicators / "sectors"
    tests = project / "tests"
    test_files = tests / "files"


# ------------------------------ Key URLs ------------------------------ #

BASE_URL: str = "https://stats.oecd.org/DownloadFiles.aspx?DatasetCode="

CRS_URL: str = f"{BASE_URL}CRS1"

FULL_CRS_URL = f"https://stats.oecd.org/wbos/fileview2.aspx?IDFile=50f0355e-8f61-4230-85f3-90b4db45bfc9"
TABLE1_URL: str = f"{BASE_URL}TABLE1"
TABLE2A_URL: str = f"{BASE_URL}TABLE2A"
MULTISYSTEM_URL: str = (
    f"https://stats.oecd.org/wbos/fileview2.aspx?IDFile=02cda88e-7c37-4504-9343-eb2af821991a"
)

CODES_URL: str = "https://webfs.oecd.org/crs-iati-xml/Lookup/DAC-CRS-CODES.xml"
