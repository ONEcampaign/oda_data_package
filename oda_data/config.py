from pathlib import Path


class OdaPATHS:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_data"
    raw_data = scripts / ".raw_data"
    pydeflate = raw_data / ".pydeflate"
    indicators = scripts / "indicators"
    cleaning_config = scripts / "clean_data"
    sectors = indicators / "sectors"


# ------------------------------ Key URLs ------------------------------ #

BASE_URL: str = "https://stats.oecd.org/DownloadFiles.aspx?DatasetCode="

CRS_URL: str = f"{BASE_URL}CRS1"
TABLE1_URL: str = f"{BASE_URL}TABLE1"
TABLE2A_URL: str = f"{BASE_URL}TABLE2A"
MULTISYSTEM_URL: str = f"{BASE_URL}MULTISYSTEM"
