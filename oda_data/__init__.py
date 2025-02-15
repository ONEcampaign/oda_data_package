__version__ = "1.5.0"

from oda_data import tools
from oda_data.api.main import Indicators
from oda_data.tools.groupings import donor_groupings, recipient_groupings
from oda_data.tools.sector_lists import add_sectors, add_broad_sectors


def set_data_path(path):
    from pathlib import Path
    from oda_data.config import OdaPATHS
    from pydeflate import set_pydeflate_path

    """Set the path to the data folder."""
    global OdaPATHS

    OdaPATHS.raw_data = Path(path).resolve()
    set_pydeflate_path(OdaPATHS.raw_data)


__all__ = [
    "Indicators",
    # "download_crs",
    # "download_dac1",
    # "download_dac2a",
    # "download_multisystem",
    # "read_crs",
    # "read_dac1",
    # "read_dac2a",
    # "read_multisystem",
    "donor_groupings",
    "recipient_groupings",
    # "add_name",
    "add_sectors",
    "add_broad_sectors",
    "set_data_path",
    "tools",
]
