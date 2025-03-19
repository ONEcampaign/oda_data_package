from importlib.metadata import version

__version__ = version("oda_data")

from oda_data import tools
from oda_data.api.main import Indicators
from oda_data.api.sources import Dac1Data, Dac2Data, MultiSystemData, CrsData
from oda_data.tools.compatibility import ODAData
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
    "Dac1Data",
    "Dac2Data",
    "MultiSystemData",
    "CrsData",
    "donor_groupings",
    "recipient_groupings",
    "add_sectors",
    "add_broad_sectors",
    "set_data_path",
    "ODAData"
]
