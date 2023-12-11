__version__ = "1.0.9"

from oda_data.classes.oda_data import ODAData
from oda_data.get_data.crs import download_crs
from oda_data.get_data.dac1 import download_dac1
from oda_data.get_data.dac2a import download_dac2a
from oda_data.get_data.multisystem import download_multisystem
from oda_data.read_data.read import read_crs, read_dac1, read_dac2a, read_multisystem
from oda_data.tools.groupings import donor_groupings, recipient_groupings
from oda_data.tools.names import add_name
from oda_data import tools


def set_data_path(path):
    from pathlib import Path
    from oda_data.config import OdaPATHS
    from bblocks import set_bblocks_data_path
    from pydeflate import set_pydeflate_path

    """Set the path to the data folder."""
    global OdaPATHS

    OdaPATHS.raw_data = Path(path).resolve()
    set_pydeflate_path(OdaPATHS.raw_data)
    set_bblocks_data_path(OdaPATHS.raw_data)


__all__ = [
    "ODAData",
    "download_crs",
    "download_dac1",
    "download_dac2a",
    "download_multisystem",
    "read_crs",
    "read_dac1",
    "read_dac2a",
    "read_multisystem",
    "donor_groupings",
    "recipient_groupings",
    "add_name",
    "set_data_path",
    "tools",
]
