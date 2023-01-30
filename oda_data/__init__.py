__version__ = "0.4.0"

from oda_data.classes.oda_data import ODAData
from oda_data.get_data.crs import download_crs
from oda_data.get_data.dac1 import download_dac1
from oda_data.get_data.dac2a import download_dac2a
from oda_data.get_data.multisystem import download_multisystem
from pydeflate import set_pydeflate_path


def set_data_path(path):
    from pathlib import Path
    from oda_data.config import OdaPATHS

    """Set the path to the data folder."""
    global OdaPATHS

    OdaPATHS.raw_data = Path(path).resolve()
    set_pydeflate_path(OdaPATHS.raw_data)
