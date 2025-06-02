from importlib.metadata import version

__version__ = version("oda_data")

from oda_data import tools
from oda_reader import set_cache_dir, disable_cache, enable_cache, clear_cache
from oda_data.api.oecd import OECDClient
from oda_data.api.sources import (
    DAC1Data,
    DAC2AData,
    MultiSystemData,
    CRSData,
    AidDataData,
)
from oda_data.indicators.research.policy_markers import bilateral_policy_marker
from oda_data.tools.compatibility import ODAData
from oda_data.tools.gni import add_gni_share_column
from oda_data.tools.groupings import provider_groupings, recipient_groupings
from oda_data.tools.names.add import add_names_columns
from oda_data.tools.sector_lists import add_sectors, add_broad_sectors


def set_data_path(path):
    from pathlib import Path
    from oda_data.config import ODAPaths
    from pydeflate import set_pydeflate_path

    """Set the path to the data folder."""
    global ODAPaths

    ODAPaths.raw_data = Path(path).resolve()
    set_pydeflate_path(ODAPaths.raw_data)
    set_cache_dir(ODAPaths.raw_data)


__all__ = [
    "OECDClient",
    "DAC1Data",
    "DAC2AData",
    "MultiSystemData",
    "CRSData",
    "AidDataData",
    "bilateral_policy_marker",
    "provider_groupings",
    "recipient_groupings",
    "add_sectors",
    "add_broad_sectors",
    "set_data_path",
    "ODAData",
    "add_names_columns",
    "add_gni_share_column",
    "set_cache_dir",
    "disable_cache",
    "enable_cache",
    "clear_cache",
]
