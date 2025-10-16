from oda_reader import clear_cache, disable_cache, enable_cache, set_cache_dir

from oda_data.api.oecd import OECDClient
from oda_data.api.sources import (
    AidDataData,
    CRSData,
    DAC1Data,
    DAC2AData,
    MultiSystemData,
)
from oda_data.indicators.research import sector_imputations
from oda_data.indicators.research.policy_markers import bilateral_policy_marker
from oda_data.tools.compatibility import ODAData
from oda_data.tools.gni import add_gni_share_column
from oda_data.tools.groupings import provider_groupings, recipient_groupings
from oda_data.tools.names.add import add_names_columns
from oda_data.tools.sector_lists import add_broad_sectors, add_sectors


def set_data_path(path):
    from pathlib import Path

    from pydeflate import set_pydeflate_path

    from oda_data.config import ODAPaths

    """Set the path to the data folder."""
    global ODAPaths

    ODAPaths.raw_data = Path(path).resolve()
    # Create on explicit user request; avoids import-time side effects
    ODAPaths.raw_data.mkdir(parents=True, exist_ok=True)
    ODAPaths.pydeflate = ODAPaths.raw_data / ".pydeflate"
    set_pydeflate_path(ODAPaths.raw_data)
    set_cache_dir(ODAPaths.raw_data)


__all__ = [
    "AidDataData",
    "CRSData",
    "DAC1Data",
    "DAC2AData",
    "MultiSystemData",
    "ODAData",
    "OECDClient",
    "add_broad_sectors",
    "add_gni_share_column",
    "add_names_columns",
    "add_sectors",
    "bilateral_policy_marker",
    "clear_cache",
    "disable_cache",
    "enable_cache",
    "provider_groupings",
    "recipient_groupings",
    "sector_imputations",
    "set_cache_dir",
    "set_data_path",
]
