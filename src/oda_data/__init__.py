from __future__ import annotations

import warnings
from pathlib import Path

from oda_data import cache
from oda_data.api.oecd import OECDClient
from oda_data.api.sources import (
    AidDataData,
    CRSData,
    DAC1Data,
    DAC2AData,
    MultiSystemData,
)
from oda_data.cache.config import set_cache_root
from oda_data.indicators.research import sector_imputations
from oda_data.indicators.research.policy_markers import bilateral_policy_marker
from oda_data.tools.compatibility import ODAData
from oda_data.tools.gni import add_gni_share_column
from oda_data.tools.groupings import provider_groupings, recipient_groupings
from oda_data.tools.names.add import add_names_columns
from oda_data.tools.sector_lists import add_broad_sectors, add_sectors

_DEPRECATION_WARNED_SET_DATA_PATH = False


def clear_cache() -> None:
    """Back-compat alias for oda_data.cache.clear('all').

    Clears all scopes (bulk, query, http, raw). Prefer ``oda_data.cache.clear()``.
    """
    from oda_data.cache import clear

    clear("all")


def disable_cache() -> None:
    """Back-compat alias for oda_data.cache.disable_cache('all').

    Disables all cache scopes. Prefer ``oda_data.cache.disable_cache()``.
    """
    from oda_data.cache import disable_cache as _disable

    _disable("all")


def enable_cache() -> None:
    """Back-compat alias for oda_data.cache.enable_cache('all').

    Enables all cache scopes. Prefer ``oda_data.cache.enable_cache()``.
    """
    from oda_data.cache import enable_cache as _enable

    _enable("all")


def set_data_path(path: str | Path) -> None:
    """Set the path for data outputs (parquet exports).

    Deprecated: cache location is now controlled via set_cache_root() or
    the ODA_DATA_CACHE_DIR env var. A one-time DeprecationWarning is emitted
    on first call. Shim preserved through 2.x, removed in 3.0.
    """
    global _DEPRECATION_WARNED_SET_DATA_PATH

    from pydeflate import set_pydeflate_path

    from oda_data.config import ODAPaths

    if not _DEPRECATION_WARNED_SET_DATA_PATH:
        warnings.warn(
            "oda_data.set_data_path() now governs only data outputs (parquet exports),"
            " not the cache. If you previously called set_data_path() to control where"
            " downloads are cached, also call"
            " oda_data.set_cache_root('<your_path>') or set the ODA_DATA_CACHE_DIR env"
            " var. The set_data_path() shim itself is preserved through oda_data 2.x"
            " and removed in 3.0.",
            DeprecationWarning,
            stacklevel=2,
        )
        _DEPRECATION_WARNED_SET_DATA_PATH = True

    ODAPaths.raw_data = Path(path).resolve()
    # Create on explicit user request; avoids import-time side effects.
    ODAPaths.raw_data.mkdir(parents=True, exist_ok=True)
    ODAPaths.pydeflate = ODAPaths.raw_data / ".pydeflate"
    set_pydeflate_path(ODAPaths.raw_data)
    # Do NOT call set_cache_dir here — cache chaining is handled lazily by
    # oda_data_cache_root() inside cache/config.py.


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
    "cache",
    "clear_cache",
    "disable_cache",
    "enable_cache",
    "provider_groupings",
    "recipient_groupings",
    "sector_imputations",
    "set_cache_root",
    "set_data_path",
]
