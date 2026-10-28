"""oda-data: named ODA indicators from the OECD DAC tables.

Names load on first use (PEP 562), so ``import oda_data`` imports none of
pandas, DuckDB, pyarrow, resolvekit, oda-reader, or pydeflate.
"""

import importlib
from importlib.metadata import version

__version__ = version("oda_data")

# Public name -> the private module that defines it.
_LAZY: dict[str, str] = {
    # engine
    "get": "oda_data._context",
    "query": "oda_data._context",
    "records": "oda_data._context",
    "Query": "oda_data._context",
    "Context": "oda_data._context",
    "Provenance": "oda_data._provenance",
    "Release": "oda_data._provenance",
    "provenance": "oda_data._provenance",
    # discovery
    "indicators": "oda_data._discover",
    "search": "oda_data._discover",
    "describe": "oda_data._discover",
    "IndicatorInfo": "oda_data._discover",
    "add_names": "oda_data._discover",
    # units
    "Unit": "oda_data._units",
    "current": "oda_data._units",
    "constant": "oda_data._units",
    # groups
    "Group": "oda_data._groups",
    "GroupTotal": "oda_data._groups",
    "SectorGroup": "oda_data._groups",
    "total": "oda_data._groups",
    "groups": "oda_data._groups",
    "sectors": "oda_data._groups",
    # errors and warnings
    "OdaDataError": "oda_data._errors",
    "UnknownIndicatorError": "oda_data._errors",
    "UnknownEntityError": "oda_data._errors",
    "AmbiguousEntityError": "oda_data._errors",
    "DimensionError": "oda_data._errors",
    "BasisRangeError": "oda_data._errors",
    "UnsupportedQueryError": "oda_data._errors",
    "MissingFactorError": "oda_data._errors",
    "DataUnavailableError": "oda_data._errors",
    "OfflineCacheMissError": "oda_data._errors",
    "ReleaseMismatchError": "oda_data._errors",
    "RemovedIn3Error": "oda_data._errors",
    "ImputationConservationError": "oda_data._errors",
    "UnmappedChannelError": "oda_data._errors",
    "OdaDataWarning": "oda_data._errors",
}

__all__ = [
    "AmbiguousEntityError",
    "BasisRangeError",
    "Context",
    "DataUnavailableError",
    "DimensionError",
    "Group",
    "GroupTotal",
    "ImputationConservationError",
    "IndicatorInfo",
    "MissingFactorError",
    "OdaDataError",
    "OdaDataWarning",
    "OfflineCacheMissError",
    "Provenance",
    "Query",
    "Release",
    "ReleaseMismatchError",
    "RemovedIn3Error",
    "SectorGroup",
    "Unit",
    "UnknownEntityError",
    "UnknownIndicatorError",
    "UnmappedChannelError",
    "UnsupportedQueryError",
    "__version__",
    "add_names",
    "constant",
    "current",
    "describe",
    "get",
    "groups",
    "indicators",
    "provenance",
    "query",
    "records",
    "search",
    "sectors",
    "total",
]


def __getattr__(name: str) -> object:
    if name not in _LAZY:
        raise AttributeError(f"module 'oda_data' has no attribute {name!r}")
    obj = getattr(importlib.import_module(_LAZY[name]), name)
    globals()[name] = obj
    return obj


def __dir__() -> list[str]:
    return sorted(__all__)
