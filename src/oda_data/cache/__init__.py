"""oda_data.cache — typed cache management surface.

Public symbols re-exported from sub-modules:

- ``path``, ``entries``, ``clear``, ``size``, ``invalidate``, ``release_info``,
  ``enable_cache``, ``disable_cache`` — from :mod:`oda_data.cache.api`
- ``migrate`` — from :mod:`oda_data.cache._migrate`
- ``set_cache_root`` — from :mod:`oda_data.cache.config`
- ``CacheRecord``, ``MigrationResult``, ``ReleaseInfo``, ``Scope`` — from
  :mod:`oda_data.cache.types`
"""

from oda_data.cache._migrate import migrate
from oda_data.cache.api import (
    clear,
    disable_cache,
    enable_cache,
    entries,
    invalidate,
    path,
    release_info,
    size,
)
from oda_data.cache.config import set_cache_root
from oda_data.cache.types import CacheRecord, MigrationResult, ReleaseInfo, Scope

__all__ = [
    "CacheRecord",
    "MigrationResult",
    "ReleaseInfo",
    "Scope",
    "clear",
    "disable_cache",
    "enable_cache",
    "entries",
    "invalidate",
    "migrate",
    "path",
    "release_info",
    "set_cache_root",
    "size",
]
