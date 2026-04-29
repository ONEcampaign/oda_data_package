"""oda_data.cache — typed cache management surface.

Public symbols re-exported from sub-modules:

- ``path``, ``entries``, ``clear``, ``size``, ``invalidate``,
  ``enable_cache``, ``disable_cache`` — from :mod:`oda_data.cache.api`
- ``migrate`` — from :mod:`oda_data.cache._migrate`
- ``set_cache_root`` — from :mod:`oda_data.cache.config`
- ``CacheRecord``, ``MigrationResult``, ``Scope`` — from :mod:`oda_data.cache.types`
"""

from oda_data.cache._migrate import migrate
from oda_data.cache.api import (
    clear,
    disable_cache,
    enable_cache,
    entries,
    invalidate,
    path,
    size,
)
from oda_data.cache.config import set_cache_root
from oda_data.cache.types import CacheRecord, MigrationResult, Scope

__all__ = [
    "CacheRecord",
    "MigrationResult",
    "Scope",
    "clear",
    "disable_cache",
    "enable_cache",
    "entries",
    "invalidate",
    "migrate",
    "path",
    "set_cache_root",
    "size",
]
