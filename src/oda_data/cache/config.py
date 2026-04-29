"""Cache-root resolution for oda_data.

Priority: set_cache_root() override > ODA_DATA_CACHE_DIR env var >
platformdirs default (user_cache_dir("oda-data") / __version__).
"""

import os
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import platformdirs

from oda_data.logger import logger

try:
    _PACKAGE_VERSION = _pkg_version("oda_data")
except PackageNotFoundError:
    _PACKAGE_VERSION = "0.0.0"

_cache_root_override: Path | None = None
_LAST_CHAINED_ROOT: Path | None = None
_AUTO_MIGRATED: bool = False


def _resolve_cache_root() -> Path:
    """Return the effective cache root without side effects."""
    if _cache_root_override is not None:
        return _cache_root_override
    env = os.environ.get("ODA_DATA_CACHE_DIR")
    if env:
        return Path(env).resolve()
    return Path(platformdirs.user_cache_dir("oda-data")) / _PACKAGE_VERSION


def _chain_oda_reader_cache(root: Path) -> None:
    """Forward the resolved cache root to oda_reader — once per unique root.

    Short-circuits when the root equals the last-chained value so repeated
    calls to oda_data_cache_root() are cheap.
    """
    global _LAST_CHAINED_ROOT

    if root == _LAST_CHAINED_ROOT:
        return

    import oda_reader

    oda_reader.set_cache_dir(root / "oda-reader")
    _LAST_CHAINED_ROOT = root


def _auto_migrate_once() -> None:
    """Run auto-migration on first call per process.

    Imports ``migrate`` lazily to avoid a circular-import at module load time
    (``migrate.py`` imports ``oda_data_cache_root`` from this module).

    Exception policy:
      - ``RuntimeError`` propagates (intentional downgrade-detection failure
        from ``_check_downgrade_breadcrumb``).
      - Any other exception is logged at WARNING and swallowed; auto-migration
        will not retry this session, but the user's first cache access still
        succeeds.
    """
    global _AUTO_MIGRATED

    if _AUTO_MIGRATED:
        return
    _AUTO_MIGRATED = True

    from oda_data.cache._migrate import migrate as _do_migrate

    try:
        _do_migrate(force=False)
    except RuntimeError:
        # Downgrade-detection — propagate intentionally (loud failure is the design).
        raise
    except Exception as exc:
        # Transient migration failure (filesystem error, etc.) must not break
        # first-touch cache access. Log and continue without migration.
        logger.warning(
            f"Cache auto-migration failed; continuing without migration: {exc}"
        )


def oda_data_cache_root() -> Path:
    """Resolve the cache root.

    Priority: set_cache_root() override > ODA_DATA_CACHE_DIR env var >
    platformdirs default.

    Side effects (cheap, idempotent):
    - Chains the resolved root to oda_reader via _chain_oda_reader_cache().
    - Runs auto-migration on the first call per process via _auto_migrate_once().

    Returns:
        Absolute path to the cache root.
    """
    root = _resolve_cache_root()
    _chain_oda_reader_cache(root)
    _auto_migrate_once()
    return root


def set_cache_root(path: str | Path) -> None:
    """Set a custom cache root, taking precedence over env var and default.

    Args:
        path: Directory to use as the cache root. Created on first use.
    """
    global _cache_root_override

    _cache_root_override = Path(path).resolve()
