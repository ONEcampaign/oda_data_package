from __future__ import annotations

from pathlib import Path


class _CacheRootDescriptor:
    """Descriptor that delegates to oda_data_cache_root() on every access.

    Resolves lazily at call time to avoid import-time side effects and to
    honour env-var / set_cache_root() overrides that may arrive after import.
    """

    def __get__(self, obj: object, objtype: type | None = None) -> Path:
        from oda_data.cache.config import oda_data_cache_root

        return oda_data_cache_root()


class _PydeflatePathDescriptor:
    """Descriptor resolving pydeflate's cache directory on every access.

    Anchored to the same version-segmented cache root as the rest of the
    package (``oda_data_cache_root()``), not to ``ODAPaths.raw_data`` —
    which is CWD-dependent and, since 2.6, documented as a data-outputs
    path rather than a cache path (see CACHING.md). This keeps pydeflate's
    cache CWD-independent regardless of where a caller's process starts.
    """

    def __get__(self, obj: object, objtype: type | None = None) -> Path:
        from oda_data.cache.config import oda_data_cache_root

        return oda_data_cache_root() / "pydeflate"


class ODAPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_data"
    raw_data = (Path.cwd() / ".raw_data").resolve()
    indicators = scripts / "indicators"
    names = scripts / "tools" / "names"
    cleaning = scripts / "clean_data"
    settings = scripts / "settings"
    sectors = indicators / "sectors"
    tests = project / "tests"
    test_files = tests / "files"

    # Lazy cache-root: delegates to cache/config.py on every access so that
    # env-var and set_cache_root() overrides are always honoured.
    cache_root: Path = _CacheRootDescriptor()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]

    # Lazy, version-segmented, CWD-independent pydeflate cache directory.
    pydeflate: Path = _PydeflatePathDescriptor()  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
