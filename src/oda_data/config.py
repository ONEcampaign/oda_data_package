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


class ODAPaths:
    """Class to store the paths to the data and output folders."""

    project = Path(__file__).resolve().parent.parent
    scripts = project / "oda_data"
    raw_data = (Path.cwd() / ".raw_data").resolve()
    pydeflate = raw_data / ".pydeflate"
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
