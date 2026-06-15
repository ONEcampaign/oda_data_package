"""Implementation of the oda_data.cache public API.

Functions here aggregate oda_data's own bulk/query caches and oda_reader's
http/raw caches into a single Scope-keyed view.
"""

import contextlib
import time
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar

from oda_data.cache.config import oda_data_cache_root
from oda_data.cache.types import CacheRecord, Scope, _validate_scope
from oda_data.logger import logger

if TYPE_CHECKING:
    from oda_data.api.sources import Source

# Scopes that belong to oda_data's own disk layout.
_ODA_DATA_SCOPES: tuple[str, ...] = ("bulk", "query")
# Leaf sub-directory names inside cache_root / "oda-data".
_SCOPE_SUBDIR: dict[str, str] = {
    "bulk": "bulk_cache",
    "query": "query_cache",
}
# All concrete scopes (never "all") — immutable constant, not rebuilt on each call.
_ALL_SCOPES: tuple[Scope, ...] = ("bulk", "query", "http", "raw")

# ── Per-scope enable/disable flags ──────────────────────────────────────────
_SCOPE_ENABLED: dict[str, bool] = {
    "bulk": True,
    "query": True,
    "http": True,
    "raw": True,
}

# ── 5 GB stale-cache warning gate ───────────────────────────────────────────
_WARNED_STALE_5GB: bool = False
_5GB = 5 * 2**30  # bytes


def _oda_data_cache_dir() -> Path:
    """Return the oda-data sub-tree of the resolved cache root."""
    return oda_data_cache_root() / "oda-data"


def _scope_dir(scope: str) -> Path:
    """Return the directory for an oda_data-owned scope."""
    subdir = _SCOPE_SUBDIR[scope]
    return _oda_data_cache_dir() / subdir


def _oda_reader_cache_dir() -> Path:
    """Return the oda-reader sub-tree of the resolved cache root."""
    return oda_data_cache_root() / "oda-reader"


# Maps logical scope names to the actual subdirectory names oda_reader uses on
# disk.  oda_reader._cache.config uses "http_cache" and "bulk_files"; these
# must match so cache.clear("raw") actually removes the right files.
_ODA_READER_SUBDIR: dict[str, str] = {
    "http": "http_cache",
    "raw": "bulk_files",
}

# oda_reader sub-directories that are NOT backed by a public Scope literal but
# are still wiped by ``clear("all")``. Today only ``dataframes`` (oda_reader's
# processed-DataFrame cache); future additions go here so the "all" sweep
# stays exhaustive.
_ODA_READER_EXTRA_SUBDIRS: tuple[str, ...] = ("dataframes",)


def _dir_size(directory: Path) -> int:
    """Return the total byte size of all files under *directory* (recursive)."""
    if not directory.is_dir():
        return 0
    total = 0
    for f in directory.rglob("*"):
        if f.is_file():
            with contextlib.suppress(OSError):
                total += f.stat().st_size
    return total


# ── Public API ───────────────────────────────────────────────────────────────


def path(scope: Scope = "all") -> Path:
    """Return the cache root, or the directory for a specific scope.

    Note: ``'all'`` returns the cache root (containing ``oda-data/`` and
    ``oda-reader/`` sub-trees); per-scope values return their leaf directory.

    Args:
        scope: One of {"all", "bulk", "query", "http", "raw"}. "all" returns
            the unified cache root.

    Returns:
        Absolute path to the requested directory.

    Raises:
        ValueError: If ``scope`` is not one of the Scope literals.
    """
    _validate_scope(scope)
    if scope == "all":
        return oda_data_cache_root()
    if scope in _ODA_DATA_SCOPES:
        return _scope_dir(scope)
    # http / raw live under the oda-reader sub-tree with different leaf names.
    return _oda_reader_cache_dir() / _ODA_READER_SUBDIR[scope]


def entries() -> dict[Scope, list[CacheRecord]]:
    """Return all cache records grouped by scope.

    Returns:
        Mapping from each scope (excluding "all") to its CacheRecord list.
        Empty list for scopes with no files.
    """
    root = oda_data_cache_root()
    oda_data_dir = root / "oda-data"
    reader_dir = root / "oda-reader"
    now = time.time()
    result: dict[Scope, list[CacheRecord]] = {}

    for scope_name in _ALL_SCOPES:
        if scope_name in _ODA_DATA_SCOPES:
            directory = oda_data_dir / _SCOPE_SUBDIR[scope_name]
        else:
            directory = reader_dir / _ODA_READER_SUBDIR[scope_name]

        scope_records: list[CacheRecord] = []
        if directory.is_dir():
            for f in directory.iterdir():
                if not f.is_file():
                    continue
                try:
                    st = f.stat()
                    age_days = (now - st.st_mtime) / 86400.0
                    scope_records.append(
                        CacheRecord(
                            key=f.stem,
                            path=f,
                            size_bytes=st.st_size,
                            age_days=round(age_days, 3),
                            version="",
                            scope=scope_name,
                        )
                    )
                except OSError:
                    pass

        result[scope_name] = scope_records

    return result


def clear(scope: Scope = "all", *, blocking: bool = True) -> dict[Scope, int | None]:
    """Delete cached files in the named scope.

    Args:
        scope: Which scope to clear. "all" clears every scope.
        blocking: If True (default), acquire the FileLock and wait up to the
            lock timeout. If False, skip scopes under contention.

    Returns:
        Mapping from each scope to the count of files deleted, or ``None`` if
        the scope was skipped due to lock contention. ``0`` means the scope was
        reachable but empty.

        Note: The ``None`` return sentinel applies only to the ``"bulk"`` and
        ``"query"`` scopes, which ``oda_data`` writes under a ``FileLock``.
        For ``"http"`` and ``"raw"`` (owned by ``oda_reader``), the returned
        value is always an ``int`` regardless of ``blocking``: every reachable
        file is unlinked, but contention with a concurrent ``oda_reader``
        writer cannot currently be detected. If you need a strict "no clears
        during active writes" guarantee for those scopes, coordinate at the
        call-site level (e.g., quiesce in-flight downloads first).

    Raises:
        ValueError: If ``scope`` is not one of the Scope literals.
    """
    _validate_scope(scope)
    scopes_to_clear = _ALL_SCOPES if scope == "all" else [scope]
    result: dict[Scope, int | None] = {}

    for s in scopes_to_clear:
        count = _clear_one_scope(s, blocking=blocking)
        result[s] = count

    # clear("all") must leave nothing behind under the oda-reader sub-tree;
    # sweep extras (e.g. dataframes) and fold their count into "raw" so the
    # public return type stays unchanged.
    if scope == "all":
        extra = _clear_oda_reader_extra_subdirs()
        raw_count = result.get("raw")
        if extra and isinstance(raw_count, int):
            result["raw"] = raw_count + extra

    return result


def _clear_oda_reader_extra_subdirs() -> int:
    """Wipe oda_reader subdirs not backed by a public Scope (e.g. dataframes).

    Returns the number of files deleted. Best-effort: ``oda_reader`` does not
    expose lock paths for these subdirs.
    """
    base = _oda_reader_cache_dir()
    count = 0
    for subdir_name in _ODA_READER_EXTRA_SUBDIRS:
        directory = base / subdir_name
        if not directory.is_dir():
            continue
        for f in list(directory.rglob("*")):
            if f.is_file():
                try:
                    f.unlink()
                    count += 1
                except OSError as e:
                    logger.warning(f"Failed to delete {f}: {e}")
    return count


def _clear_one_scope(scope_name: str, *, blocking: bool) -> int | None:
    """Clear a single scope; returns file count or None on contention."""
    if scope_name in _ODA_DATA_SCOPES:
        return _clear_oda_data_scope(scope_name, blocking=blocking)
    return _clear_oda_reader_scope(scope_name, blocking=blocking)


def _clear_oda_data_scope(scope_name: str, *, blocking: bool) -> int | None:
    """Clear an oda_data-owned scope (bulk or query)."""
    directory = _scope_dir(scope_name)
    if not directory.is_dir():
        return 0

    from filelock import FileLock, Timeout

    lock_name = ".cache.lock" if scope_name == "bulk" else ".query.lock"
    lock_path = directory / lock_name
    lock_timeout = 1200 if blocking else 0

    try:
        with FileLock(str(lock_path), timeout=lock_timeout):
            count = 0
            for f in list(directory.iterdir()):
                if f.is_file() and f.suffix in (".parquet", ".json"):
                    try:
                        f.unlink()
                        count += 1
                    except OSError as e:
                        logger.warning(f"Failed to delete {f}: {e}")
            return count
    except Timeout:
        logger.debug(f"Skipping scope {scope_name!r}: lock held by another process.")
        return None


def _clear_oda_reader_scope(scope_name: str, *, blocking: bool) -> int | None:
    """Clear an oda_reader-owned scope (http or raw).

    ``oda_reader`` does not currently expose a public lock path for these
    scopes, so ``blocking`` is accepted for signature parity but cannot be
    honoured. Every reachable file is unlinked; the function always returns
    an ``int`` (never ``None``).
    """
    # TODO(jorge 2026-04-28): switch to FileLock once oda_reader publishes a
    # public lock-path getter for the http/raw caches.
    directory = _oda_reader_cache_dir() / _ODA_READER_SUBDIR[scope_name]
    if not directory.is_dir():
        return 0

    count = 0
    for f in list(directory.rglob("*")):
        if f.is_file():
            try:
                f.unlink()
                count += 1
            except OSError as e:
                logger.warning(f"Failed to delete {f}: {e}")
    return count


def size() -> dict[Scope, int]:
    """Return cache size per scope, in bytes.

    Side effect: if the total bytes across the current cache root **and** every
    sibling version-segmented subtree exceeds 5 GB, logs one WARNING per
    process.

    Returns:
        Mapping from each scope (excluding "all") to size in bytes.
    """
    global _WARNED_STALE_5GB

    # Resolve root once to avoid repeated calls across the loop + stale check.
    root = oda_data_cache_root()
    oda_data_dir = root / "oda-data"
    reader_dir = root / "oda-reader"

    result: dict[Scope, int] = {}
    for s in _ALL_SCOPES:
        if s in _ODA_DATA_SCOPES:
            d = oda_data_dir / _SCOPE_SUBDIR[s]
        else:
            d = reader_dir / _ODA_READER_SUBDIR[s]
        result[s] = _dir_size(d)

    if not _WARNED_STALE_5GB:
        total = _stale_total_bytes(root)
        if total > _5GB:
            _WARNED_STALE_5GB = True
            logger.warning(
                f"Total ODA cache size (including older-version caches at "
                f"{root.parent}) exceeds 5 GB ({total / 2**30:.1f} GB). "
                'Run oda_data.cache.clear(scope="raw") to free space, or delete '
                "older-version directories manually."
            )

    return result


def _stale_total_bytes(current: Path) -> int:
    """Sum bytes across *current* and all sibling-version subtrees.

    Args:
        current: The resolved cache root for this process.
    """
    parent = current.parent  # e.g. <platformdirs>/oda-data/
    if not parent.is_dir():
        return _dir_size(current)
    total = 0
    for child in parent.iterdir():
        if child.is_dir():
            total += _dir_size(child)
    return total


def invalidate(dataset: "type[Source] | str") -> None:
    """Invalidate cache entries for a single dataset (always blocking).

    Blocking semantics: acquires the bulk and query write locks (60-second
    timeout each) so the unlink sequence cannot race with concurrent writers.
    Callers that already hold either lock will deadlock against themselves or
    hit ``filelock.Timeout`` — do not call ``cache.invalidate`` from inside a
    ``BulkCacheManager`` or ``QueryCacheManager`` write context (e.g. a
    fetcher callback).

    On ``filelock.Timeout``, retry after the current writer completes;
    persistent timeouts indicate a stuck process and may require a manual
    ``cache.clear()``.

    Args:
        dataset: Either a Source subclass (e.g. ``CRSData``) or its class
            ``__name__`` as a string (e.g. ``"CRSData"``). The string must
            match the class name exactly — lowercase or partial names are
            rejected.

    Raises:
        ValueError: If ``dataset`` is a string that does not match any
            registered Source subclass ``__name__``.
        filelock.Timeout: If a write lock cannot be acquired within 60
            seconds.
    """
    from oda_data.api.sources import Source as _Source

    name = dataset if isinstance(dataset, str) else dataset.__name__

    # Build the registry from all known Source subclasses.
    known: dict[str, type[_Source]] = {
        cls.__name__: cls for cls in _all_source_subclasses(_Source)
    }

    if name not in known:
        valid = sorted(known)
        raise ValueError(
            f"Unknown dataset {name!r}. "
            f"Expected one of: {valid}. "
            f"Note: names are case-sensitive and must match the class __name__ exactly."
        )

    # Remove matching bulk and query entries.
    bulk_key = f"{name}_bulk"
    _invalidate_bulk_entry(bulk_key)
    _invalidate_query_entries(name)


_T = TypeVar("_T")


def _all_source_subclasses(base: type[_T]) -> list[type[_T]]:
    """Recursively collect all concrete subclasses of *base*."""
    result: list[type[_T]] = []
    for sub in base.__subclasses__():
        result.append(sub)
        result.extend(_all_source_subclasses(sub))
    return result


def _invalidate_bulk_entry(key: str) -> None:
    """Remove a single bulk parquet file and its manifest entry.

    Reuses ``Source._shared_bulk_cache`` when its base directory matches the
    current cache root, so callers that already hold the bulk lock on that
    singleton (e.g. a fetcher callback) do not deadlock against a fresh
    ``FileLock`` instance pointing at the same path. Falls back to a brand-new
    manager when no singleton has been initialised yet — cross-process
    coordination is preserved either way (same lock-file path).
    """
    from oda_data.api.sources import Source
    from oda_data.tools.cache import BulkCacheManager

    target_root = _oda_data_cache_dir()
    mgr = Source._shared_bulk_cache
    if mgr is None or mgr.base_dir.parent != target_root:
        mgr = BulkCacheManager(target_root)
    mgr.clear(key=key)
    orphan = _scope_dir("bulk") / f"{key}.parquet"
    orphan.unlink(missing_ok=True)


def _invalidate_query_entries(dataset_name: str) -> None:
    """Remove all query-cache files whose name starts with *dataset_name*.

    Acquires ``.query.lock`` (timeout 60 s) so the unlink loop cannot race
    with concurrent ``QueryCacheManager.save``. This realises the
    ``cache.invalidate`` "always blocking" contract.
    """
    from filelock import FileLock

    query_dir = _scope_dir("query")
    if not query_dir.is_dir():
        return
    lock_path = query_dir / ".query.lock"
    prefix = f"{dataset_name}-"
    # 60 s timeout is intentional: invalidate is a single-dataset operation,
    # not a full cache wipe. cache.clear's 1200 s timeout fits a slower
    # whole-cache sweep; 60 s here keeps invalidate from blocking a CLI
    # session indefinitely if a writer is wedged.
    with FileLock(str(lock_path), timeout=60):
        for f in list(query_dir.iterdir()):
            if f.is_file() and f.name.startswith(prefix):
                try:
                    f.unlink()
                except OSError as e:
                    logger.warning(f"Failed to delete query cache {f}: {e}")


def enable_cache(scope: Scope = "all") -> None:
    """Enable caching for the named scope.

    Args:
        scope: Which scope to enable. "all" enables every scope.

    Note:
        ``"http"`` and ``"raw"`` share a single underlying toggle in
        ``oda_reader``: enabling either enables both. ``oda_data`` mirrors
        the coupling in its own per-scope flags so ``is_scope_enabled``
        reports a consistent state on both sides.

    Raises:
        ValueError: If ``scope`` is not one of the Scope literals.
    """
    _validate_scope(scope)
    _set_scope_enabled(scope, enabled=True)


def disable_cache(scope: Scope = "all") -> None:
    """Disable caching for the named scope.

    Args:
        scope: Which scope to disable. "all" disables every scope.

    Note:
        ``"http"`` and ``"raw"`` share a single underlying toggle in
        ``oda_reader``: disabling either disables both. ``oda_data`` mirrors
        the coupling in its own per-scope flags so ``is_scope_enabled``
        reports a consistent state on both sides.

    Raises:
        ValueError: If ``scope`` is not one of the Scope literals.
    """
    _validate_scope(scope)
    _set_scope_enabled(scope, enabled=False)


def _set_scope_enabled(scope: str, *, enabled: bool) -> None:
    scopes: list[str] = list(_ALL_SCOPES) if scope == "all" else [scope]

    # http and raw share a single oda_reader-side toggle; mirror that in
    # _SCOPE_ENABLED so toggling one scope does not leave the other in a
    # state that contradicts the underlying truth on disk.
    if scope != "all" and ("http" in scopes or "raw" in scopes):
        scopes = list({*scopes, "http", "raw"})

    for s in scopes:
        _SCOPE_ENABLED[s] = enabled

    reader_scopes = [s for s in scopes if s in ("http", "raw")]
    if reader_scopes:
        try:
            import oda_reader

            if enabled:
                oda_reader.enable_cache()
            else:
                oda_reader.disable_cache()
        except Exception as e:
            logger.debug(
                f"Could not toggle oda_reader cache for scopes {reader_scopes!r}: {e}"
            )


def is_scope_enabled(scope: str) -> bool:
    """Return True if *scope* is currently enabled (internal helper)."""
    return _SCOPE_ENABLED.get(scope, True)
