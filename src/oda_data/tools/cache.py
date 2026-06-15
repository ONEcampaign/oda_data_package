"""
Cache management for oda_data package.

This module provides a three-tier caching system:
1. ThreadSafeMemoryCache: Fast in-memory cache with thread safety
2. BulkCacheManager: Manages large bulk dataset parquet files
3. QueryCacheManager: Manages filtered query result parquet files

Design follows pydeflate patterns for robustness and thread safety.
"""

import contextlib
import hashlib
import json
import os
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import pandas as pd
from cachetools import TTLCache
from filelock import FileLock

from oda_data.logger import logger
from oda_data.tools.names.create_mapping import snake_to_pascal

# ISO format for timestamps
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

# Hostname cached at module load — used in tmp-file suffixes so NFS/multi-host
# writes don't collide on the same PID.
_HOSTNAME: str = socket.gethostname()

# Major version of the installed oda_data package, used in query-cache filenames
# to auto-invalidate on major upgrades.
try:
    _PACKAGE_MAJOR_VERSION: str = _pkg_version("oda_data").split(".")[0]
except PackageNotFoundError:
    _PACKAGE_MAJOR_VERSION = "0"

# Sweep threshold: tmp files older than this are removed on manager __init__.
_TMP_SWEEP_MAX_AGE_SECONDS: int = 86400  # 24 hours


def _tmp_path(path: Path, suffix: str = "") -> Path:
    """Return a sibling temp path tagged with hostname + pid (+ optional suffix).

    The hostname segment guards NFS / shared-volume scenarios where two hosts
    can share a PID; the suffix allows multiple concurrent temp files in the
    same atomic-write sequence (e.g. ``"retry"``, ``"nocache"``).
    """
    tail = f"-{suffix}" if suffix else ""
    return Path(f"{path}.tmp-{_HOSTNAME}-{os.getpid()}{tail}")


def _sweep_old_tmp_files(base_dir: Path) -> None:
    """Remove ``*.tmp-*`` files in *base_dir* older than 24 hours.

    Cleans up orphan temp files left by SIGKILL or other abnormal terminations
    on any host. Errors are swallowed — a stale temp file is not worth raising.
    """
    cutoff = time.time() - _TMP_SWEEP_MAX_AGE_SECONDS
    for f in base_dir.glob("*.tmp-*"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                logger.debug(f"Swept stale tmp file: {f}")
        except OSError:
            pass


def generate_param_hash(filters: list[tuple]) -> str:
    """Generates a reproducible hash from a list of tuples.

    Args:
        filters: List of (column, predicate, value) tuples

    Returns:
        First 10 characters of MD5 hash
    """
    # convert list of tuples to dictionary
    params = {k: v for k, _, v in filters}

    sorted_params = {
        k: sorted(v) if isinstance(v, list) else v for k, v in params.items()
    }
    json_str = json.dumps(sorted_params, sort_keys=True)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()[:10]


def generate_projection_hash(param_hash: str, columns: list[str]) -> str:
    """Generates a cache key for a projected (column-subset) read.

    Combines the filter-based param_hash with a hash of the requested columns,
    so that projected reads are cached separately from full reads without
    polluting the shared filter-only cache key.

    Args:
        param_hash: The filter-only hash from generate_param_hash
        columns: List of column names in the projection

    Returns:
        A combined hash string
    """
    col_str = json.dumps(sorted(columns), sort_keys=True)
    col_hash = hashlib.md5(col_str.encode("utf-8")).hexdigest()[:10]
    return f"{param_hash}_proj_{col_hash}"


class ThreadSafeMemoryCache:
    """Thread-safe wrapper around cachetools.TTLCache.

    Provides thread-safe access to an in-memory TTL cache using a reentrant lock.
    All operations are atomic and thread-safe.

    Args:
        maxsize: Maximum number of entries in cache
        ttl: Time-to-live in seconds for cache entries
    """

    def __init__(self, maxsize: int, ttl: int) -> None:
        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)
        self._lock = threading.RLock()  # Reentrant lock for nested calls

    def get(self, key: object, default: object = None) -> object:
        """Get item from cache (thread-safe)."""
        with self._lock:
            return self._cache.get(key, default)

    def __setitem__(self, key: object, value: object) -> None:
        """Set item in cache (thread-safe)."""
        with self._lock:
            self._cache[key] = value

    def __getitem__(self, key: object) -> object:
        """Get item from cache with KeyError on miss (thread-safe)."""
        with self._lock:
            return self._cache[key]

    def __contains__(self, key: object) -> bool:
        """Check if key exists in cache (thread-safe)."""
        with self._lock:
            return key in self._cache

    def clear(self) -> None:
        """Clear all entries from cache (thread-safe)."""
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        """Return number of items in cache (thread-safe)."""
        with self._lock:
            return len(self._cache)


@dataclass(frozen=True)
class BulkCacheEntry:
    """Immutable descriptor for a cacheable bulk dataset.

    Follows pydeflate CacheEntry pattern. The frozen dataclass ensures
    cache keys cannot be accidentally mutated.

    Args:
        key: Unique identifier for this dataset (e.g., "CRSData_bulk")
        fetcher: Callback that downloads and writes parquet to provided path
        ttl_days: Time-to-live in days before cache is considered stale
        version: Version string for cache invalidation (e.g., package version)
    """

    key: str
    fetcher: Callable[[Path], None]
    ttl_days: int = 30
    version: str | None = None


class BulkCacheManager:
    """Manages bulk dataset parquet files with manifest tracking.

    Uses FileLock for thread and process safety. Ensures only one thread/process
    downloads a dataset while others wait (prevents concurrent download waste).

    Storage structure:
        {base_dir}/bulk_cache/
        ├── manifest.json       # Metadata tracking
        ├── .cache.lock         # FileLock for coordination
        ├── CRSData_bulk.parquet
        ├── DAC1Data_bulk.parquet
        └── ...

    Args:
        base_dir: Root directory for cache storage
        ttl_seconds: Time-to-live in seconds (default: 30 days)
    """

    def __init__(self, base_dir: Path, ttl_seconds: int = 2592000) -> None:
        self.base_dir = Path(base_dir) / "bulk_cache"
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.manifest_path = self.base_dir / "manifest.json"
        self.lock_path = self.base_dir / ".cache.lock"
        self._lock = FileLock(self.lock_path, timeout=1200)  # 20 min timeout
        self.ttl_seconds = ttl_seconds

        _sweep_old_tmp_files(self.base_dir)

    def _get_path(self, entry: BulkCacheEntry) -> Path:
        """Get file path for a cache entry."""
        return self.base_dir / f"{entry.key}.parquet"

    def _load_manifest(self) -> dict:
        """Load manifest from disk."""
        if not self.manifest_path.exists():
            return {}
        try:
            with open(self.manifest_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to load manifest: {e}. Creating new manifest.")
            return {}

    def _save_manifest(self, manifest: dict) -> None:
        """Save manifest to disk (atomic write)."""
        tmp_path = _tmp_path(self.manifest_path)
        try:
            with open(tmp_path, "w") as f:
                json.dump(manifest, f, indent=2)
            tmp_path.replace(self.manifest_path)
        except Exception:
            # Only clean up temp file if replace failed
            tmp_path.unlink(missing_ok=True)
            raise

    def _is_stale(self, record: dict, entry: BulkCacheEntry) -> bool:
        """Check if a cached entry is stale."""
        # Check version mismatch
        if entry.version is not None and entry.version != record.get("version"):
            return True

        # Check TTL expiration
        try:
            downloaded = datetime.fromisoformat(record["downloaded_at"])
            age = datetime.now(UTC) - downloaded
            return age > timedelta(seconds=self.ttl_seconds)
        except (KeyError, ValueError):
            return True

    def ensure(self, entry: BulkCacheEntry, refresh: bool = False) -> Path:
        """Get cached bulk file or download if needed.

        This method blocks with FileLock — if another thread/process is
        downloading the same dataset, this call will wait for it to complete,
        preventing concurrent duplicate downloads.

        If the fetcher raises ``BulkPayloadCorruptError`` (e.g. a truncated zip
        on disk), the corrupt entry is cleared and the fetch is retried once.
        On a second failure the exception is re-raised with an actionable
        message naming the cache path and the recovery hint.

        If the ``bulk`` scope is disabled via ``oda_data.cache.disable_cache``,
        every call re-fetches: the FileLock still serializes concurrent callers
        and the parquet is written atomically to the canonical path, but the
        manifest is NOT updated — so any later read with bulk enabled sees no
        record and re-fetches as well, preserving the "no trusted cache"
        semantic.

        Args:
            entry: Cache entry descriptor
            refresh: If True, force re-download even if cached

        Returns:
            Path to cached parquet file
        """
        from oda_data.cache.api import is_scope_enabled

        with self._lock:
            bulk_path = self._ensure_inner(
                entry,
                refresh=refresh,
                skip_manifest=not is_scope_enabled("bulk"),
            )
        return bulk_path

    def _ensure_inner(
        self,
        entry: BulkCacheEntry,
        *,
        refresh: bool,
        skip_manifest: bool = False,
    ) -> Path:
        """Core fetch logic; assumes ``self._lock`` is already held.

        ``skip_manifest=True`` is the disabled-scope path: re-fetch every time,
        write the canonical file atomically, but leave the manifest alone so the
        next enabled read still sees no trusted record.
        """
        from oda_reader import BulkPayloadCorruptError

        path = self._get_path(entry)
        manifest = self._load_manifest()
        record = manifest.get(entry.key)

        # Check if we have a fresh cached version.
        if (
            not refresh
            and record
            and path.exists()
            and not self._is_stale(record, entry)
        ):
            logger.info(f"Using cached bulk data for {entry.key}")
            return path

        # Need to download — use atomic write pattern.
        logger.info(f"Downloading bulk data for {entry.key}")
        tmp_path = _tmp_path(path, "nocache" if skip_manifest else "")
        manifest_dirty = False
        try:
            try:
                entry.fetcher(tmp_path)
            except BulkPayloadCorruptError as exc:
                # First corruption: clear the stale entry and retry once.
                logger.warning(
                    f"Corrupt bulk payload for {entry.key}: {exc}. "
                    "Clearing cache and retrying once."
                )
                self._clear_locked(entry.key)
                manifest_dirty = True
                tmp_path.unlink(missing_ok=True)
                tmp_path2 = _tmp_path(
                    path, "nocache-retry" if skip_manifest else "retry"
                )
                try:
                    entry.fetcher(tmp_path2)
                    tmp_path2.replace(path)
                except Exception as exc2:
                    tmp_path2.unlink(missing_ok=True)
                    raise RuntimeError(
                        f"Bulk payload at {path} failed integrity validation after "
                        "retry. Run oda_data.cache.clear('raw') to wipe the raw "
                        "cache and try again."
                    ) from exc2
            else:
                tmp_path.replace(path)  # Atomic on POSIX systems.

            if skip_manifest:
                # Disabled-scope path: leave the manifest alone so the next
                # enabled read sees no trusted record and re-fetches.
                logger.info(
                    f"Bulk scope disabled — fetched {entry.key} without "
                    "updating manifest"
                )
                return path

            # Update manifest. If _clear_locked ran, our local copy is stale —
            # re-read so we don't resurrect cleared keys.
            if manifest_dirty:
                manifest = self._load_manifest()
            file_size = path.stat().st_size / (1024 * 1024)  # MB
            manifest[entry.key] = {
                "filename": path.name,
                "downloaded_at": datetime.now(UTC).isoformat(),
                "version": entry.version,
                "size_mb": round(file_size, 2),
            }
            self._save_manifest(manifest)

            logger.info(f"Cached bulk data for {entry.key} ({file_size:.1f} MB)")
            return path

        finally:
            tmp_path.unlink(missing_ok=True)

    def _clear_locked(self, key: str | None = None) -> None:
        """Remove cached entries — assumes ``self._lock`` is already held.

        Args:
            key: Specific entry to remove, or None to remove all.
        """
        manifest = self._load_manifest()

        if key is None:
            for record in manifest.values():
                filepath = self.base_dir / record["filename"]
                filepath.unlink(missing_ok=True)
            manifest.clear()
            logger.info("Cleared all bulk cache entries")
        elif key in manifest:
            filepath = self.base_dir / manifest[key]["filename"]
            filepath.unlink(missing_ok=True)
            del manifest[key]
            logger.info(f"Cleared bulk cache entry: {key}")

        self._save_manifest(manifest)

    def clear(self, key: str | None = None) -> None:
        """Remove cached entries.

        Args:
            key: Specific entry to remove, or None to remove all.
        """
        with self._lock:
            self._clear_locked(key)

    def stats(self) -> dict:
        """Get cache statistics.

        Returns:
            Dictionary with total_entries, total_size_mb, stale_entries
        """
        # No lock needed - read-only operation on manifest
        manifest = self._load_manifest()
        total_size = sum(r.get("size_mb", 0) for r in manifest.values())

        # Count stale entries (need entry to check, so we can't do it here perfectly)
        # Just count entries with expired timestamps
        stale_count = 0
        for record in manifest.values():
            try:
                downloaded = datetime.fromisoformat(record["downloaded_at"])
                age = datetime.now(UTC) - downloaded
                if age > timedelta(seconds=self.ttl_seconds):
                    stale_count += 1
            except (KeyError, ValueError):
                stale_count += 1

        return {
            "total_entries": len(manifest),
            "total_size_mb": round(total_size, 2),
            "stale_entries": stale_count,
        }

    def list_records(self) -> list[dict]:
        """List all cached entries with metadata.

        Returns:
            List of dicts with key, size_mb, age_days, is_stale
        """
        manifest = self._load_manifest()
        records = []

        for key, record in manifest.items():
            try:
                downloaded = datetime.fromisoformat(record["downloaded_at"])
                age = datetime.now(UTC) - downloaded
                age_days = age.total_seconds() / 86400

                records.append(
                    {
                        "key": key,
                        "size_mb": record.get("size_mb", 0),
                        "age_days": round(age_days, 1),
                        "is_stale": age > timedelta(seconds=self.ttl_seconds),
                        "version": record.get("version"),
                    }
                )
            except (KeyError, ValueError):
                records.append(
                    {
                        "key": key,
                        "size_mb": record.get("size_mb", 0),
                        "age_days": None,
                        "is_stale": True,
                        "version": record.get("version"),
                    }
                )

        return records


class QueryCacheManager:
    """Manages filtered query result parquet files.

    Unlike BulkCacheManager, this handles smaller filtered query results.
    Files are immutable after creation, so no lock needed for reads.

    Storage structure:
        {base_dir}/query_cache/
        ├── .query.lock                         # FileLock for writes
        ├── CRSData-abc123-v2.parquet
        ├── DAC1Data-def456-v2.parquet
        └── ...

    The ``-v{major}`` suffix in filenames auto-invalidates cached results when
    the oda_data major version changes.

    Args:
        base_dir: Root directory for cache storage
        ttl_seconds: Time-to-live in seconds (default: 1 day)
    """

    def __init__(self, base_dir: Path, ttl_seconds: int = 86400) -> None:
        self.base_dir = Path(base_dir) / "query_cache"
        self.base_dir.mkdir(parents=True, exist_ok=True)

        self.lock_path = self.base_dir / ".query.lock"
        self._lock = FileLock(self.lock_path, timeout=60)
        self.ttl_seconds = ttl_seconds

        _sweep_old_tmp_files(self.base_dir)

    def get_file_path(self, dataset_name: str, param_hash: str) -> Path:
        """Get file path for a cached query result.

        Filename pattern: ``{dataset_name}-{param_hash}-v{major}.parquet``.
        The ``-v{major}`` segment ensures cross-version isolation.
        """
        return (
            self.base_dir
            / f"{dataset_name}-{param_hash}-v{_PACKAGE_MAJOR_VERSION}.parquet"
        )

    def _is_expired(self, path: Path) -> bool:
        """Check if a cached file has expired based on mtime."""
        try:
            age = time.time() - path.stat().st_mtime
            return age > self.ttl_seconds
        except OSError:
            return True

    def load(
        self,
        dataset_name: str,
        param_hash: str,
        filters: list | None,
        columns: list | None,
    ) -> pd.DataFrame | None:
        """Load from query cache.

        No lock needed - files are immutable after write. If file doesn't exist
        or is expired, returns None.

        If the ``query`` scope is disabled via ``oda_data.cache.disable_cache``,
        always returns None (forces re-compute).

        Args:
            dataset_name: Name of the dataset class
            param_hash: Hash of query parameters
            filters: Optional parquet filters for predicate pushdown
            columns: Optional column subset to read

        Returns:
            DataFrame if cached and fresh, None otherwise
        """
        from oda_data.cache.api import is_scope_enabled

        if not is_scope_enabled("query"):
            return None

        path = self.get_file_path(dataset_name, param_hash)

        # Handle MultiSystemData special case (PascalCase columns)
        if dataset_name == "MultiSystemData":
            columns = [snake_to_pascal(col) for col in columns] if columns else columns
            filters = (
                [(snake_to_pascal(col), op, val) for col, op, val in filters]
                if filters
                else None
            )

        if not path.exists():
            logger.debug(f"Query cache miss: {dataset_name}-{param_hash}")
            return None

        if self._is_expired(path):
            logger.debug(f"Query cache expired: {dataset_name}-{param_hash}")
            # Expired - try to delete (non-blocking, another process might have deleted it)
            with contextlib.suppress(OSError):
                path.unlink()
            return None

        try:
            logger.debug(f"Query cache hit: {dataset_name}-{param_hash}")
            return pd.read_parquet(
                path, filters=filters, engine="pyarrow", columns=columns
            )
        except Exception as e:
            logger.warning(f"Failed to load query cache: {e}")
            # Corrupted file - try to delete
            with contextlib.suppress(OSError):
                path.unlink()
            return None

    def save(self, dataset_name: str, param_hash: str, df: pd.DataFrame) -> None:
        """Save DataFrame to query cache.

        Uses FileLock + atomic write (temp file then rename) to prevent
        corruption from concurrent writes.

        If the ``query`` scope is disabled via ``oda_data.cache.disable_cache``,
        this is a no-op.

        Args:
            dataset_name: Name of the dataset class
            param_hash: Hash of query parameters
            df: DataFrame to cache
        """
        from oda_data.cache.api import is_scope_enabled

        if not is_scope_enabled("query"):
            return

        with self._lock:
            path = self.get_file_path(dataset_name, param_hash)
            tmp_path = _tmp_path(path)
            try:
                df.to_parquet(tmp_path)
                tmp_path.replace(path)  # Atomic
            except Exception as e:
                logger.warning(f"Failed to save query cache: {e}")
            finally:
                tmp_path.unlink(missing_ok=True)

    def clear(self) -> None:
        """Remove all cached query results."""
        with self._lock:
            count = 0
            for file in self.base_dir.glob("*.parquet"):
                try:
                    file.unlink()
                    count += 1
                except OSError as e:
                    logger.warning(f"Failed to delete {file}: {e}")
            logger.info(f"Cleared {count} query cache entries")

    def cleanup_expired(self) -> None:
        """Remove expired cache files (no lock - best effort)."""
        count = 0
        for file in self.base_dir.glob("*.parquet"):
            if self._is_expired(file):
                try:
                    file.unlink()
                    count += 1
                except OSError:
                    pass  # Another process might have deleted it
        if count > 0:
            logger.info(f"Cleaned up {count} expired query cache entries")


# Backward compatibility - kept for external code that might import this
OnDiskCache = QueryCacheManager


# ============================================================================
# Bulk Download Fetcher Functions
# ============================================================================
# These functions handle downloading and processing parquet files from oda_reader


def create_crs_bulk_fetcher() -> Callable[[Path], None]:
    """Create a fetcher function for CRS bulk download.

    The fetcher downloads parquet files from oda_reader, cleans the column
    names using batch processing to minimize memory usage, and writes to the
    target path.

    Note: oda_reader>=1.3.0 saves parquet files directly to a directory
    instead of creating a zip file.

    Returns:
        Fetcher function that takes a target Path and writes parquet to it
    """
    from oda_reader import bulk_download_crs

    from oda_data.cache.api import is_scope_enabled
    from oda_data.clean_data.common import clean_parquet_file_in_batches

    def fetcher(target_path: Path) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            download_dir = tmpdir / "crs_download"

            try:
                # Download from oda_reader (creates directory with parquet files).
                # Honour cache.disable_cache("raw"): the per-call use_raw_cache
                # flag is the only way to bypass oda_reader's bulk_files cache.
                logger.info(
                    "Starting CRS bulk download (this may take several minutes)"
                )
                bulk_download_crs(
                    save_to_path=download_dir,
                    use_raw_cache=is_scope_enabled("raw"),
                )

                # Find parquet files in the download directory
                parquet_files = list(download_dir.glob("*.parquet"))
                if not parquet_files:
                    raise ValueError(
                        f"No parquet files found in download directory. Contents: {list(download_dir.iterdir())}"
                    )

                # Use the first (should be only) parquet file
                parquet_file = parquet_files[0]
                logger.info(
                    f"CRS data downloaded successfully ({parquet_file.stat().st_size / (1024**2):.1f} MB)"
                )

            except Exception as e:
                logger.error(f"Failed to download CRS bulk data: {e}")
                raise RuntimeError(
                    f"CRS bulk download failed. Please check your internet connection and try again. Error: {e}"
                ) from e

            try:
                # Clean and write to target using batch processing
                logger.info(
                    "Cleaning CRS data (using batch processing to minimize memory)"
                )
                clean_parquet_file_in_batches(
                    parquet_file, target_path, batch_size=100_000
                )
                logger.info(
                    f"CRS bulk cache created successfully ({target_path.stat().st_size / (1024**2):.1f} MB)"
                )
            except Exception as e:
                logger.error(f"Failed to process CRS data: {e}")
                raise RuntimeError(f"Failed to process CRS data. Error: {e}") from e

    return fetcher


def create_multisystem_bulk_fetcher() -> Callable[[Path], None]:
    """Create a fetcher function for MultiSystem bulk download.

    The fetcher downloads parquet files from oda_reader, cleans the column
    names using batch processing to minimize memory usage, and writes to the
    target path.

    Note: oda_reader>=1.3.0 saves parquet files directly to a directory
    instead of creating a zip file.

    Returns:
        Fetcher function that takes a target Path and writes parquet to it
    """
    from oda_reader import bulk_download_multisystem

    from oda_data.cache.api import is_scope_enabled
    from oda_data.clean_data.common import clean_parquet_file_in_batches

    def fetcher(target_path: Path) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            download_dir = tmpdir / "multisystem_download"

            try:
                # Download from oda_reader (creates directory with parquet files).
                # Honour cache.disable_cache("raw") via the per-call flag.
                logger.info(
                    "Starting MultiSystem bulk download (this may take several minutes)"
                )
                bulk_download_multisystem(
                    save_to_path=download_dir,
                    use_raw_cache=is_scope_enabled("raw"),
                )

                # Find parquet files in the download directory
                parquet_files = list(download_dir.glob("*.parquet"))
                if not parquet_files:
                    raise ValueError(
                        f"No parquet files found in download directory. Contents: {list(download_dir.iterdir())}"
                    )

                # Use the first (should be only) parquet file
                parquet_file = parquet_files[0]
                logger.info(
                    f"MultiSystem data downloaded successfully ({parquet_file.stat().st_size / (1024**2):.1f} MB)"
                )

            except Exception as e:
                logger.error(f"Failed to download MultiSystem bulk data: {e}")
                raise RuntimeError(
                    f"MultiSystem bulk download failed. Please check your internet connection and try again. Error: {e}"
                ) from e

            try:
                # Clean and write to target using batch processing
                logger.info(
                    "Cleaning MultiSystem data (using batch processing to minimize memory)"
                )
                clean_parquet_file_in_batches(
                    parquet_file, target_path, batch_size=100_000
                )
                logger.info(
                    f"MultiSystem bulk cache created successfully ({target_path.stat().st_size / (1024**2):.1f} MB)"
                )
            except Exception as e:
                logger.error(f"Failed to process MultiSystem data: {e}")
                raise RuntimeError(
                    f"Failed to process MultiSystem data. Error: {e}"
                ) from e

    return fetcher


def create_aiddata_bulk_fetcher() -> Callable[[Path], None]:
    """Create a fetcher function for AidData bulk download.

    The fetcher downloads parquet files from oda_reader, cleans the column
    names using batch processing to minimize memory usage, and writes to the
    target path.

    Note: oda_reader>=1.3.0 saves parquet files directly to a directory
    instead of creating a zip file.

    Returns:
        Fetcher function that takes a target Path and writes parquet to it
    """
    from oda_reader import download_aiddata

    from oda_data.clean_data.common import clean_parquet_file_in_batches

    def fetcher(target_path: Path) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            download_dir = tmpdir / "aiddata_download"

            try:
                # Download from oda_reader (creates directory with parquet files)
                logger.info(
                    "Starting AidData bulk download (this may take several minutes)"
                )
                download_aiddata(save_to_path=download_dir)

                # Find parquet files in the download directory
                parquet_files = list(download_dir.glob("*.parquet"))
                if not parquet_files:
                    raise ValueError(
                        f"No parquet files found in download directory. Contents: {list(download_dir.iterdir())}"
                    )

                # Use the first (should be only) parquet file
                parquet_file = parquet_files[0]
                logger.info(
                    f"AidData data downloaded successfully ({parquet_file.stat().st_size / (1024**2):.1f} MB)"
                )

            except Exception as e:
                logger.error(f"Failed to download AidData bulk data: {e}")
                raise RuntimeError(
                    f"AidData bulk download failed. Please check your internet connection and try again. Error: {e}"
                ) from e

            try:
                # Clean and write to target using batch processing
                logger.info(
                    "Cleaning AidData data (using batch processing to minimize memory)"
                )
                clean_parquet_file_in_batches(
                    parquet_file, target_path, batch_size=100_000
                )
                logger.info(
                    f"AidData bulk cache created successfully ({target_path.stat().st_size / (1024**2):.1f} MB)"
                )
            except Exception as e:
                logger.error(f"Failed to process AidData data: {e}")
                raise RuntimeError(f"Failed to process AidData data. Error: {e}") from e

    return fetcher
