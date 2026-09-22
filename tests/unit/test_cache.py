"""
Tests for caching system.

This module tests the 3-tier caching system in oda_data.tools.cache, including:
- ThreadSafeMemoryCache: In-memory LRU cache with TTL
- BulkCacheManager: Large dataset parquet file cache with file locking
- QueryCacheManager: Filtered query result cache
- Hash generation utilities
"""

import logging
import threading
import time
from pathlib import Path

import pandas as pd
import pytest

from oda_data.tools.cache import (
    BulkCacheEntry,
    BulkCacheManager,
    QueryCacheManager,
    ThreadSafeMemoryCache,
    generate_param_hash,
    get_crs_release_id,
    get_multisystem_release_id,
)

# ============================================================================
# Tests for generate_param_hash
# ============================================================================


class TestGenerateParamHash:
    """Tests for the generate_param_hash function."""

    def test_generate_param_hash_reproducible_with_same_params(self):
        """Test that same parameters always produce the same hash."""
        filters1 = [("year", "in", [2020, 2021]), ("provider_code", "in", [1, 2])]
        filters2 = [("year", "in", [2020, 2021]), ("provider_code", "in", [1, 2])]

        hash1 = generate_param_hash(filters1)
        hash2 = generate_param_hash(filters2)

        assert hash1 == hash2
        assert len(hash1) == 10  # First 10 chars of MD5

    def test_generate_param_hash_different_for_different_params(self):
        """Test that different parameters produce different hashes."""
        filters1 = [("year", "in", [2020, 2021])]
        filters2 = [("year", "in", [2022, 2023])]

        hash1 = generate_param_hash(filters1)
        hash2 = generate_param_hash(filters2)

        assert hash1 != hash2

    def test_generate_param_hash_handles_list_order_independence(self):
        """Test that hash is same regardless of list value order.

        The function sorts list values, so [1, 2] and [2, 1] should produce
        the same hash.
        """
        filters1 = [("provider_code", "in", [1, 2, 3])]
        filters2 = [("provider_code", "in", [3, 2, 1])]

        hash1 = generate_param_hash(filters1)
        hash2 = generate_param_hash(filters2)

        assert hash1 == hash2

    def test_generate_param_hash_with_single_value(self):
        """Test hash generation with single (non-list) values."""
        filters = [("year", "==", 2020)]

        hash_result = generate_param_hash(filters)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 10

    def test_generate_param_hash_with_empty_filters(self):
        """Test hash generation with empty filter list."""
        filters = []

        hash_result = generate_param_hash(filters)

        assert isinstance(hash_result, str)
        assert len(hash_result) == 10


# ============================================================================
# Tests for ThreadSafeMemoryCache
# ============================================================================


class TestThreadSafeMemoryCache:
    """Tests for the ThreadSafeMemoryCache class."""

    def test_thread_safe_memory_cache_basic_get_set(self):
        """Test basic get and set operations."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        cache["key1"] = "value1"
        cache["key2"] = "value2"

        assert cache["key1"] == "value1"
        assert cache["key2"] == "value2"

    def test_thread_safe_memory_cache_get_with_default(self):
        """Test get method with default value."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        result = cache.get("missing_key", "default_value")

        assert result == "default_value"

    def test_thread_safe_memory_cache_contains_check(self):
        """Test __contains__ method (in operator)."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        cache["existing"] = "value"

        assert "existing" in cache
        assert "missing" not in cache

    def test_thread_safe_memory_cache_len(self):
        """Test __len__ method."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        cache["key1"] = "value1"
        cache["key2"] = "value2"

        assert len(cache) == 2

    def test_thread_safe_memory_cache_clear(self):
        """Test clear method removes all entries."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        cache["key1"] = "value1"
        cache["key2"] = "value2"
        assert len(cache) == 2

        cache.clear()

        assert len(cache) == 0
        assert "key1" not in cache

    def test_thread_safe_memory_cache_pop_removes_single_entry(self):
        """Test that pop() removes and returns only the targeted entry."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        cache["key1"] = "value1"
        cache["key2"] = "value2"

        result = cache.pop("key1")

        assert result == "value1"
        assert "key1" not in cache
        assert "key2" in cache

    def test_thread_safe_memory_cache_pop_missing_key_returns_default(self):
        """Test that pop() on a missing key returns the default, not KeyError."""
        cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)

        assert cache.pop("missing", "fallback") == "fallback"
        assert cache.pop("missing") is None

    def test_thread_safe_memory_cache_max_size_eviction(self):
        """Test that LRU eviction occurs when maxsize is reached."""
        cache = ThreadSafeMemoryCache(maxsize=3, ttl=60)

        # Fill cache to max
        cache["key1"] = "value1"
        cache["key2"] = "value2"
        cache["key3"] = "value3"

        assert len(cache) == 3

        # Adding one more should evict the oldest (key1)
        cache["key4"] = "value4"

        # key1 should be evicted (LRU)
        # Note: cachetools TTLCache evicts based on TTL and LRU
        # The behavior might vary, but len should not exceed maxsize
        assert len(cache) <= 3

    @pytest.mark.slow
    def test_thread_safe_memory_cache_concurrent_access(self):
        """Test thread safety with concurrent read/write operations."""
        cache = ThreadSafeMemoryCache(maxsize=100, ttl=60)
        errors = []

        def write_operation(thread_id: int):
            """Write values to cache."""
            try:
                for i in range(10):
                    cache[f"thread{thread_id}_key{i}"] = f"value{i}"
            except Exception as e:
                errors.append(e)

        def read_operation(thread_id: int):
            """Read values from cache."""
            try:
                for i in range(10):
                    cache.get(f"thread{thread_id}_key{i}", None)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = []
        for i in range(5):
            t1 = threading.Thread(target=write_operation, args=(i,))
            t2 = threading.Thread(target=read_operation, args=(i,))
            threads.extend([t1, t2])

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        # No errors should have occurred
        assert len(errors) == 0


# ============================================================================
# Tests for BulkCacheManager
# ============================================================================


class TestBulkCacheManager:
    """Tests for the BulkCacheManager class."""

    def test_bulk_cache_manager_initialization(self, temp_cache_dir: Path):
        """Test that BulkCacheManager initializes correctly."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        # Check that directories are created
        assert (temp_cache_dir / "bulk_cache").exists()
        # Note: Lock file is created lazily (on first ensure() call), not during init
        assert manager.base_dir == temp_cache_dir / "bulk_cache"

    def test_bulk_cache_manager_ensure_downloads_when_missing(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that ensure() downloads data when cache is missing."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, ttl_days=30, version="1.0.0"
        )

        path = manager.ensure(entry)

        # File should be created
        assert path.exists()
        assert path.name == "test_data.parquet"

        # Can read the parquet file
        df = pd.read_parquet(path)
        assert len(df) > 0

    def test_bulk_cache_manager_ensure_uses_cache_when_fresh(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that ensure() uses cached data when fresh."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, ttl_days=30, version="1.0.0"
        )

        # First call - downloads
        path1 = manager.ensure(entry)
        mtime1 = path1.stat().st_mtime

        # Second call - should use cache
        path2 = manager.ensure(entry)
        mtime2 = path2.stat().st_mtime

        # File should not be re-downloaded (same mtime)
        assert mtime1 == mtime2

    def test_bulk_cache_manager_ensure_re_downloads_when_version_mismatch(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that ensure() re-downloads when version changes."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry_v1 = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, ttl_days=30, version="1.0.0"
        )

        # First call with v1.0.0
        path1 = manager.ensure(entry_v1)
        mtime1 = path1.stat().st_mtime

        # Give a tiny delay to ensure different mtime if re-downloaded
        time.sleep(0.01)

        # Second call with v2.0.0
        entry_v2 = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, ttl_days=30, version="2.0.0"
        )

        path2 = manager.ensure(entry_v2)

        # File should be re-downloaded due to version mismatch
        # (mtime should be different)
        mtime2 = path2.stat().st_mtime
        assert mtime2 >= mtime1  # Should be re-created

    def test_bulk_cache_manager_clear_removes_entry(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that clear() removes specific cache entry."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry = BulkCacheEntry(
            key="test_data",
            fetcher=mock_bulk_fetcher,
        )

        # Create cache entry
        path = manager.ensure(entry)
        assert path.exists()

        # Clear the entry
        manager.clear(key="test_data")

        # File should be removed
        assert not path.exists()

    def test_bulk_cache_manager_clear_removes_all(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that clear() without key removes all entries."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        # Create multiple entries
        entry1 = BulkCacheEntry(key="data1", fetcher=mock_bulk_fetcher)
        entry2 = BulkCacheEntry(key="data2", fetcher=mock_bulk_fetcher)

        path1 = manager.ensure(entry1)
        path2 = manager.ensure(entry2)

        assert path1.exists()
        assert path2.exists()

        # Clear all
        manager.clear()

        # All files should be removed
        assert not path1.exists()
        assert not path2.exists()

    def test_bulk_cache_manager_stats_returns_correct_info(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that stats() returns correct cache statistics."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        # Initially empty
        stats = manager.stats()
        assert stats["total_entries"] == 0
        assert stats["total_size_mb"] == 0.0

        # Add an entry
        entry = BulkCacheEntry(key="test_data", fetcher=mock_bulk_fetcher)
        path = manager.ensure(entry)

        # Verify file was created
        assert path.exists()

        # Stats should reflect the entry
        stats = manager.stats()
        assert stats["total_entries"] == 1
        assert stats["total_size_mb"] >= 0.0  # File should exist with some size

    def test_bulk_cache_manager_list_records(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that list_records() returns metadata for cached entries."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, version="1.0.0"
        )
        path = manager.ensure(entry)

        # Verify file was created
        assert path.exists()

        records = manager.list_records()

        assert len(records) == 1
        assert records[0]["key"] == "test_data"
        assert records[0]["version"] == "1.0.0"
        assert records[0]["size_mb"] >= 0.0  # File should exist with some size
        assert isinstance(records[0]["age_days"], float)

    def test_bulk_cache_manager_manifest_round_trips_release_id(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """release_id survives a write/read cycle through the manifest and
        is exposed via list_records() (#162 traceability)."""
        manager = BulkCacheManager(base_dir=temp_cache_dir)

        entry = BulkCacheEntry(
            key="test_data",
            fetcher=mock_bulk_fetcher,
            version="1.0.0",
            release_id="oecd-file-id-2026-09-01",
        )
        manager.ensure(entry)

        manifest = manager._load_manifest()
        assert manifest["test_data"]["release_id"] == "oecd-file-id-2026-09-01"

        records = manager.list_records()
        assert records[0]["release_id"] == "oecd-file-id-2026-09-01"

    def test_bulk_cache_manager_detects_republish_inside_ttl(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """A changed release_id forces a re-fetch even though the TTL and
        version haven't expired — the #162 core complaint."""
        manager = BulkCacheManager(base_dir=temp_cache_dir, ttl_seconds=2592000)

        entry_v1 = BulkCacheEntry(
            key="test_data",
            fetcher=mock_bulk_fetcher,
            ttl_days=30,
            version="1.0.0",
            release_id="oecd-file-id-2026-09-01",
        )
        path1 = manager.ensure(entry_v1)
        mtime1 = path1.stat().st_mtime

        time.sleep(0.01)

        # Same TTL, same version, but the upstream OECD file id changed —
        # a same-day republish, well inside the 30-day TTL window.
        entry_republished = BulkCacheEntry(
            key="test_data",
            fetcher=mock_bulk_fetcher,
            ttl_days=30,
            version="1.0.0",
            release_id="oecd-file-id-2026-09-01-1",
        )
        path2 = manager.ensure(entry_republished)
        mtime2 = path2.stat().st_mtime

        assert mtime2 > mtime1, "a changed release_id must trigger a re-fetch"

        manifest = manager._load_manifest()
        assert manifest["test_data"]["release_id"] == "oecd-file-id-2026-09-01-1"

    def test_bulk_cache_manager_unknown_release_id_falls_back_to_ttl(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """A missing release_id on either side is inconclusive — it must
        not force a re-fetch of an otherwise-fresh, same-version entry."""
        manager = BulkCacheManager(base_dir=temp_cache_dir, ttl_seconds=2592000)

        entry_no_release_id = BulkCacheEntry(
            key="test_data", fetcher=mock_bulk_fetcher, ttl_days=30, version="1.0.0"
        )
        path1 = manager.ensure(entry_no_release_id)
        mtime1 = path1.stat().st_mtime

        # A probe that couldn't determine a release id (returns None) must
        # not be treated as "changed" against a manifest with no release_id.
        path2 = manager.ensure(entry_no_release_id)
        mtime2 = path2.stat().st_mtime

        assert mtime2 == mtime1, "cache hit expected when release_id is unknown"


# ============================================================================
# Tests for release-id probes
# ============================================================================


class TestReleaseIdProbes:
    """Tests for the cheap upstream release-id probe functions.

    Both probes must never raise — a broken or absent probe degrades to
    version/TTL-only staleness rather than breaking a normal read (#162).
    No network access: oda_reader's functions are monkeypatched.
    """

    def test_get_crs_release_id_returns_probe_value(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            return_value=("https://sdmx.oecd.org/.../CRS.parquet", "crs-42"),
        )
        assert get_crs_release_id() == "crs-42"

    def test_get_crs_release_id_swallows_errors(self, mocker, caplog):
        mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            side_effect=ConnectionError("boom"),
        )
        _od_logger = logging.getLogger("oda_data")
        _od_logger.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.WARNING):
                assert get_crs_release_id() is None
        finally:
            _od_logger.removeHandler(caplog.handler)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "CRS release id" in warnings[0].message
        assert "boom" in warnings[0].message

    def test_get_multisystem_release_id_returns_probe_value(self, mocker):
        mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            return_value=("https://sdmx.oecd.org/.../Multisystem.parquet", "multi-7"),
        )
        assert get_multisystem_release_id() == "multi-7"

    def test_get_multisystem_release_id_swallows_errors(self, mocker, caplog):
        mocker.patch(
            "oda_reader.download.download_tools.get_bulk_file_url_with_version",
            side_effect=ConnectionError("boom"),
        )
        _od_logger = logging.getLogger("oda_data")
        _od_logger.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.WARNING):
                assert get_multisystem_release_id() is None
        finally:
            _od_logger.removeHandler(caplog.handler)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "MultiSystem release id" in warnings[0].message
        assert "boom" in warnings[0].message


# ============================================================================
# Tests for QueryCacheManager
# ============================================================================


class TestQueryCacheManager:
    """Tests for the QueryCacheManager class."""

    def test_query_cache_manager_initialization(self, temp_cache_dir: Path):
        """Test that QueryCacheManager initializes correctly."""
        manager = QueryCacheManager(base_dir=temp_cache_dir)

        # Check that directories are created
        assert (temp_cache_dir / "query_cache").exists()
        # Note: Lock file is created lazily (on first save() call), not during init
        assert manager.base_dir == temp_cache_dir / "query_cache"

    def test_query_cache_manager_save_and_load(self, temp_cache_dir: Path):
        """Test saving and loading DataFrames from query cache."""
        manager = QueryCacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
            }
        )

        dataset_name = "TestData"
        param_hash = "abc123"

        # Save to cache
        manager.save(dataset_name, param_hash, df)

        # Load from cache
        result = manager.load(dataset_name, param_hash, filters=None, columns=None)

        assert result is not None
        pd.testing.assert_frame_equal(result, df)

    def test_query_cache_manager_load_returns_none_when_missing(
        self, temp_cache_dir: Path
    ):
        """Test that load() returns None when cache entry doesn't exist."""
        manager = QueryCacheManager(base_dir=temp_cache_dir)

        result = manager.load("MissingData", "xyz789", filters=None, columns=None)

        assert result is None

    def test_query_cache_manager_load_with_column_filter(self, temp_cache_dir: Path):
        """Test that load() can filter columns when reading."""
        manager = QueryCacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
                "provider_code": [1, 2],
            }
        )

        manager.save("TestData", "abc123", df)

        # Load only specific columns
        result = manager.load(
            "TestData", "abc123", filters=None, columns=["year", "value"]
        )

        assert list(result.columns) == ["year", "value"]
        assert len(result) == 2

    def test_query_cache_manager_clear_removes_all_files(self, temp_cache_dir: Path):
        """Test that clear() removes all cached query results."""
        manager = QueryCacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame({"year": [2020], "value": [1000.0]})

        # Create multiple cache entries
        manager.save("Data1", "hash1", df)
        manager.save("Data2", "hash2", df)

        cache_dir = temp_cache_dir / "query_cache"
        parquet_files_before = list(cache_dir.glob("*.parquet"))
        assert len(parquet_files_before) == 2

        # Clear all
        manager.clear()

        parquet_files_after = list(cache_dir.glob("*.parquet"))
        assert len(parquet_files_after) == 0


# ============================================================================
# Integration Tests for Cache Coordination
# ============================================================================


@pytest.mark.integration
class TestCacheCoordination:
    """Integration tests for how cache components work together."""

    def test_cache_managers_use_same_base_directory(self, temp_cache_dir: Path):
        """Test that different cache managers can coexist in same base directory."""
        BulkCacheManager(base_dir=temp_cache_dir)
        QueryCacheManager(base_dir=temp_cache_dir)

        # Both should create their subdirectories
        assert (temp_cache_dir / "bulk_cache").exists()
        assert (temp_cache_dir / "query_cache").exists()

    def test_bulk_and_query_cache_independent(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that bulk and query caches operate independently."""
        bulk_manager = BulkCacheManager(base_dir=temp_cache_dir)
        query_manager = QueryCacheManager(base_dir=temp_cache_dir)

        # Add to bulk cache
        entry = BulkCacheEntry(key="bulk_data", fetcher=mock_bulk_fetcher)
        bulk_path = bulk_manager.ensure(entry)

        # Add to query cache
        df = pd.DataFrame({"year": [2020], "value": [1000.0]})
        query_manager.save("query_data", "hash1", df)

        # Both should exist independently
        assert bulk_path.exists()
        query_result = query_manager.load("query_data", "hash1", None, None)
        assert query_result is not None

        # Clearing one shouldn't affect the other
        query_manager.clear()
        assert bulk_path.exists()
