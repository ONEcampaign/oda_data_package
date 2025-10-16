"""
Integration tests for 3-tier cache coordination.

This module tests how the different cache layers (memory, query, bulk) work
together in realistic data retrieval scenarios, validating the cache tier
fallback logic and coordination.
"""

from pathlib import Path

import pandas as pd
import pytest

from oda_data.tools.cache import (
    BulkCacheEntry,
    BulkCacheManager,
    QueryCacheManager,
    ThreadSafeMemoryCache,
)

# ============================================================================
# Integration Tests - Cache Tier Coordination
# ============================================================================


@pytest.mark.integration
class TestCacheTierCoordination:
    """Integration tests for cache tier fallback and coordination."""

    def test_cache_tiers_work_together_correctly(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that all three cache tiers coordinate correctly.

        Simulates a typical data retrieval workflow:
        1. Miss in all caches → fetch from source (bulk)
        2. Populate query cache after filtering
        3. Populate memory cache after read
        4. Subsequent reads hit memory cache
        """
        # Initialize all three cache managers
        memory_cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)
        query_manager = QueryCacheManager(base_dir=temp_cache_dir)
        bulk_manager = BulkCacheManager(base_dir=temp_cache_dir)

        # First access - all caches miss, fetch from bulk
        cache_key = "test_data"

        # Check memory cache - miss
        assert cache_key not in memory_cache

        # Check query cache - miss
        query_result = query_manager.load("TestData", "hash1", None, None)
        assert query_result is None

        # Fetch from bulk cache
        entry = BulkCacheEntry(key="TestData", fetcher=mock_bulk_fetcher)
        bulk_path = bulk_manager.ensure(entry)
        df = pd.read_parquet(bulk_path)

        # Populate query cache
        query_manager.save("TestData", "hash1", df)

        # Populate memory cache
        memory_cache[cache_key] = df

        # Second access - should hit memory cache
        assert cache_key in memory_cache
        cached_df = memory_cache[cache_key]
        pd.testing.assert_frame_equal(cached_df, df)

    def test_memory_cache_miss_falls_back_to_query_cache(self, temp_cache_dir: Path):
        """Test that memory cache miss correctly falls back to query cache."""
        memory_cache = ThreadSafeMemoryCache(maxsize=10, ttl=60)
        query_manager = QueryCacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
            }
        )

        # Populate only query cache (skip memory)
        query_manager.save("TestData", "hash1", df)

        # Memory cache miss
        assert "test_key" not in memory_cache

        # Query cache hit
        query_result = query_manager.load("TestData", "hash1", None, None)
        assert query_result is not None
        pd.testing.assert_frame_equal(query_result, df)

    def test_query_cache_miss_falls_back_to_bulk_cache(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that query cache miss correctly falls back to bulk cache."""
        query_manager = QueryCacheManager(base_dir=temp_cache_dir)
        bulk_manager = BulkCacheManager(base_dir=temp_cache_dir)

        # Query cache miss
        query_result = query_manager.load("TestData", "hash1", None, None)
        assert query_result is None

        # Bulk cache should be fetched
        entry = BulkCacheEntry(key="TestData", fetcher=mock_bulk_fetcher)
        bulk_path = bulk_manager.ensure(entry)

        assert bulk_path.exists()

        # Read from bulk and populate query cache
        df = pd.read_parquet(bulk_path)
        query_manager.save("TestData", "hash1", df)

        # Now query cache should hit
        query_result = query_manager.load("TestData", "hash1", None, None)
        assert query_result is not None


# ============================================================================
# Integration Tests - Concurrent Access
# ============================================================================


@pytest.mark.integration
@pytest.mark.slow
class TestCacheConcurrentAccess:
    """Integration tests for concurrent access to cache system."""

    def test_concurrent_bulk_cache_downloads_use_file_lock(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that concurrent bulk cache accesses are serialized by file lock.

        This simulates multiple threads trying to download the same bulk data
        simultaneously. The file lock should ensure only one download occurs.
        """
        import threading

        manager = BulkCacheManager(base_dir=temp_cache_dir)
        entry = BulkCacheEntry(key="concurrent_test", fetcher=mock_bulk_fetcher)

        results = []
        errors = []

        def download_data():
            """Thread worker that attempts to ensure cache."""
            try:
                path = manager.ensure(entry)
                results.append(path)
            except Exception as e:
                errors.append(e)

        # Create multiple threads that all try to download
        threads = [threading.Thread(target=download_data) for _ in range(5)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all to complete
        for t in threads:
            t.join()

        # No errors should occur
        assert len(errors) == 0

        # All threads should get the same path
        assert len(results) == 5
        assert all(path == results[0] for path in results)

        # Only one file should exist
        cache_dir = temp_cache_dir / "bulk_cache"
        parquet_files = list(cache_dir.glob("*.parquet"))
        assert len(parquet_files) == 1

    def test_concurrent_query_cache_writes_are_safe(self, temp_cache_dir: Path):
        """Test that concurrent query cache writes don't corrupt data."""
        import threading

        manager = QueryCacheManager(base_dir=temp_cache_dir)

        errors = []

        def write_data(thread_id: int):
            """Thread worker that writes to query cache."""
            try:
                df = pd.DataFrame({"thread_id": [thread_id] * 10, "value": range(10)})
                manager.save(f"ThreadData{thread_id}", f"hash{thread_id}", df)
            except Exception as e:
                errors.append(e)

        # Create multiple threads writing different data
        threads = [threading.Thread(target=write_data, args=(i,)) for i in range(5)]

        # Start all threads
        for t in threads:
            t.start()

        # Wait for all to complete
        for t in threads:
            t.join()

        # No errors should occur
        assert len(errors) == 0

        # All files should exist and be readable
        for i in range(5):
            result = manager.load(f"ThreadData{i}", f"hash{i}", None, None)
            assert result is not None
            assert len(result) == 10
            assert (result["thread_id"] == i).all()


# ============================================================================
# Integration Tests - Cache Cleanup
# ============================================================================


@pytest.mark.integration
class TestCacheCleanup:
    """Integration tests for cache cleanup operations."""

    def test_clearing_one_cache_doesnt_affect_others(
        self, temp_cache_dir: Path, mock_bulk_fetcher
    ):
        """Test that clearing one cache tier doesn't affect other tiers."""
        query_manager = QueryCacheManager(base_dir=temp_cache_dir)
        bulk_manager = BulkCacheManager(base_dir=temp_cache_dir)

        df = pd.DataFrame({"year": [2020], "value": [1000.0]})

        # Populate both caches
        query_manager.save("TestData", "hash1", df)

        entry = BulkCacheEntry(key="BulkTest", fetcher=mock_bulk_fetcher)
        bulk_path = bulk_manager.ensure(entry)

        # Clear query cache
        query_manager.clear()

        # Query cache should be empty
        query_result = query_manager.load("TestData", "hash1", None, None)
        assert query_result is None

        # Bulk cache should still exist
        assert bulk_path.exists()

    def test_cache_directory_isolation(self, temp_cache_dir: Path):
        """Test that different cache types use isolated directories."""
        QueryCacheManager(base_dir=temp_cache_dir)
        BulkCacheManager(base_dir=temp_cache_dir)

        # Different subdirectories should exist
        query_dir = temp_cache_dir / "query_cache"
        bulk_dir = temp_cache_dir / "bulk_cache"

        assert query_dir.exists()
        assert bulk_dir.exists()
        assert query_dir != bulk_dir
