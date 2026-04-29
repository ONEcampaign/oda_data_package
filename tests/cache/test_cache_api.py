"""Unit tests for the oda_data.cache public API.

Each test exercises one specific behaviour of a public cache function.
The tmp_cache_root fixture (from conftest.py) ensures tests never touch
the real user cache.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import re
import time
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

# Guard: slice (b) must land before these tests can run.
pytest.importorskip(
    "oda_data.cache",
    reason="requires slice (b): oda_data.cache not yet available",
)

from oda_data import cache

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_parquet(path: Path) -> None:
    """Write a minimal parquet file to *path*."""
    df = pd.DataFrame({"year": [2022], "value": [1.0]})
    df.to_parquet(path, engine="pyarrow")


def _hold_lock(
    lock_path: str,
    hold_event: mp.synchronize.Event,
    release_event: mp.synchronize.Event,
) -> None:
    """Acquire FileLock at *lock_path*, signal *hold_event*, wait for *release_event*.

    Must be module-level so it is picklable by the ``spawn`` multiprocessing
    context used in the contention test.
    """
    from filelock import FileLock

    with FileLock(lock_path):
        hold_event.set()
        release_event.wait(timeout=10)


# ---------------------------------------------------------------------------
# cache.path
# ---------------------------------------------------------------------------


def test_path_default_returns_root(tmp_cache_root: Path) -> None:
    """cache.path() with no args returns the cache root (scope='all')."""
    result = cache.path()
    assert result == tmp_cache_root


def test_path_per_scope_returns_subdir(tmp_cache_root: Path) -> None:
    """cache.path(scope) returns a subdirectory for each non-'all' scope."""
    bulk = cache.path("bulk")
    assert bulk == tmp_cache_root / "oda-data" / "bulk_cache"

    query = cache.path("query")
    assert query == tmp_cache_root / "oda-data" / "query_cache"

    http = cache.path("http")
    assert http == tmp_cache_root / "oda-reader" / "http_cache"

    raw = cache.path("raw")
    assert raw == tmp_cache_root / "oda-reader" / "bulk_files"


def test_path_rejects_unknown_scope(tmp_cache_root: Path) -> None:
    """cache.path('nonsense') raises ValueError."""
    with pytest.raises(ValueError, match="nonsense"):
        cache.path("nonsense")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# cache.entries
# ---------------------------------------------------------------------------


def test_entries_returns_records_per_scope(
    populated_bulk_cache: Path, tmp_cache_root: Path
) -> None:
    """cache.entries() returns a four-key dict, one list per scope."""
    result = cache.entries()
    # Must have exactly the four concrete scopes (not "all").
    assert set(result.keys()) == {"bulk", "query", "http", "raw"}
    # populated_bulk_cache fixture wrote one parquet file in bulk.
    assert len(result["bulk"]) >= 1
    # Spot-check the CacheRecord fields.
    record = result["bulk"][0]
    assert record.scope == "bulk"
    assert record.size_bytes > 0
    assert record.age_days >= 0


def test_entries_does_not_shadow_builtin_list(tmp_cache_root: Path) -> None:
    """Calling cache.entries() does not break list(...)."""
    cache.entries()
    # If 'entries' shadowed the builtin 'list', this would raise NameError.
    assert list(range(3)) == [0, 1, 2]


# ---------------------------------------------------------------------------
# cache.clear
# ---------------------------------------------------------------------------


def test_clear_blocking_clears_all(tmp_cache_root: Path) -> None:
    """cache.clear('all') empties every scope."""
    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    _write_parquet(bulk_dir / "CRSData_bulk.parquet")

    result = cache.clear("all")
    assert set(result.keys()) == {"bulk", "query", "http", "raw"}
    # bulk had one file → should return 1 (or possibly 2 if manifest existed).
    assert result["bulk"] is not None and result["bulk"] >= 1
    assert not list(bulk_dir.glob("*.parquet"))


def test_clear_returns_int_per_scope(tmp_cache_root: Path) -> None:
    """cache.clear('bulk') returns {'bulk': N} where N is files deleted."""
    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    for name in ("A_bulk.parquet", "B_bulk.parquet", "C_bulk.parquet"):
        _write_parquet(bulk_dir / name)

    result = cache.clear("bulk")
    assert set(result.keys()) == {"bulk"}
    assert isinstance(result["bulk"], int)
    assert result["bulk"] == 3


def test_clear_non_blocking_returns_none_under_contention(
    tmp_cache_root: Path,
) -> None:
    """cache.clear(blocking=False) returns None for the contended scope."""
    ctx = mp.get_context("spawn")
    hold_event = ctx.Event()
    release_event = ctx.Event()

    # BulkCacheManager uses this lock path.
    lock_path = tmp_cache_root / "oda-data" / "bulk_cache" / ".cache.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.touch()

    holder = ctx.Process(
        target=_hold_lock,
        args=(str(lock_path), hold_event, release_event),
    )
    holder.start()
    try:
        assert hold_event.wait(timeout=5), "Holder process never acquired the lock"
        result = cache.clear("bulk", blocking=False)
        assert result == {"bulk": None}, f"Expected {{'bulk': None}}, got {result}"
    finally:
        release_event.set()
        holder.join(timeout=5)


def test_clear_rejects_unknown_scope(tmp_cache_root: Path) -> None:
    """cache.clear('nonsense') raises ValueError."""
    with pytest.raises(ValueError, match="nonsense"):
        cache.clear("nonsense")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# cache.size
# ---------------------------------------------------------------------------


def test_size_returns_int_bytes(tmp_cache_root: Path) -> None:
    """cache.size()['bulk'] equals the on-disk byte count."""
    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    sentinel = bulk_dir / "CRSData_bulk.parquet"
    sentinel.write_bytes(b"\x00" * 1024)

    result = cache.size()
    assert set(result.keys()) == {"bulk", "query", "http", "raw"}
    assert isinstance(result["bulk"], int)
    assert result["bulk"] == 1024


def test_size_warns_above_5gb(
    tmp_cache_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """cache.size() logs one WARNING when total cache exceeds 5 GB."""
    import oda_data.cache.api as _api

    # The "oda_data" logger has propagate=False, so caplog's root handler doesn't
    # see records. Attach caplog.handler directly to it for the test.
    _od_logger = logging.getLogger("oda_data")
    _od_logger.addHandler(caplog.handler)

    # Reset and restore the module-level gate so other tests in the same process
    # are not affected by the flag being True after this test runs.
    original = _api._WARNED_STALE_5GB
    _api._WARNED_STALE_5GB = False
    try:
        # Use a sparse file (no real disk space consumed on APFS / ext4) to push
        # the stale total over the 5 GB threshold.
        parent = tmp_cache_root.parent
        stale_root = parent / "stale-version"
        stale_root.mkdir(parents=True, exist_ok=True)
        stale_file = stale_root / "big.bin"
        stale_file.touch()
        os.truncate(stale_file, 6 * 2**30)  # 6 GB sparse

        with caplog.at_level(logging.WARNING):
            cache.size()

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) >= 1
        assert any('clear(scope="raw")' in r.message for r in warnings)

        # Second call must NOT emit another warning (process-level gate).
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            cache.size()
        second_warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "5 GB" in r.message
        ]
        assert len(second_warnings) == 0, "Should not warn twice in the same process"
    finally:
        _api._WARNED_STALE_5GB = original
        _od_logger.removeHandler(caplog.handler)


def test_size_rejects_unknown_scope_via_path(tmp_cache_root: Path) -> None:
    """Calling cache.path with an invalid scope propagates a ValueError."""
    with pytest.raises(ValueError, match="nonsense"):
        cache.path("nonsense")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# cache.invalidate
# ---------------------------------------------------------------------------


def test_invalidate_known_class_clears_one_entry(
    populated_bulk_cache: Path,
    tmp_cache_root: Path,
) -> None:
    """cache.invalidate(CRSData) removes only the CRSData_bulk entry."""
    from oda_data import (
        CRSData,
        DAC1Data,  # noqa: F401 — imported to register the subclass
    )

    bulk_dir = populated_bulk_cache  # already has CRSData_bulk.parquet
    # Also add a second dataset entry so we can verify only one is removed.
    _write_parquet(bulk_dir / "DAC1Data_bulk.parquet")

    cache.invalidate(CRSData)

    assert not (bulk_dir / "CRSData_bulk.parquet").exists()
    assert (bulk_dir / "DAC1Data_bulk.parquet").exists()


def test_invalidate_known_string_clears_one_entry(
    populated_bulk_cache: Path,
    tmp_cache_root: Path,
) -> None:
    """cache.invalidate('CRSData') removes only the CRSData_bulk entry."""
    from oda_data import DAC1Data  # noqa: F401 — imported to register subclass

    bulk_dir = populated_bulk_cache
    _write_parquet(bulk_dir / "DAC1Data_bulk.parquet")

    cache.invalidate("CRSData")

    assert not (bulk_dir / "CRSData_bulk.parquet").exists()
    assert (bulk_dir / "DAC1Data_bulk.parquet").exists()


def test_invalidate_lowercase_string_raises(tmp_cache_root: Path) -> None:
    """cache.invalidate('crsdata') raises ValueError; message lists 'CRSData'."""
    with pytest.raises(ValueError) as exc_info:
        cache.invalidate("crsdata")
    assert "CRSData" in str(exc_info.value)


def test_invalidate_unknown_string_raises_valueerror(tmp_cache_root: Path) -> None:
    """cache.invalidate('Nonsense') raises ValueError."""
    with pytest.raises(ValueError, match="Nonsense"):
        cache.invalidate("Nonsense")


# ---------------------------------------------------------------------------
# cache.enable_cache / cache.disable_cache
# ---------------------------------------------------------------------------


def test_enable_disable_cache_per_scope(tmp_cache_root: Path) -> None:
    """disable_cache('bulk') prevents caching; enable_cache restores it."""
    from oda_data.cache.api import is_scope_enabled

    # Disable bulk scope and verify the flag is set.
    cache.disable_cache("bulk")
    assert not is_scope_enabled("bulk")

    # Re-enable and confirm the flag is restored.
    cache.enable_cache("bulk")
    assert is_scope_enabled("bulk")

    # Invalid scope should raise ValueError on both.
    with pytest.raises(ValueError, match="nonsense"):
        cache.disable_cache("nonsense")  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="nonsense"):
        cache.enable_cache("nonsense")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# DACSource.read(refresh=True)
# ---------------------------------------------------------------------------


def test_read_refresh_true_bypasses_cache(tmp_cache_root: Path) -> None:
    """BulkCacheManager.ensure(refresh=True) re-fetches even when cache is warm."""
    from oda_data.tools.cache import BulkCacheEntry, BulkCacheManager

    call_count = {"n": 0}

    def _sentinel_fetcher(target_path: Path) -> None:
        call_count["n"] += 1
        pd.DataFrame({"year": [2022], "value": [float(call_count["n"])]}).to_parquet(
            target_path, engine="pyarrow"
        )

    mgr = BulkCacheManager(tmp_cache_root / "oda-data")
    entry = BulkCacheEntry(key="CRSData_bulk", fetcher=_sentinel_fetcher, ttl_days=30)

    # First ensure — populates the cache (1 fetch).
    mgr.ensure(entry, refresh=False)
    assert call_count["n"] == 1

    # Second ensure without refresh — should be a cache hit (no extra fetch).
    mgr.ensure(entry, refresh=False)
    assert call_count["n"] == 1

    # Third ensure with refresh=True — must trigger a re-fetch even though cached.
    mgr.ensure(entry, refresh=True)
    assert call_count["n"] == 2, "refresh=True must trigger a re-fetch"


# ---------------------------------------------------------------------------
# Lock reentrancy
# ---------------------------------------------------------------------------


def test_clear_lock_reentrancy_via_clear_locked(tmp_cache_root: Path) -> None:
    """Retry path in ensure() uses _clear_locked and does not deadlock."""
    from oda_data.tools.cache import BulkCacheEntry, BulkCacheManager

    bulk_dir = tmp_cache_root / "oda-data"
    mgr = BulkCacheManager(bulk_dir)

    fetch_count = {"n": 0}

    def _corrupt_then_valid(target_path: Path) -> None:
        from oda_reader import BulkPayloadCorruptError

        fetch_count["n"] += 1
        if fetch_count["n"] == 1:
            # First call: write a corrupt payload and raise.
            target_path.write_bytes(b"NOT A PARQUET")
            raise BulkPayloadCorruptError(target_path, reason="test corruption")
        # Second call: write a real parquet.
        df = pd.DataFrame({"year": [2022], "value": [1.0]})
        df.to_parquet(target_path, engine="pyarrow")

    entry = BulkCacheEntry(
        key="CRSData_bulk",
        fetcher=_corrupt_then_valid,
        ttl_days=30,
    )

    # Should not deadlock; BulkPayloadCorruptError triggers _clear_locked (reentrancy).
    result_path = mgr.ensure(entry)
    assert result_path.exists()
    assert fetch_count["n"] == 2, "Expected exactly one retry after corruption"


# ---------------------------------------------------------------------------
# Level-1 holdovers (absorbed from slice (f))
# ---------------------------------------------------------------------------


def test_query_cache_filename_includes_major_version(tmp_cache_root: Path) -> None:
    """Query-cache filename matches r'.+-v\\d+\\.parquet$'."""
    from oda_data.tools.cache import QueryCacheManager

    mgr = QueryCacheManager(tmp_cache_root / "oda-data")
    p = mgr.get_file_path("CRSData", "abc123")
    assert re.search(r".+-v\d+\.parquet$", p.name), (
        f"Query-cache filename {p.name!r} does not include major version segment"
    )


def test_startup_sweep_removes_old_tmp_files(tmp_cache_root: Path) -> None:
    """BulkCacheManager.__init__ removes *.tmp-* files older than 24 h."""
    from oda_data.tools.cache import BulkCacheManager

    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)

    stale_tmp = bulk_dir / "CRSData_bulk.parquet.tmp-host-1234"
    stale_tmp.write_bytes(b"\x00" * 64)
    # Back-date the mtime by 25 hours so it appears stale.
    stale_mtime = time.time() - 25 * 3600
    os.utime(stale_tmp, (stale_mtime, stale_mtime))

    # Constructing BulkCacheManager triggers _sweep_old_tmp_files().
    BulkCacheManager(tmp_cache_root / "oda-data")

    assert not stale_tmp.exists(), (
        "Stale tmp file should have been removed by startup sweep"
    )


def test_startup_sweep_keeps_recent_tmp_files(tmp_cache_root: Path) -> None:
    """BulkCacheManager.__init__ leaves *.tmp-* files younger than 24 h untouched."""
    from oda_data.tools.cache import BulkCacheManager

    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)

    recent_tmp = bulk_dir / "CRSData_bulk.parquet.tmp-host-5678"
    recent_tmp.write_bytes(b"\x00" * 64)
    # mtime is 1 hour ago — should survive the sweep.
    recent_mtime = time.time() - 3600
    os.utime(recent_tmp, (recent_mtime, recent_mtime))

    BulkCacheManager(tmp_cache_root / "oda-data")

    assert recent_tmp.exists(), "Recent tmp file should NOT be removed by startup sweep"


# ---------------------------------------------------------------------------
# F-1: _clear_oda_reader_scope always returns int, never None
# ---------------------------------------------------------------------------


def test_clear_oda_reader_scope_returns_int_not_none(tmp_cache_root: Path) -> None:
    """cache.clear('http', blocking=False) returns an int, never None."""
    http_dir = tmp_cache_root / "oda-reader" / "http_cache"
    http_dir.mkdir(parents=True, exist_ok=True)
    (http_dir / "dummy.bin").write_bytes(b"\x00" * 64)

    result = cache.clear("http", blocking=False)

    assert isinstance(result["http"], int), (
        f"Expected int for http scope, got {result['http']!r}"
    )
    assert result["http"] == 1


# ---------------------------------------------------------------------------
# F-4: enable/disable coupling between http and raw scopes
# ---------------------------------------------------------------------------


def test_disable_http_couples_raw(tmp_cache_root: Path) -> None:
    """disable_cache('http') also disables the 'raw' scope."""
    from oda_data.cache.api import is_scope_enabled

    cache.disable_cache("http")
    assert not is_scope_enabled("http"), "http should be disabled"
    assert not is_scope_enabled("raw"), "raw should be disabled when http is toggled"

    cache.enable_cache("http")
    assert is_scope_enabled("http"), "http should be re-enabled"
    assert is_scope_enabled("raw"), "raw should be re-enabled when http is toggled"


def test_disable_raw_couples_http(tmp_cache_root: Path) -> None:
    """disable_cache('raw') also disables the 'http' scope."""
    from oda_data.cache.api import is_scope_enabled

    cache.disable_cache("raw")
    assert not is_scope_enabled("raw"), "raw should be disabled"
    assert not is_scope_enabled("http"), "http should be disabled when raw is toggled"

    cache.enable_cache("raw")
    assert is_scope_enabled("raw"), "raw should be re-enabled"
    assert is_scope_enabled("http"), "http should be re-enabled when raw is toggled"


# ---------------------------------------------------------------------------
# F-6: _invalidate_query_entries acquires .query.lock
# ---------------------------------------------------------------------------


def test_invalidate_query_acquires_lock(tmp_cache_root: Path) -> None:
    """cache.invalidate blocks while .query.lock is held by another process."""
    import threading

    from oda_data import CRSData  # noqa: F401 — register subclass

    query_dir = tmp_cache_root / "oda-data" / "query_cache"
    query_dir.mkdir(parents=True, exist_ok=True)

    ctx = mp.get_context("spawn")
    hold_event = ctx.Event()
    release_event = ctx.Event()

    lock_path = query_dir / ".query.lock"
    lock_path.touch()

    holder = ctx.Process(
        target=_hold_lock,
        args=(str(lock_path), hold_event, release_event),
    )
    holder.start()

    invalidate_error: list[BaseException] = []

    def _run_invalidate() -> None:
        try:
            cache.invalidate("CRSData")
        except Exception as exc:
            invalidate_error.append(exc)

    try:
        assert hold_event.wait(timeout=5), "Holder process never acquired the lock"

        t = threading.Thread(target=_run_invalidate, daemon=True)
        t.start()
        t.join(timeout=2.0)
        assert t.is_alive(), (
            "invalidate should have been blocked on .query.lock but it already finished"
        )

        release_event.set()
        t.join(timeout=5.0)
        assert not t.is_alive(), (
            "invalidate should have finished after lock was released"
        )
    finally:
        release_event.set()
        holder.join(timeout=5)

    if invalidate_error:
        raise invalidate_error[0]


# ---------------------------------------------------------------------------
# F-7: _invalidate_bulk_entry reuses Source._shared_bulk_cache singleton
# ---------------------------------------------------------------------------


def test_invalidate_reuses_shared_bulk_cache_singleton(
    tmp_cache_root: Path,
) -> None:
    """cache.invalidate uses Source._shared_bulk_cache when base_dir matches."""
    from oda_data import CRSData
    from oda_data.api.sources import Source
    from oda_data.tools.cache import BulkCacheManager

    # Prime the singleton by accessing the bulk_cache property.
    _ = CRSData().bulk_cache
    assert Source._shared_bulk_cache is not None, "Singleton should be set after access"

    with patch.object(
        BulkCacheManager,
        "__init__",
        autospec=True,
        side_effect=BulkCacheManager.__init__,
    ) as init_spy:
        cache.invalidate("CRSData")

    assert init_spy.call_count == 0, (
        f"BulkCacheManager.__init__ was called {init_spy.call_count} time(s) — "
        "expected 0 (singleton should have been reused)"
    )


def test_invalidate_constructs_when_no_singleton(
    tmp_cache_root: Path,
) -> None:
    """cache.invalidate constructs a fresh BulkCacheManager when singleton is None."""
    from oda_data import CRSData  # noqa: F401 — register subclass
    from oda_data.api.sources import Source
    from oda_data.tools.cache import BulkCacheManager

    # Ensure no singleton is set.
    Source._shared_bulk_cache = None

    with patch.object(
        BulkCacheManager,
        "__init__",
        autospec=True,
        side_effect=BulkCacheManager.__init__,
    ) as init_spy:
        cache.invalidate("CRSData")

    assert init_spy.call_count == 1, (
        f"BulkCacheManager.__init__ was called {init_spy.call_count} time(s) — "
        "expected exactly 1 (fallback construction)"
    )


def test_tmp_suffix_uses_host_and_pid(tmp_cache_root: Path) -> None:
    """Temp files use .tmp-<hostname>-<pid> suffix."""
    from oda_data.tools.cache import BulkCacheEntry, BulkCacheManager

    bulk_dir = tmp_cache_root / "oda-data"

    seen_paths: list[Path] = []

    def _capturing_fetcher(target_path: Path) -> None:
        seen_paths.append(target_path)
        pd.DataFrame({"year": [2022], "value": [1.0]}).to_parquet(
            target_path, engine="pyarrow"
        )

    fake_host = "alice"
    fake_pid = 1234

    with (
        patch("oda_data.tools.cache._HOSTNAME", fake_host),
        patch("os.getpid", return_value=fake_pid),
    ):
        mgr = BulkCacheManager(bulk_dir)
        entry = BulkCacheEntry(
            key="CRSData_bulk",
            fetcher=_capturing_fetcher,
            ttl_days=30,
        )
        mgr.ensure(entry)

    assert seen_paths, "Fetcher was never called"
    tmp_name = seen_paths[0].name
    expected_suffix = f"tmp-{fake_host}-{fake_pid}"
    assert expected_suffix in tmp_name, (
        f"Tmp filename {tmp_name!r} should contain {expected_suffix!r}"
    )


# ---------------------------------------------------------------------------
# F-8: ODA_DATA_CACHE_DIR relative path is resolved to absolute
# ---------------------------------------------------------------------------


def test_env_var_relative_path_is_resolved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_resolve_cache_root resolves a relative ODA_DATA_CACHE_DIR to an absolute path.

    Uses monkeypatch.chdir + monkeypatch.setenv so the test is self-contained.
    Explicitly clears _cache_root_override so the env-var branch is reached
    (defensive until Slice C autouse fixture is in place).
    """
    import oda_data.cache.config as cfg

    # Explicit override reset — guards against state leakage from prior tests
    # that called set_cache_root (via tmp_cache_root fixture).  Slice C's
    # autouse fixture will make this redundant once it lands.
    monkeypatch.setattr(cfg, "_cache_root_override", None)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ODA_DATA_CACHE_DIR", "./relative_cache")

    result = cache.path()
    assert result == (tmp_path / "relative_cache").resolve(), (
        f"Expected absolute resolved path, got {result!r}"
    )
