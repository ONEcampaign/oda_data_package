"""Multi-process race test for bulk-cache FileLock serialisation.

The fake fetcher is defined at module scope so it pickles cleanly under
``multiprocessing.get_context("spawn")``.
"""

from __future__ import annotations

import io
import multiprocessing as mp
import os
import time
import zipfile
from pathlib import Path

import pytest

# Guard: slice (b)/(c) must land before this test can run.
pytest.importorskip(
    "oda_data.cache",
    reason="requires slices (b)/(c): oda_data.cache not yet available",
)

# ---------------------------------------------------------------------------
# Module-scope fake fetcher — must live here so spawn workers can pickle it.
# ---------------------------------------------------------------------------

# Initialised by the pool initializer; stays None until workers start.
_COUNTER: mp.sharedctypes.Synchronized[int] | None = None  # type: ignore[assignment]


def _slow_fetcher(target_path: Path) -> None:
    """Write a 1 KB valid zip and increment the shared counter.

    Sleeps 200 ms to make concurrent calls overlap, maximising the chance
    of a race condition if FileLock is not held correctly.

    Args:
        target_path: Destination path for the fake zip.
    """
    time.sleep(0.2)
    if _COUNTER is not None:
        with _COUNTER.get_lock():
            _COUNTER.value += 1
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("crs.parquet", b"PAR1" + b"\x00" * 1000)
    target_path.write_bytes(buf.getvalue())


def _init_worker(counter: mp.sharedctypes.Synchronized[int], cache_root: Path) -> None:
    """Pool initialiser — sets per-worker globals before the first task runs.

    Args:
        counter: Shared integer counter tracking fetch invocations.
        cache_root: Path to the temporary cache root; written to
            ODA_DATA_CACHE_DIR so each worker uses the shared temp dir.
    """
    global _COUNTER
    _COUNTER = counter
    os.environ["ODA_DATA_CACHE_DIR"] = str(cache_root)


def _worker_call(_unused: int) -> None:
    """Worker task: call BulkCacheManager.ensure with a slow fake fetcher.

    Drives the cache-manager layer directly so we exercise the FileLock
    serialisation contract without going through the network-discovery layer.

    Args:
        _unused: Pool.map index; not used.
    """
    from pathlib import Path

    from oda_data.tools.cache import BulkCacheEntry, BulkCacheManager

    cache_root = Path(os.environ["ODA_DATA_CACHE_DIR"])
    mgr = BulkCacheManager(cache_root / "oda-data")
    entry = BulkCacheEntry(key="CRSData_bulk", fetcher=_slow_fetcher, ttl_days=30)
    mgr.ensure(entry)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


def test_four_workers_one_fetch(tmp_path: Path) -> None:
    """Four concurrent workers trigger exactly one network fetch (FileLock test)."""
    ctx = mp.get_context("spawn")
    counter = ctx.Value("i", 0)
    # Set ODA_DATA_CACHE_DIR before pool start so spawned children inherit it via
    # the environment copy that spawn makes for each new process.
    prev = os.environ.get("ODA_DATA_CACHE_DIR")
    os.environ["ODA_DATA_CACHE_DIR"] = str(tmp_path)
    try:
        with ctx.Pool(
            4, initializer=_init_worker, initargs=(counter, tmp_path)
        ) as pool:
            pool.map(_worker_call, range(4))
    finally:
        if prev is None:
            os.environ.pop("ODA_DATA_CACHE_DIR", None)
        else:
            os.environ["ODA_DATA_CACHE_DIR"] = prev
    assert counter.value == 1, (
        f"Expected exactly 1 fetch across 4 workers, got {counter.value}"
    )
