"""Shared fixtures for oda_data cache tests."""

import io
import os
import zipfile
from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def tmp_cache_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Temporary cache root isolated from the real cache.

    Sets ODA_DATA_CACHE_DIR to a subdirectory of tmp_path so tests never
    touch the user's real platformdirs cache. The env var is restored on
    teardown by monkeypatch automatically.

    If slice (b) has landed, also calls ``set_cache_root`` so the module-level
    override is consistent with the env var.

    Args:
        monkeypatch: pytest monkeypatch fixture.
        tmp_path: pytest temporary directory.

    Yields:
        Path: Temporary cache root directory.
    """
    cache_dir = (tmp_path / "cache").resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("ODA_DATA_CACHE_DIR", str(cache_dir))

    # Best-effort: apply the programmatic override too, but don't fail if the
    # cache subpackage (slice (b)) has not landed yet.
    try:
        from oda_data.cache.config import set_cache_root

        set_cache_root(cache_dir)
    except ImportError:
        pass

    yield cache_dir


def _make_valid_zip(target: Path) -> None:
    """Write a ~1 KB valid zip containing one parquet-like entry."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("data.parquet", b"PAR1" + b"\x00" * 1000)
    target.write_bytes(buf.getvalue())


def _make_valid_parquet(target: Path) -> None:
    """Write a minimal parquet file to *target*."""
    df = pd.DataFrame({"year": [2022], "value": [1.0]})
    df.to_parquet(target, engine="pyarrow")


@pytest.fixture()
def valid_tiny_zip(tmp_path: Path) -> Path:
    """Path to a ~1 KB valid zip file with one parquet-like member.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path: Path to the valid zip file.
    """
    path = tmp_path / "valid.zip"
    _make_valid_zip(path)
    return path


@pytest.fixture()
def corrupt_zip_file(tmp_path: Path) -> Path:
    """Path to a non-zip 1 KB file for corruption-recovery tests.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path: Path to the corrupt (non-zip) file.
    """
    path = tmp_path / "corrupt.zip"
    path.write_bytes(b"NOT A ZIP" + b"\xff" * 1000)
    return path


@pytest.fixture()
def monkeypatched_fetcher():
    """Factory returning a callable that writes a 1 KB valid zip.

    The returned callable matches the ``fetcher(target_path: Path)``
    signature used by ``BulkCacheManager`` entries.

    Returns:
        Callable[[Path], None]: Fake fetcher writing a valid zip.
    """

    def _fetcher(target_path: Path) -> None:
        _make_valid_zip(target_path)

    return _fetcher


@pytest.fixture()
def populated_bulk_cache(tmp_cache_root: Path) -> Path:
    """Cache root pre-populated with one bulk parquet entry.

    Creates the directory layout expected by ``BulkCacheManager`` and
    places a minimal parquet file so cache-hit paths can be exercised
    without a real download.

    Args:
        tmp_cache_root: Fixture providing the temporary cache root.

    Returns:
        Path: Path to the bulk cache directory containing the seeded entry.
    """
    bulk_dir = tmp_cache_root / "oda-data" / "bulk_cache"
    bulk_dir.mkdir(parents=True, exist_ok=True)
    entry = bulk_dir / "CRSData_bulk.parquet"
    _make_valid_parquet(entry)
    return bulk_dir


@pytest.fixture()
def populated_query_cache(tmp_cache_root: Path) -> Path:
    """Cache root pre-populated with one query parquet entry.

    Creates the directory layout expected by ``QueryCacheManager`` and
    places a minimal parquet file so query-cache-hit paths can be
    exercised without a real download.

    Args:
        tmp_cache_root: Fixture providing the temporary cache root.

    Returns:
        Path: Path to the query cache directory containing the seeded entry.
    """
    query_dir = tmp_cache_root / "oda-data" / "query_cache"
    query_dir.mkdir(parents=True, exist_ok=True)
    entry = query_dir / "CRSData-abc123-v2.parquet"
    _make_valid_parquet(entry)
    return query_dir


@pytest.fixture()
def skip_if_no_network() -> None:
    """Skip the current test when RUN_NETWORK_TESTS is not set to '1'."""
    if os.environ.get("RUN_NETWORK_TESTS") != "1":
        pytest.skip("set RUN_NETWORK_TESTS=1 to run network tests")
