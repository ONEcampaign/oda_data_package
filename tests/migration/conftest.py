"""Shared fixtures for oda_data migration tests."""

from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture()
def tmp_old_layout(tmp_path: Path) -> Path:
    """A pre-2.6 cache layout under tmp_path for migration tests.

    Creates the CWD ``.raw_data`` directory structure that the migration
    scanner expects to find, containing a minimal parquet file in each
    sub-directory.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Path: Root of the old layout (``tmp_path / ".raw_data"``).
    """
    old_root = tmp_path / ".raw_data"
    # Build once; written into each sub-directory so migration has something to move.
    seed_df = pd.DataFrame({"year": [2022], "value": [1.0]})
    for sub in ("http_cache", "bulk_cache", "query_cache", "bulk_files"):
        sub_dir = old_root / sub
        sub_dir.mkdir(parents=True, exist_ok=True)
        seed_df.to_parquet(sub_dir / "seed.parquet", engine="pyarrow")
    return old_root


@pytest.fixture()
def synced_drive_marker_factory(tmp_path: Path):
    """Factory that plants a synced-drive marker inside tmp_path.

    Usage::

        def test_dropbox(synced_drive_marker_factory):
            old_root = synced_drive_marker_factory(".dropbox")
            # old_root is now the simulated old cache dir whose ancestor
            # contains the .dropbox marker.

    Parametrize over the marker names from ``_is_synced_drive``'s
    authoritative list:

    - ``.dropbox``
    - ``.dropbox.cache``
    - ``com.apple.iCloud``
    - a path segment named ``OneDrive``
    - a path segment matching ``OneDrive - Personal``
    - a path segment matching ``OneDrive - Some Tenant``

    iCloud's ``~/Library/Mobile Documents/`` case is handled by path
    rather than a file marker; use ``synced_drive_marker_factory("mobile_documents")``
    to create a path under a synthetic ``Mobile Documents`` ancestor.

    Args:
        tmp_path: pytest temporary directory.

    Returns:
        Callable[[str], Path]: Factory(marker_name) -> old_cache_root.
    """

    def _factory(marker_name: str) -> Path:
        """Plant *marker_name* as an ancestor of the returned old-cache root.

        Args:
            marker_name: One of the known marker names, or ``'mobile_documents'``
                to simulate an iCloud path.

        Returns:
            Path: Old cache root with the marker already in place.
        """
        if marker_name == "mobile_documents":
            # Simulate ~/Library/Mobile Documents/ by nesting under that name.
            ancestor = tmp_path / "Library" / "Mobile Documents" / "app"
        elif marker_name in (
            "OneDrive",
            "OneDrive - Personal",
            "OneDrive - Some Tenant",
        ):
            # OneDrive is detected by path-segment name, not a file marker.
            ancestor = tmp_path / marker_name / "data"
        else:
            # File-based markers (.dropbox, .dropbox.cache, com.apple.iCloud).
            ancestor = tmp_path / "synced_parent"
            ancestor.mkdir(parents=True, exist_ok=True)
            (ancestor / marker_name).touch()

        ancestor.mkdir(parents=True, exist_ok=True)
        old_root = ancestor / "old_cache"
        old_root.mkdir(parents=True, exist_ok=True)
        return old_root

    return _factory
