"""Network integration tests for oda_data bulk download.

These tests require live network access and are skipped unless
RUN_NETWORK_TESTS=1 is set in the environment.

Run with:
    RUN_NETWORK_TESTS=1 uv run pytest tests/cache/test_network_integration.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip(
    "oda_data.cache",
    reason="requires slice (b): oda_data.cache not yet available",
)


@pytest.mark.network
def test_crs_bulk_download_returns_nonempty(
    tmp_cache_root: Path, skip_if_no_network: None
) -> None:
    """CRSData.read(using_bulk_download=True) returns a non-empty DataFrame."""
    from oda_data import CRSData

    df = CRSData(years=[2022]).read(using_bulk_download=True)
    assert df is not None
    assert len(df) > 0
