"""
Shared pytest fixtures and configuration for oda-data test suite.

This module provides reusable fixtures for testing the oda-data package,
including sample DataFrames, mock objects, and test utilities.
"""

from pathlib import Path
from typing import Callable

import pandas as pd
import pytest
from unittest.mock import MagicMock


# ============================================================================
# Sample DataFrame Fixtures
# ============================================================================


@pytest.fixture
def sample_dac1_df() -> pd.DataFrame:
    """Sample DAC1 DataFrame with typical structure.

    Contains:
    - Multiple years (2019-2021) to test pre/post-2018 logic
    - Multiple providers
    - Different aid types (including ODA codes)
    - Realistic value ranges

    Returns:
        pd.DataFrame: Sample DAC1 data
    """
    return pd.DataFrame({
        "year": [2019, 2019, 2020, 2020, 2021, 2021],
        "provider_code": [1, 2, 1, 2, 1, 2],
        "provider_name": ["Donor A", "Donor B", "Donor A", "Donor B", "Donor A", "Donor B"],
        "aidtype_code": [1010, 1010, 1010, 1010, 11010, 11010],
        "aidtype_name": ["ODA", "ODA", "ODA", "ODA", "ODA GE", "ODA GE"],
        "flow_type_code": [10, 10, 10, 10, 10, 10],
        "value": [1000.0, 2000.0, 1500.0, 2500.0, 1800.0, 2800.0],
        "currency": ["USD", "USD", "USD", "USD", "USD", "USD"],
        "prices": ["current", "current", "current", "current", "current", "current"],
    })


@pytest.fixture
def sample_dac2a_df() -> pd.DataFrame:
    """Sample DAC2A DataFrame with bilateral flow structure.

    Contains:
    - Provider-recipient pairs
    - Multiple years
    - Different aid types
    - Recipient codes and names

    Returns:
        pd.DataFrame: Sample DAC2A data
    """
    return pd.DataFrame({
        "year": [2020, 2020, 2020, 2021, 2021, 2021],
        "provider_code": [1, 1, 2, 1, 1, 2],
        "provider_name": ["Donor A", "Donor A", "Donor B", "Donor A", "Donor A", "Donor B"],
        "recipient_code": [100, 200, 100, 100, 200, 100],
        "recipient_name": ["Country X", "Country Y", "Country X", "Country X", "Country Y", "Country X"],
        "aidtype_code": [1010, 1010, 1010, 11010, 11010, 11010],
        "aidtype_name": ["ODA", "ODA", "ODA", "ODA GE", "ODA GE", "ODA GE"],
        "value": [500.0, 750.0, 600.0, 550.0, 800.0, 650.0],
        "currency": ["USD", "USD", "USD", "USD", "USD", "USD"],
        "prices": ["current", "current", "current", "current", "current", "current"],
    })


@pytest.fixture
def sample_crs_df() -> pd.DataFrame:
    """Sample CRS DataFrame with project-level structure.

    Contains:
    - Project-level detail
    - Multiple sectors
    - Different flow types
    - Provider-recipient-sector combinations

    Returns:
        pd.DataFrame: Sample CRS data
    """
    return pd.DataFrame({
        "year": [2020, 2020, 2020, 2021, 2021, 2021],
        "provider_code": [1, 1, 2, 1, 1, 2],
        "provider_name": ["Donor A", "Donor A", "Donor B", "Donor A", "Donor A", "Donor B"],
        "recipient_code": [100, 100, 200, 100, 100, 200],
        "recipient_name": ["Country X", "Country X", "Country Y", "Country X", "Country X", "Country Y"],
        "sector_code": [110, 120, 110, 110, 130, 120],
        "sector_name": ["Education", "Health", "Education", "Education", "Population", "Health"],
        "purpose_code": [11220, 12110, 11220, 11220, 13010, 12110],
        "purpose_name": ["Primary education", "Health policy", "Primary education", "Primary education", "Population policy", "Health policy"],
        "flow_type_code": [10, 10, 10, 10, 10, 10],
        "commitment_current": [100.0, 150.0, 120.0, 110.0, 160.0, 130.0],
        "disbursement_current": [80.0, 120.0, 100.0, 90.0, 140.0, 110.0],
        "grant_equivalent": [75.0, 115.0, 95.0, 85.0, 135.0, 105.0],
        "currency": ["USD", "USD", "USD", "USD", "USD", "USD"],
        "prices": ["current", "current", "current", "current", "current", "current"],
    })


@pytest.fixture
def sample_multisystem_df() -> pd.DataFrame:
    """Sample MultiSystem DataFrame.

    Contains:
    - Providers' use of multilateral system
    - Multiple years
    - Different channels

    Returns:
        pd.DataFrame: Sample MultiSystem data
    """
    return pd.DataFrame({
        "Year": [2020, 2020, 2021, 2021],  # Note: PascalCase for MultiSystem
        "ProviderCode": [1, 2, 1, 2],
        "ProviderName": ["Donor A", "Donor B", "Donor A", "Donor B"],
        "ChannelCode": [1000, 1000, 1000, 1000],
        "ChannelName": ["World Bank", "World Bank", "World Bank", "World Bank"],
        "Amount": [5000.0, 7000.0, 5500.0, 7500.0],
    })


@pytest.fixture
def sample_gni_df() -> pd.DataFrame:
    """Sample GNI data for ODA/GNI calculations.

    Returns:
        pd.DataFrame: Sample GNI data
    """
    return pd.DataFrame({
        "year": [2019, 2020, 2021],
        "provider_code": [1, 1, 1],
        "aidtype_code": [1, 1, 1],  # GNI code
        "value": [1_000_000.0, 1_100_000.0, 1_200_000.0],
    })


# ============================================================================
# Path and Directory Fixtures
# ============================================================================


@pytest.fixture
def temp_cache_dir(tmp_path: Path) -> Path:
    """Create a temporary cache directory for testing.

    Args:
        tmp_path: Pytest's built-in temporary directory fixture

    Returns:
        Path: Temporary cache directory
    """
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


@pytest.fixture
def temp_data_path(tmp_path: Path) -> Path:
    """Create a temporary data directory for testing.

    Args:
        tmp_path: Pytest's built-in temporary directory fixture

    Returns:
        Path: Temporary data directory
    """
    data_dir = tmp_path / "test_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


# ============================================================================
# Mock External Dependencies
# ============================================================================


@pytest.fixture
def mock_oda_reader(mocker):
    """Mock oda-reader bulk download functions.

    Provides mocks for:
    - bulk_download_crs
    - bulk_download_multisystem
    - download_aiddata

    Returns:
        dict: Dictionary of mocked functions
    """
    return {
        "bulk_download_crs": mocker.patch("oda_reader.bulk_download_crs"),
        "bulk_download_multisystem": mocker.patch("oda_reader.bulk_download_multisystem"),
        "download_aiddata": mocker.patch("oda_reader.download_aiddata"),
    }


@pytest.fixture
def mock_pydeflate(mocker):
    """Mock pydeflate currency conversion and deflation functions.

    Provides mocks for:
    - oecd_dac_exchange
    - oecd_dac_deflate
    - set_pydeflate_path

    Returns:
        dict: Dictionary of mocked functions
    """
    # Mock exchange - returns data with currency column added
    def mock_exchange(data, **kwargs):
        df = data.copy()
        df["currency"] = kwargs.get("target_currency", "EUR")
        return df

    # Mock deflate - returns data with currency and prices columns
    def mock_deflate(data, **kwargs):
        df = data.copy()
        df["currency"] = kwargs.get("target_currency", "USD")
        df["prices"] = "constant"
        return df

    return {
        "exchange": mocker.patch(
            "oda_data.clean_data.common.oecd_dac_exchange",
            side_effect=mock_exchange
        ),
        "deflate": mocker.patch(
            "oda_data.clean_data.common.oecd_dac_deflate",
            side_effect=mock_deflate
        ),
        "set_path": mocker.patch(
            "oda_data.clean_data.common.set_pydeflate_path"
        ),
    }


# ============================================================================
# Cache Mock Fixtures
# ============================================================================


@pytest.fixture
def mock_bulk_fetcher(tmp_path: Path) -> Callable[[Path], None]:
    """Create a mock bulk fetcher function for testing BulkCacheManager.

    The mock fetcher creates a simple parquet file at the target path.

    Args:
        tmp_path: Pytest's temporary path fixture

    Returns:
        Callable: Mock fetcher function
    """
    def fetcher(target_path: Path):
        """Mock fetcher that creates a simple parquet file."""
        df = pd.DataFrame({
            "year": [2020, 2021],
            "provider_code": [1, 2],
            "value": [1000.0, 2000.0],
        })
        df.to_parquet(target_path, engine="pyarrow")

    return fetcher


# ============================================================================
# Test Utility Functions
# ============================================================================


def assert_dataframes_equal(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    check_dtype: bool = True,
    check_column_type: bool = True,
    check_index_type: bool = False,
    rtol: float = 1e-5,
) -> None:
    """Assert that two DataFrames are equal with flexible comparison options.

    This is a convenience wrapper around pd.testing.assert_frame_equal with
    sensible defaults for most test cases.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        check_dtype: Whether to check dtype equivalence
        check_column_type: Whether to check column type
        check_index_type: Whether to check index type
        rtol: Relative tolerance for numeric comparisons

    Raises:
        AssertionError: If DataFrames are not equal
    """
    pd.testing.assert_frame_equal(
        df1,
        df2,
        check_dtype=check_dtype,
        check_column_type=check_column_type,
        check_index_type=check_index_type,
        rtol=rtol,
    )


def assert_series_equal(
    s1: pd.Series,
    s2: pd.Series,
    check_dtype: bool = True,
    rtol: float = 1e-5,
) -> None:
    """Assert that two Series are equal with flexible comparison options.

    Args:
        s1: First Series
        s2: Second Series
        check_dtype: Whether to check dtype equivalence
        rtol: Relative tolerance for numeric comparisons

    Raises:
        AssertionError: If Series are not equal
    """
    pd.testing.assert_series_equal(
        s1,
        s2,
        check_dtype=check_dtype,
        rtol=rtol,
    )


# ============================================================================
# Pytest Configuration Hooks
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom settings.

    Adds custom markers defined in pytest.ini.
    """
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
