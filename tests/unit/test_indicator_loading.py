"""
Tests for indicator loading and structure validation.

This module tests the logic for loading indicator definitions from JSON files
and validating their structure.
"""

from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from oda_data.api.oecd import load_indicators

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_dac1_indicators():
    """Sample DAC1 indicator definitions."""
    return {
        "DAC1.TEST1": {
            "name": "Test DAC1 Indicator 1",
            "description": "A test indicator",
            "sources": ["DAC1"],
            "filters": {"DAC1": {"flowtype_code": ["in", [10]]}},
            "custom_function": "",
        },
        "DAC1.TEST2": {
            "name": "Test DAC1 Indicator 2",
            "description": "Another test indicator",
            "sources": ["DAC1"],
            "filters": {"DAC1": {}},
            "custom_function": "official_oda",
        },
    }


@pytest.fixture
def sample_dac2a_indicators():
    """Sample DAC2A indicator definitions."""
    return {
        "DAC2A.TEST1": {
            "name": "Test DAC2A Indicator",
            "description": "A test indicator",
            "sources": ["DAC2A"],
            "filters": {"DAC2A": {}},
            "custom_function": "",
        },
    }


@pytest.fixture
def sample_crs_indicators():
    """Sample CRS indicator definitions."""
    return {
        "CRS.TEST1": {
            "name": "Test CRS Indicator",
            "description": "A test indicator",
            "sources": ["CRS"],
            "filters": {"CRS": {}},
            "custom_function": "",
        },
    }


# ============================================================================
# Tests for load_indicators
# ============================================================================


class TestLoadIndicators:
    """Tests for the load_indicators function."""

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("oda_data.api.oecd.config")
    def test_load_indicators_all_codes_are_strings(
        self,
        mock_config,
        mock_json_load,
        mock_file,
        sample_dac1_indicators,
        sample_dac2a_indicators,
        sample_crs_indicators,
    ):
        """Test that all indicator codes are strings."""
        mock_config.ODAPaths = type(
            "obj", (object,), {"indicators": Path("/mock/indicators")}
        )()

        mock_json_load.side_effect = [
            sample_dac1_indicators,
            sample_dac2a_indicators,
            sample_crs_indicators,
        ]

        result = load_indicators()

        # All keys should be strings
        assert all(isinstance(key, str) for key in result)


# ============================================================================
# Tests for Indicator Structure Validation
# ============================================================================


class TestIndicatorStructureValidation:
    """Tests for validating indicator structure."""

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    @patch("oda_data.api.oecd.config")
    def test_custom_function_is_string_or_empty(
        self,
        mock_config,
        mock_json_load,
        mock_file,
        sample_dac1_indicators,
    ):
        """Test that custom_function field is always a string."""
        mock_config.ODAPaths = type(
            "obj", (object,), {"indicators": Path("/mock/indicators")}
        )()

        mock_json_load.side_effect = [
            sample_dac1_indicators,
            {},  # Empty DAC2A
            {},  # Empty CRS
        ]

        result = load_indicators()

        for indicator_code, indicator_data in result.items():
            if "custom_function" in indicator_data:
                assert isinstance(indicator_data["custom_function"], str), (
                    f"Indicator {indicator_code} has non-string custom_function"
                )
