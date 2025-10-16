"""
Tests for v1.x compatibility layer.

This module tests the ODAData class which provides backward compatibility
for users migrating from version 1.x to 2.x of the package.
"""

from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest

from oda_data.tools.compatibility import ODAData

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_v1_mapping():
    """Sample v1 to v2 indicator mapping."""
    return {
        "total_oda_official_definition": {
            "indicator": "total_oda_flow_net",
            "measure": "disbursement",
            "bulk": True,
        },
        "total_oda_grant_equiv": {
            "indicator": "oda_ge_flow_net",
            "measure": "grant_equivalent",
            "bulk": False,
        },
    }


# ============================================================================
# Tests for ODAData Initialization
# ============================================================================


class TestODADataInitialization:
    """Tests for ODAData class initialization."""

    @patch("builtins.open", new_callable=mock_open, read_data='{"test": {}}')
    @patch("json.load")
    def test_odadata_init_with_all_parameters(self, mock_json_load, mock_file):
        """Test initialization with all parameters."""
        mock_json_load.return_value = {}

        with pytest.warns(DeprecationWarning, match="partial compatibility"):
            oda_data = ODAData(
                years=[2020, 2021],
                donors=[1, 2, 3],
                recipients=[100, 200],
                currency="EUR",
                prices="constant",
                base_year=2020,
                use_bulk_download=True,
            )

        assert oda_data.years == [2020, 2021]
        assert oda_data.donors == [1, 2, 3]
        assert oda_data.recipients == [100, 200]
        assert oda_data.currency == "EUR"
        assert oda_data.prices == "constant"
        assert oda_data.base_year == 2020
        assert oda_data.use_bulk_download is True

    @patch("builtins.open", new_callable=mock_open, read_data='{"test": {}}')
    @patch("json.load")
    def test_odadata_init_raises_deprecation_warning(self, mock_json_load, mock_file):
        """Test that DeprecationWarning is raised on initialization."""
        mock_json_load.return_value = {}

        with pytest.warns(DeprecationWarning) as warning_list:
            ODAData(years=[2020])

        # Should have raised exactly one warning
        assert len(warning_list) == 1
        assert "partial compatibility" in str(warning_list[0].message)


# ============================================================================
# Tests for load_indicator Method
# ============================================================================


class TestODADataLoadIndicator:
    """Tests for the load_indicator method."""

    @patch("oda_data.tools.compatibility.OECDClient")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    def test_load_indicator_single_indicator_maps_to_v2(
        self, mock_json_load, mock_file, mock_oecd_client, sample_v1_mapping
    ):
        """Test loading single indicator maps to new v2 indicators."""
        mock_json_load.return_value = sample_v1_mapping

        # Mock OECDClient
        mock_client_instance = MagicMock()
        mock_client_instance.get_indicators.return_value = pd.DataFrame(
            {
                "year": [2020],
                "value": [1000.0],
            }
        )
        mock_oecd_client.return_value = mock_client_instance

        with pytest.warns(DeprecationWarning):
            oda_data = ODAData(years=[2020])

        # Load a v1 indicator
        oda_data.load_indicator("total_oda_official_definition")

        # Should have called OECDClient with correct v2 indicator
        mock_oecd_client.assert_called_once()
        call_kwargs = mock_oecd_client.call_args[1]
        assert call_kwargs["measure"] == "disbursement"
        assert call_kwargs["use_bulk_download"] is True

        # Should have stored the indicator
        assert "total_oda_official_definition" in oda_data.indicators

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    def test_load_indicator_raises_error_for_unsupported_indicator(
        self, mock_json_load, mock_file, sample_v1_mapping
    ):
        """Test that KeyError is raised for unsupported v1 indicators."""
        mock_json_load.return_value = sample_v1_mapping

        with pytest.warns(DeprecationWarning):
            oda_data = ODAData(years=[2020])

        # Try to load an indicator not in the mapping
        with pytest.raises(KeyError, match="not supported"):
            oda_data.load_indicator("unsupported_indicator")


# ============================================================================
# Tests for get_data Method
# ============================================================================


class TestODADataGetData:
    """Tests for the get_data method."""

    @patch("oda_data.tools.compatibility.OECDClient")
    @patch("builtins.open", new_callable=mock_open)
    @patch("json.load")
    def test_get_data_returns_concatenated_dataframe(
        self, mock_json_load, mock_file, mock_oecd_client, sample_v1_mapping
    ):
        """Test that get_data returns concatenated DataFrame of loaded indicators."""
        mock_json_load.return_value = sample_v1_mapping

        # Mock OECDClient to return different data for each indicator
        mock_client_instance = MagicMock()
        mock_client_instance.get_indicators.side_effect = [
            pd.DataFrame({"year": [2020], "value": [1000.0], "indicator": ["ind1"]}),
            pd.DataFrame({"year": [2021], "value": [2000.0], "indicator": ["ind2"]}),
        ]
        mock_oecd_client.return_value = mock_client_instance

        with pytest.warns(DeprecationWarning):
            oda_data = ODAData(years=[2020, 2021])

        # Load two indicators
        oda_data.load_indicator("total_oda_official_definition")
        oda_data.load_indicator("total_oda_grant_equiv")

        # Get all data
        result = oda_data.get_data("all")

        # Should concatenate both DataFrames
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "year" in result.columns
        assert "value" in result.columns
