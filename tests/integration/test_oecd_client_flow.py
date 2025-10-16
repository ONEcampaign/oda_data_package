"""
Integration tests for OECDClient end-to-end workflows.

This module tests complete data retrieval workflows through the OECDClient,
from initialization through indicator retrieval with all processing steps,
using mocked external dependencies.
"""

from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from oda_data.api.oecd import OECDClient


# ============================================================================
# Integration Tests - Complete Workflows
# ============================================================================


@pytest.mark.integration
class TestOECDClientFullWorkflow:
    """Integration tests for complete OECDClient workflows."""

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    @patch("oda_data.clean_data.common.oecd_dac_exchange")
    def test_full_workflow_single_dac1_indicator(
        self,
        mock_exchange,
        mock_readers,
        mock_load_indicators,
        sample_dac1_df
    ):
        """Test complete workflow for retrieving a single DAC1 indicator.

        This integration test validates:
        1. Client initialization
        2. Filter application
        3. Data loading from source
        4. Custom processing
        5. Unit conversion
        6. Result assembly
        """
        # Mock indicator configuration
        mock_load_indicators.return_value = {
            "DAC1.TEST": {
                "sources": ["DAC1"],
                "filters": {
                    "DAC1": {"flowtype_code": ["in", [10]]}
                },
                "custom_function": ""
            }
        }

        # Mock DAC1Data reader
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac1_df.copy()
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        # Mock currency exchange
        def exchange_side_effect(data, **kwargs):
            df = data.copy()
            df["currency"] = kwargs.get("target_currency", "EUR")
            return df

        mock_exchange.side_effect = exchange_side_effect

        # Create client and get indicator
        client = OECDClient(
            years=[2020, 2021],
            providers=[1, 2],
            currency="EUR",
            base_year=None
        )

        result = client.get_indicators("DAC1.TEST")

        # Verify result structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert "one_indicator" in result.columns
        assert (result["one_indicator"] == "DAC1.TEST").all()

        # Verify currency conversion was applied
        assert "currency" in result.columns
        assert (result["currency"] == "EUR").all()

        # Verify reader was called with correct parameters
        mock_reader_class.assert_called_once()
        call_kwargs = mock_reader_class.call_args[1]
        assert call_kwargs["years"] == [2020, 2021]
        assert call_kwargs["providers"] == [1, 2]

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    @patch("oda_data.clean_data.common.oecd_dac_deflate")
    def test_full_workflow_with_base_year_deflation(
        self,
        mock_deflate,
        mock_readers,
        mock_load_indicators,
        sample_dac2a_df
    ):
        """Test complete workflow with constant price conversion.

        This validates that base_year parameter triggers deflation.
        """
        mock_load_indicators.return_value = {
            "DAC2A.TEST": {
                "sources": ["DAC2A"],
                "filters": {"DAC2A": {}},
                "custom_function": ""
            }
        }

        # Mock DAC2AData reader
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac2a_df.copy()
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        # Mock deflation
        def deflate_side_effect(data, **kwargs):
            df = data.copy()
            df["currency"] = kwargs.get("target_currency", "USD")
            df["prices"] = "constant"
            return df

        mock_deflate.side_effect = deflate_side_effect

        # Create client with base_year
        client = OECDClient(
            years=[2020, 2021],
            providers=[1],
            recipients=[100],
            currency="USD",
            base_year=2020
        )

        result = client.get_indicators("DAC2A.TEST")

        # Verify deflation was applied
        mock_deflate.assert_called_once()
        assert "prices" in result.columns
        assert (result["prices"] == "constant").all()

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_full_workflow_multiple_indicators_from_different_sources(
        self,
        mock_readers,
        mock_load_indicators,
        sample_dac1_df,
        sample_dac2a_df
    ):
        """Test workflow with multiple indicators from different data sources.

        This validates that the client can handle indicators from DAC1 and
        DAC2A simultaneously and combine results correctly.
        """
        mock_load_indicators.return_value = {
            "DAC1.IND": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}},
                "custom_function": ""
            },
            "DAC2A.IND": {
                "sources": ["DAC2A"],
                "filters": {"DAC2A": {}},
                "custom_function": ""
            }
        }

        # Mock both readers
        def reader_side_effect(source):
            if source == "DAC1":
                instance = MagicMock()
                instance.read.return_value = sample_dac1_df.copy()
                return MagicMock(return_value=instance)
            elif source == "DAC2A":
                instance = MagicMock()
                instance.read.return_value = sample_dac2a_df.copy()
                return MagicMock(return_value=instance)

        mock_readers.__getitem__.side_effect = lambda key: reader_side_effect(key)()

        client = OECDClient(years=[2020, 2021])
        result = client.get_indicators(["DAC1.IND", "DAC2A.IND"])

        # Result should combine both indicators
        assert isinstance(result, pd.DataFrame)
        assert "one_indicator" in result.columns

        indicators_present = result["one_indicator"].unique()
        assert "DAC1.IND" in indicators_present
        assert "DAC2A.IND" in indicators_present

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    @patch("oda_data.indicators.dac1.dac1_functions.official_oda")
    def test_full_workflow_with_custom_function(
        self,
        mock_custom_function,
        mock_readers,
        mock_load_indicators,
        sample_dac1_df
    ):
        """Test workflow that includes custom processing function.

        This validates that custom indicator functions are called during
        the processing pipeline.
        """
        mock_load_indicators.return_value = {
            "DAC1.ODA": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}},
                "custom_function": "official_oda"
            }
        }

        # Mock reader
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac1_df.copy()
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        # Mock custom function
        processed_df = sample_dac1_df.copy()
        processed_df["processed"] = True
        mock_custom_function.return_value = processed_df

        client = OECDClient()
        result = client.get_indicators("DAC1.ODA")

        # Custom function should have been called
        mock_custom_function.assert_called_once()

        # Result should have been processed
        assert "processed" in result.columns


# ============================================================================
# Integration Tests - Error Handling
# ============================================================================


@pytest.mark.integration
class TestOECDClientErrorHandling:
    """Integration tests for error handling in workflows."""

    @patch("oda_data.api.oecd.load_indicators")
    def test_invalid_indicator_code_raises_error(self, mock_load_indicators):
        """Test that requesting non-existent indicator raises appropriate error."""
        mock_load_indicators.return_value = {
            "VALID.INDICATOR": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}}
            }
        }

        client = OECDClient()

        # Requesting invalid indicator should raise KeyError
        with pytest.raises(KeyError):
            client.get_indicators("INVALID.INDICATOR")

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_reader_failure_propagates_error(
        self,
        mock_readers,
        mock_load_indicators
    ):
        """Test that errors from data readers propagate correctly."""
        mock_load_indicators.return_value = {
            "TEST.IND": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}}
            }
        }

        # Mock reader that raises an error
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.side_effect = ConnectionError("API unavailable")
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        client = OECDClient()

        with pytest.raises(ConnectionError, match="API unavailable"):
            client.get_indicators("TEST.IND")


# ============================================================================
# Integration Tests - Data Filtering
# ============================================================================


@pytest.mark.integration
class TestOECDClientDataFiltering:
    """Integration tests for data filtering across the workflow."""

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_year_filtering_applied_to_reader(
        self,
        mock_readers,
        mock_load_indicators,
        sample_dac1_df
    ):
        """Test that year filters are correctly passed to data readers."""
        mock_load_indicators.return_value = {
            "TEST": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}},
                "custom_function": ""
            }
        }

        mock_reader_instance = MagicMock()
        # Return only 2020 data
        filtered_df = sample_dac1_df[sample_dac1_df["year"] == 2020].copy()
        mock_reader_instance.read.return_value = filtered_df
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        client = OECDClient(years=[2020])
        result = client.get_indicators("TEST")

        # Verify years parameter was passed
        call_kwargs = mock_reader_class.call_args[1]
        assert call_kwargs["years"] == [2020]

        # Result should only contain 2020 data
        assert (result["year"] == 2020).all()

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_provider_filtering_applied_to_reader(
        self,
        mock_readers,
        mock_load_indicators,
        sample_dac1_df
    ):
        """Test that provider filters are correctly passed to data readers."""
        mock_load_indicators.return_value = {
            "TEST": {
                "sources": ["DAC1"],
                "filters": {"DAC1": {}},
                "custom_function": ""
            }
        }

        mock_reader_instance = MagicMock()
        # Return only provider 1 data
        filtered_df = sample_dac1_df[sample_dac1_df["provider_code"] == 1].copy()
        mock_reader_instance.read.return_value = filtered_df
        mock_reader_class = MagicMock(return_value=mock_reader_instance)
        mock_readers.__getitem__.return_value = mock_reader_class

        client = OECDClient(providers=[1])
        result = client.get_indicators("TEST")

        # Verify providers parameter was passed
        call_kwargs = mock_reader_class.call_args[1]
        assert call_kwargs["providers"] == [1]

        # Result should only contain provider 1
        assert (result["provider_code"] == 1).all()
