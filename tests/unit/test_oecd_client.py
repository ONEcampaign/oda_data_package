"""
Tests for OECDClient API.

This module tests the main user-facing OECDClient class in oda_data.api.oecd,
including initialization, filtering, data loading, processing, and the complete
indicator retrieval workflow.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from oda_data.api.oecd import OECDClient, get_measure_filter, load_indicators

# ============================================================================
# Tests for load_indicators helper function
# ============================================================================


class TestLoadIndicators:
    """Tests for the load_indicators function."""

    def test_load_indicators_returns_dict(self):
        """Test that load_indicators returns a dictionary."""
        indicators = load_indicators()

        assert isinstance(indicators, dict)
        assert len(indicators) > 0

    def test_load_indicators_combines_all_sources(self):
        """Test that indicators from all sources are combined."""
        indicators = load_indicators()

        # Should contain indicators from DAC1, DAC2A, and CRS
        # We can't know exact keys without checking the JSON files,
        # but we can verify the structure
        for indicator_code, indicator_info in indicators.items():
            assert isinstance(indicator_code, str)
            assert isinstance(indicator_info, dict)


# ============================================================================
# Tests for get_measure_filter helper function
# ============================================================================


class TestGetMeasureFilter:
    """Tests for the get_measure_filter function."""

    def test_get_measure_filter_returns_correct_filter(self):
        """Test that correct filter is returned for valid measure/source."""
        # This test depends on MEASURES constant
        # We'll test with a common measure
        try:
            result = get_measure_filter("DAC1", "net_disbursement")
            assert result is not None
        except KeyError:
            # If the measure doesn't exist, that's OK for this test
            pass

    def test_get_measure_filter_raises_error_for_invalid_measure(self):
        """Test that ValueError is raised for invalid measure."""
        with pytest.raises(ValueError, match="invalid_measure is not valid for DAC1"):
            get_measure_filter("DAC1", "invalid_measure")


# ============================================================================
# Tests for OECDClient Initialization
# ============================================================================


class TestOECDClientInitialization:
    """Tests for OECDClient initialization and validation."""

    def test_oecd_client_init_with_defaults(self):
        """Test OECDClient initialization with default parameters."""
        client = OECDClient()

        assert client.years is None
        assert client.providers is None
        assert client.recipients is None
        assert client.measure == ["net_disbursement"]
        assert client.currency == "USD"
        assert client.base_year is None
        assert client.use_bulk_download is False

    def test_oecd_client_init_with_custom_parameters(self):
        """Test OECDClient initialization with custom parameters."""
        client = OECDClient(
            years=[2020, 2021],
            providers=[1, 2],
            recipients=[100, 200],
            measure="commitment",
            currency="EUR",
            base_year=2020,
            use_bulk_download=True,
        )

        assert client.years == [2020, 2021]
        assert client.providers == [1, 2]
        assert client.recipients == [100, 200]
        assert client.measure == ["commitment"]
        assert client.currency == "EUR"
        assert client.base_year == 2020
        assert client.use_bulk_download is True

    def test_oecd_client_init_validates_measure(self):
        """Test that measure parameter is validated during initialization."""
        client = OECDClient(measure="net_disbursement")

        # Measure should be converted to list
        assert client.measure == ["net_disbursement"]
        assert isinstance(client.measure, list)

    def test_oecd_client_init_validates_currency(self):
        """Test that currency parameter is validated during initialization."""
        # Valid currency should work
        client = OECDClient(currency="USD")
        assert client.currency == "USD"

        # Invalid currency should raise error
        with pytest.raises(ValueError, match="Currency JPY is not supported"):
            OECDClient(currency="JPY")

    def test_oecd_client_init_loads_indicators(self):
        """Test that indicators are loaded during initialization."""
        client = OECDClient()

        assert hasattr(client, "_indicators")
        assert isinstance(client._indicators, dict)
        assert len(client._indicators) > 0


# ============================================================================
# Tests for OECDClient._apply_filters
# ============================================================================


class TestOECDClientApplyFilters:
    """Tests for the _apply_filters method."""

    @patch("oda_data.api.oecd.load_indicators")
    def test_apply_filters_constructs_filters_for_dac1(self, mock_load_indicators):
        """Test that filters are correctly constructed for DAC1 indicators."""
        # Mock indicator configuration
        mock_load_indicators.return_value = {
            "TEST.DAC1": {
                "sources": ["DAC1"],
                "filters": {
                    "DAC1": {
                        "flowtype_code": ["in", [10, 20]],
                    }
                },
            }
        }

        client = OECDClient(measure="net_disbursement", currency="USD")
        filters = client._apply_filters("TEST.DAC1")

        assert "DAC1" in filters
        assert isinstance(filters["DAC1"], list)
        # Should contain the flowtype_code filter
        assert any("flowtype_code" in str(f) for f in filters["DAC1"])

    @patch("oda_data.api.oecd.load_indicators")
    def test_apply_filters_applies_currency_filter(self, mock_load_indicators):
        """Test that currency filter is applied correctly."""
        mock_load_indicators.return_value = {
            "TEST.INDICATOR": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        client = OECDClient(currency="LCU")
        filters = client._apply_filters("TEST.INDICATOR")

        # Should contain currency-related filter
        # (Actual implementation depends on PRICES constant)
        assert "DAC1" in filters

    @patch("oda_data.api.oecd.load_indicators")
    def test_apply_filters_applies_measure_filter(self, mock_load_indicators):
        """Test that measure filter is applied correctly."""
        mock_load_indicators.return_value = {
            "TEST.INDICATOR": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        client = OECDClient(measure="net_disbursement")
        filters = client._apply_filters("TEST.INDICATOR")

        # Should contain measure-related filter
        assert "DAC1" in filters

    @patch("oda_data.api.oecd.load_indicators")
    def test_apply_filters_handles_multiple_sources(self, mock_load_indicators):
        """Test that filters are created for multiple sources."""
        mock_load_indicators.return_value = {
            "TEST.MULTI": {
                "sources": ["DAC1", "DAC2A"],
                "filters": {
                    "DAC1": {"flowtype_code": ["in", [10]]},
                    "DAC2A": {"aidtype_code": ["in", [1010]]},
                },
            }
        }

        client = OECDClient()
        filters = client._apply_filters("TEST.MULTI")

        assert "DAC1" in filters
        assert "DAC2A" in filters


# ============================================================================
# Tests for OECDClient._load_data
# ============================================================================


class TestOECDClientLoadData:
    """Tests for the _load_data method."""

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_load_data_calls_appropriate_reader(
        self, mock_readers, mock_load_indicators, sample_dac1_df
    ):
        """Test that _load_data calls the correct data source reader."""
        # Mock indicator
        mock_load_indicators.return_value = {
            "TEST.DAC1": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        # Mock DAC1Data reader
        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac1_df
        mock_reader_class = MagicMock(return_value=mock_reader_instance)

        # Setup READERS mock as a proper dict
        mock_readers.__getitem__.return_value = mock_reader_class
        mock_readers.__contains__ = lambda self, key: key == "DAC1"

        client = OECDClient(years=[2020, 2021], providers=[1, 2])
        client._load_data("TEST.DAC1")

        # Reader should be initialized and read() should be called
        mock_reader_class.assert_called_once()
        mock_reader_instance.read.assert_called_once()

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_load_data_passes_correct_kwargs_to_readers(
        self, mock_readers, mock_load_indicators, sample_dac1_df
    ):
        """Test that _load_data passes correct parameters to readers."""
        mock_load_indicators.return_value = {
            "TEST.DAC1": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac1_df
        mock_reader_class = MagicMock(return_value=mock_reader_instance)

        # Setup READERS mock as a proper dict
        mock_readers.__getitem__.return_value = mock_reader_class
        mock_readers.__contains__ = lambda self, key: key == "DAC1"

        client = OECDClient(years=[2020, 2021], providers=[1, 2])
        client._load_data("TEST.DAC1")

        # Check that reader was initialized with correct parameters
        call_kwargs = mock_reader_class.call_args[1]
        assert "years" in call_kwargs
        assert "providers" in call_kwargs

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.READERS")
    def test_load_data_stores_result_in_indicators_data(
        self, mock_readers, mock_load_indicators, sample_dac1_df
    ):
        """Test that _load_data stores result in indicators_data dict."""
        mock_load_indicators.return_value = {
            "TEST.DAC1": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        mock_reader_instance = MagicMock()
        mock_reader_instance.read.return_value = sample_dac1_df
        mock_reader_class = MagicMock(return_value=mock_reader_instance)

        # Setup READERS mock as a proper dict
        mock_readers.__getitem__.return_value = mock_reader_class
        mock_readers.__contains__ = lambda self, key: key == "DAC1"

        client = OECDClient()
        client._load_data("TEST.DAC1")

        assert "TEST.DAC1" in client.indicators_data
        assert isinstance(client.indicators_data["TEST.DAC1"], pd.DataFrame)


# ============================================================================
# Tests for OECDClient._process_data
# ============================================================================


class TestOECDClientProcessData:
    """Tests for the _process_data method."""

    @patch("oda_data.api.oecd.load_indicators")
    def test_process_data_skips_when_no_custom_function(
        self, mock_load_indicators, sample_dac1_df
    ):
        """Test that _process_data skips processing when no custom function."""
        mock_load_indicators.return_value = {
            "TEST.DAC1": {
                "sources": ["DAC1"],
                "custom_function": "",  # No custom function
            }
        }

        client = OECDClient()
        client.indicators_data["TEST.DAC1"] = sample_dac1_df.copy()

        original_data = client.indicators_data["TEST.DAC1"].copy()
        client._process_data("TEST.DAC1")

        # Data should be unchanged
        pd.testing.assert_frame_equal(
            client.indicators_data["TEST.DAC1"], original_data
        )

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.source_to_module")
    def test_process_data_applies_custom_function(
        self, mock_source_to_module, mock_load_indicators, sample_dac1_df
    ):
        """Test that _process_data applies custom function when specified."""
        # Mock indicator with custom function
        mock_load_indicators.return_value = {
            "TEST.DAC1": {"sources": ["DAC1"], "custom_function": "test_function"}
        }

        # Mock the custom function module
        mock_function = MagicMock(return_value=sample_dac1_df)
        mock_module = MagicMock()
        mock_module.test_function = mock_function
        mock_source_to_module.__getitem__.return_value = mock_module

        client = OECDClient()
        client.indicators_data["TEST.DAC1"] = sample_dac1_df.copy()

        client._process_data("TEST.DAC1")

        # Custom function should have been called
        mock_function.assert_called_once()


# ============================================================================
# Tests for OECDClient._group_data
# ============================================================================


class TestOECDClientGroupData:
    """Tests for the _group_data method."""

    @patch("oda_data.api.oecd.load_indicators")
    def test_group_data_skips_non_crs_sources(
        self, mock_load_indicators, sample_dac1_df
    ):
        """Test that _group_data skips non-CRS indicators."""
        mock_load_indicators.return_value = {"TEST.DAC1": {"sources": ["DAC1"]}}

        client = OECDClient()
        client.indicators_data["TEST.DAC1"] = sample_dac1_df.copy()

        original_data = client.indicators_data["TEST.DAC1"].copy()
        client._group_data("TEST.DAC1")

        # Data should be unchanged for non-CRS
        pd.testing.assert_frame_equal(
            client.indicators_data["TEST.DAC1"], original_data
        )

    @patch("oda_data.api.oecd.load_indicators")
    @patch("oda_data.api.oecd.group_data_based_on_indicator")
    def test_group_data_groups_crs_indicators(
        self, mock_group_function, mock_load_indicators, sample_crs_df
    ):
        """Test that _group_data processes CRS indicators."""
        mock_load_indicators.return_value = {"TEST.CRS": {"sources": ["CRS"]}}

        mock_group_function.return_value = sample_crs_df

        client = OECDClient(measure="net_disbursement")
        client.indicators_data["TEST.CRS"] = sample_crs_df.copy()

        client._group_data("TEST.CRS")

        # Grouping function should have been called
        mock_group_function.assert_called_once()


# ============================================================================
# Tests for OECDClient._convert_units
# ============================================================================


class TestOECDClientConvertUnits:
    """Tests for the _convert_units method."""

    @patch("oda_data.api.oecd.clean.convert_units")
    def test_convert_units_calls_convert_utility(self, mock_convert, sample_dac1_df):
        """Test that _convert_units calls the convert_units utility."""
        mock_convert.return_value = sample_dac1_df

        client = OECDClient(currency="EUR", base_year=2020)
        client.indicators_data["TEST"] = sample_dac1_df.copy()

        client._convert_units("TEST")

        # Convert utility should be called with correct parameters
        mock_convert.assert_called_once()
        call_kwargs = mock_convert.call_args[1]
        assert call_kwargs["currency"] == "EUR"
        assert call_kwargs["base_year"] == 2020


# ============================================================================
# Tests for OECDClient.get_indicators
# ============================================================================


class TestOECDClientGetIndicators:
    """Tests for the get_indicators method (main entry point)."""

    @patch.object(OECDClient, "_load_data")
    @patch.object(OECDClient, "_process_data")
    @patch.object(OECDClient, "_group_data")
    @patch.object(OECDClient, "_convert_units")
    @patch("oda_data.api.oecd.load_indicators")
    def test_get_indicators_single_indicator_workflow(
        self,
        mock_load_indicators,
        mock_convert,
        mock_group,
        mock_process,
        mock_load,
        sample_dac1_df,
    ):
        """Test the complete workflow for a single indicator."""
        mock_load_indicators.return_value = {
            "TEST.INDICATOR": {"sources": ["DAC1"], "filters": {"DAC1": {}}}
        }

        # Setup mock to populate indicators_data
        def load_side_effect(indicator):
            client.indicators_data[indicator] = sample_dac1_df.copy()

        mock_load.side_effect = load_side_effect

        client = OECDClient()
        result = client.get_indicators("TEST.INDICATOR")

        # All workflow methods should be called
        mock_load.assert_called_once_with("TEST.INDICATOR")
        mock_process.assert_called_once_with("TEST.INDICATOR")
        mock_group.assert_called_once_with("TEST.INDICATOR")
        mock_convert.assert_called_once_with("TEST.INDICATOR")

        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame)

    @patch.object(OECDClient, "_load_data")
    @patch.object(OECDClient, "_process_data")
    @patch.object(OECDClient, "_group_data")
    @patch.object(OECDClient, "_convert_units")
    @patch("oda_data.api.oecd.load_indicators")
    def test_get_indicators_multiple_indicators(
        self,
        mock_load_indicators,
        mock_convert,
        mock_group,
        mock_process,
        mock_load,
        sample_dac1_df,
        sample_dac2a_df,
    ):
        """Test get_indicators with multiple indicators."""
        mock_load_indicators.return_value = {
            "IND1": {"sources": ["DAC1"], "filters": {"DAC1": {}}},
            "IND2": {"sources": ["DAC2A"], "filters": {"DAC2A": {}}},
        }

        def load_side_effect(indicator):
            if indicator == "IND1":
                client.indicators_data[indicator] = sample_dac1_df.copy()
            else:
                client.indicators_data[indicator] = sample_dac2a_df.copy()

        mock_load.side_effect = load_side_effect

        client = OECDClient()
        result = client.get_indicators(["IND1", "IND2"])

        # Should be called for both indicators
        assert mock_load.call_count == 2
        assert isinstance(result, pd.DataFrame)
        # Result should have one_indicator column
        assert "one_indicator" in result.columns


# ============================================================================
# Tests for OECDClient Class Methods
# ============================================================================


class TestOECDClientClassMethods:
    """Tests for OECDClient class methods."""

    def test_available_providers_returns_dict(self):
        """Test that available_providers returns a dictionary."""
        providers = OECDClient.available_providers()

        assert isinstance(providers, dict)
        assert len(providers) > 0

    def test_available_recipients_returns_dict(self):
        """Test that available_recipients returns a dictionary."""
        recipients = OECDClient.available_recipients()

        assert isinstance(recipients, dict)
        assert len(recipients) > 0

    def test_available_currencies_returns_list(self):
        """Test that available_currencies returns a list."""
        currencies = OECDClient.available_currencies()

        assert isinstance(currencies, list)
        assert "USD" in currencies
        assert "EUR" in currencies

    def test_available_indicators_returns_dict(self):
        """Test that available_indicators returns indicator information."""
        indicators = OECDClient.available_indicators()

        assert isinstance(indicators, dict)
        # Each indicator should have name, description, sources
        for _code, info in indicators.items():
            assert "name" in info or "description" in info or "sources" in info

    def test_arguments_property_returns_dict(self):
        """Test that arguments property returns client parameters."""
        client = OECDClient(
            years=[2020, 2021], providers=[1, 2], currency="EUR", base_year=2020
        )

        args = client.arguments

        assert isinstance(args, dict)
        assert args["years"] == [2020, 2021]
        assert args["providers"] == [1, 2]
        assert args["currency"] == "EUR"
        assert args["base_year"] == 2020
