"""
Tests for EU institution weighting logic.

This module tests the business logic for computing weight adjustments for EU
institution ODA contributions based on bilateral donor inflows.

Key formula: weight = 1 - (inflow / spending)
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.eu import (
    _compute_inflows_by_providers,
    _compute_spending_by_eui,
    get_eui_oda_weights,
    get_eui_plus_bilateral_providers_indicator,
)

# ============================================================================
# Tests for _compute_spending_by_eui
# ============================================================================


class TestComputeSpendingByEUI:
    """Tests for the _compute_spending_by_eui function."""

    def test_compute_spending_filters_to_provider_918(self):
        """Test that function filters to provider 918 (EU institutions)."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 918, 1, 2],
                ODASchema.AIDTYPE_CODE: [1010, 1010, 1010, 1010],
                ODASchema.YEAR: [2020, 2021, 2020, 2021],
                ODASchema.FLOWS_CODE: [1, 1, 1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net", "Net", "Net"],
                ODASchema.VALUE: [100.0, 200.0, 50.0, 75.0],
            }
        )

        result = _compute_spending_by_eui(df)

        # Should only have provider 918
        assert len(result) == 2  # Two years for provider 918

    def test_compute_spending_filters_to_aidtype_1010(self):
        """Test that function filters to aid type 1010."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 918],
                ODASchema.AIDTYPE_CODE: [1010, 2102],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 200.0],
            }
        )

        result = _compute_spending_by_eui(df)

        # Should only have aid type 1010
        assert len(result) == 1

    def test_compute_spending_groups_and_sums_correctly(self):
        """Test that function groups by year/flows/fund_flows/aidtype and sums."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 918],
                ODASchema.AIDTYPE_CODE: [1010, 1010],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 50.0],
            }
        )

        result = _compute_spending_by_eui(df)

        # Should sum the two values
        assert result["spending"].iloc[0] == 150.0

    def test_compute_spending_renames_value_to_spending(self):
        """Test that value column is renamed to 'spending'."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918],
                ODASchema.AIDTYPE_CODE: [1010],
                ODASchema.YEAR: [2020],
                ODASchema.FLOWS_CODE: [1],
                ODASchema.FUND_FLOWS: ["Net"],
                ODASchema.VALUE: [100.0],
            }
        )

        result = _compute_spending_by_eui(df)

        # Should have 'spending' column, not 'value'
        assert "spending" in result.columns
        assert ODASchema.VALUE not in result.columns


# ============================================================================
# Tests for _compute_inflows_by_providers
# ============================================================================


class TestComputeInflowsByProviders:
    """Tests for the _compute_inflows_by_providers function."""

    def test_compute_inflows_filters_to_specified_providers(self):
        """Test that function filters to specified provider list."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 2, 3, 918],
                ODASchema.AIDTYPE_CODE: [2102, 2102, 2102, 2102],
                ODASchema.YEAR: [2020, 2020, 2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1, 1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net", "Net", "Net"],
                ODASchema.VALUE: [10.0, 20.0, 30.0, 40.0],
            }
        )

        result = _compute_inflows_by_providers(df, providers=[1, 2])

        # Should only have providers 1 and 2
        # Sum should be 30.0 (10 + 20)
        assert result[ODASchema.VALUE].iloc[0] == 30.0

    def test_compute_inflows_filters_to_aidtype_2102(self):
        """Test that function filters to aid type 2102."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.AIDTYPE_CODE: [2102, 1010],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 200.0],
            }
        )

        result = _compute_inflows_by_providers(df, providers=[1])

        # Should only have aid type 2102
        assert len(result) == 1
        assert result[ODASchema.VALUE].iloc[0] == 100.0

    def test_compute_inflows_groups_and_sums_correctly(self):
        """Test that function groups and sums by year/flows/fund_flows/aidtype."""
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 2, 1],
                ODASchema.AIDTYPE_CODE: [2102, 2102, 2102],
                ODASchema.YEAR: [2020, 2020, 2021],
                ODASchema.FLOWS_CODE: [1, 1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net", "Net"],
                ODASchema.VALUE: [100.0, 50.0, 200.0],
            }
        )

        result = _compute_inflows_by_providers(df, providers=[1, 2])

        # Should have two rows (one for each year)
        assert len(result) == 2
        # 2020 should sum to 150.0
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 150.0


# ============================================================================
# Tests for get_eui_oda_weights
# ============================================================================


class TestGetEUIWeights:
    """Tests for the get_eui_oda_weights function."""

    @patch("oda_data.indicators.research.eu._load_dac1_eui_data")
    def test_get_eui_weights_formula_basic(self, mock_load_data):
        """Test weight formula: weight = 1 - (inflow / spending)."""
        # Mock data with known values
        mock_load_data.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 1],
                ODASchema.AIDTYPE_CODE: [1010, 2102],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 30.0],  # spending=100, inflow=30
            }
        )

        result = get_eui_oda_weights(
            years=[2020], providers=[1], measure="gross_disbursement"
        )

        # Weight should be 1 - (30 / 100) = 0.7
        assert result[2020] == pytest.approx(0.7)

    @patch("oda_data.indicators.research.eu._load_dac1_eui_data")
    def test_get_eui_weights_with_zero_inflow(self, mock_load_data):
        """Test that weight calculation works when inflow is 0."""
        # Provide both spending and zero inflow
        mock_load_data.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 1],
                ODASchema.AIDTYPE_CODE: [1010, 2102],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 0.0],  # spending=100, inflow=0
            }
        )

        result = get_eui_oda_weights(
            years=[2020], providers=[1], measure="gross_disbursement"
        )

        # Weight should be 1.0 when inflow is 0: 1 - (0/100) = 1.0
        assert result[2020] == pytest.approx(1.0)

    @patch("oda_data.indicators.research.eu._load_dac1_eui_data")
    def test_get_eui_weights_when_inflow_equals_spending(self, mock_load_data):
        """Test that weight is 0.0 when inflow equals spending."""
        mock_load_data.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 1],
                ODASchema.AIDTYPE_CODE: [1010, 2102],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net"],
                ODASchema.VALUE: [100.0, 100.0],  # spending=inflow=100
            }
        )

        result = get_eui_oda_weights(
            years=[2020], providers=[1], measure="gross_disbursement"
        )

        # Weight should be 1 - (100 / 100) = 0.0
        assert result[2020] == pytest.approx(0.0)

    @patch("oda_data.indicators.research.eu._load_dac1_eui_data")
    def test_get_eui_weights_returns_dict_mapping_year_to_weight(self, mock_load_data):
        """Test that function returns dict mapping year to weight."""
        mock_load_data.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 1, 918, 2],
                ODASchema.AIDTYPE_CODE: [1010, 2102, 1010, 2102],
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                ODASchema.FLOWS_CODE: [1, 1, 1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net", "Net", "Net"],
                ODASchema.VALUE: [100.0, 30.0, 200.0, 80.0],
            }
        )

        result = get_eui_oda_weights(
            years=[2020, 2021], providers=[1, 2], measure="gross_disbursement"
        )

        # Should be a dict with years as keys
        assert isinstance(result, dict)
        assert 2020 in result
        assert 2021 in result
        # Check formulas
        assert result[2020] == pytest.approx(0.7)  # 1 - (30/100)
        assert result[2021] == pytest.approx(0.6)  # 1 - (80/200)

    @patch("oda_data.indicators.research.eu._load_dac1_eui_data")
    def test_get_eui_weights_with_realistic_values(self, mock_load_data):
        """Test with realistic spending and inflow values."""
        # Realistic scenario: EU spends 10 billion, bilateral donors contribute 3 billion
        mock_load_data.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [918, 1, 2, 3],
                ODASchema.AIDTYPE_CODE: [1010, 2102, 2102, 2102],
                ODASchema.YEAR: [2020, 2020, 2020, 2020],
                ODASchema.FLOWS_CODE: [1, 1, 1, 1],
                ODASchema.FUND_FLOWS: ["Net", "Net", "Net", "Net"],
                ODASchema.VALUE: [10000.0, 1000.0, 1000.0, 1000.0],
            }
        )

        result = get_eui_oda_weights(
            years=[2020], providers=[1, 2, 3], measure="gross_disbursement"
        )

        # Weight should be 1 - (3000 / 10000) = 0.7
        assert result[2020] == pytest.approx(0.7)


# ============================================================================
# Integration Tests
# ============================================================================


class TestGetEUIPlusBilateralProvidersIndicator:
    """Integration tests for get_eui_plus_bilateral_providers_indicator."""

    @patch("oda_data.indicators.research.eu.get_eui_oda_weights")
    def test_applies_weights_to_eui_rows_only(self, mock_get_weights):
        """Test that weights are applied only to EU institution rows."""
        # Mock weights
        mock_get_weights.return_value = {2020: 0.8, 2021: 0.7}

        # Mock client
        client = MagicMock()
        client.years = [2020, 2021]
        client.providers = [1, 918]
        client.measure = ["gross_disbursement"]
        client.use_bulk_download = False

        # Mock indicator data
        client.get_indicators.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 918, 1, 918],
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                ODASchema.VALUE: [100.0, 1000.0, 150.0, 1500.0],
            }
        )

        result = get_eui_plus_bilateral_providers_indicator(
            client, indicator="test_indicator"
        )

        # Provider 1 should be unchanged
        prov_1_2020 = result[
            (result[ODASchema.PROVIDER_CODE] == 1) & (result[ODASchema.YEAR] == 2020)
        ]
        assert prov_1_2020[ODASchema.VALUE].iloc[0] == 100.0

        # Provider 918 should be weighted
        prov_918_2020 = result[
            (result[ODASchema.PROVIDER_CODE] == 918) & (result[ODASchema.YEAR] == 2020)
        ]
        assert prov_918_2020[ODASchema.VALUE].iloc[0] == pytest.approx(
            1000.0 * 0.8
        )  # 800.0

        prov_918_2021 = result[
            (result[ODASchema.PROVIDER_CODE] == 918) & (result[ODASchema.YEAR] == 2021)
        ]
        assert prov_918_2021[ODASchema.VALUE].iloc[0] == pytest.approx(
            1500.0 * 0.7
        )  # 1050.0

    @patch("oda_data.indicators.research.eu.get_eui_oda_weights")
    def test_adds_provider_918_if_not_in_list(self, mock_get_weights):
        """Test that provider 918 is added if not already in list."""
        mock_get_weights.return_value = {2020: 0.8}

        client = MagicMock()
        client.years = [2020]
        client.providers = [1, 2]  # No 918
        client.measure = ["gross_disbursement"]
        client.use_bulk_download = False
        client.get_indicators.return_value = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 2, 918],
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.VALUE: [100.0, 150.0, 1000.0],
            }
        )

        get_eui_plus_bilateral_providers_indicator(client, indicator="test_indicator")

        # 918 should have been added to providers list
        assert 918 in client.providers
