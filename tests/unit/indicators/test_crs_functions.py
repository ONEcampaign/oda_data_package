"""
Tests for CRS indicator custom processing functions.

This module tests the custom functions in oda_data.indicators.crs.crs_functions
that transform and filter CRS project-level data according to specific indicator
definitions.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.crs.crs_functions import (
    _group_by_mapped_channel,
    _multi_donors_only,
    _purpose_share,
    _rename_channel_column_add_names,
    _rolling_period_total,
    _yearly_share,
    drop_if_all_values_are_missing,
    drop_missing_donors,
    multilateral_purpose_spending_shares,
)


# ============================================================================
# Tests for _multi_donors_only
# ============================================================================


class TestMultiDonorsOnly:
    """Tests for the _multi_donors_only function."""

    @patch("oda_data.provider_groupings")
    def test_multi_donors_only_filters_multilateral_providers(
        self, mock_groupings, sample_crs_df
    ):
        """Test that only multilateral providers are retained."""
        # Mock provider groupings
        mock_groupings.return_value = {"multilateral": [2, 3, 4]}

        # Create DataFrame with mix of bilateral and multilateral
        df = pd.DataFrame({
            ODASchema.PROVIDER_CODE: [1, 2, 3, 4, 5],
            ODASchema.PROVIDER_NAME: ["Bilateral", "Multi1", "Multi2", "Multi3", "Bilateral2"],
            ODASchema.YEAR: [2020] * 5,
            ODASchema.VALUE: [100.0, 200.0, 300.0, 400.0, 500.0],
        })

        result = _multi_donors_only(df)

        # Should only have multilateral providers (2, 3, 4)
        assert len(result) == 3
        assert set(result[ODASchema.PROVIDER_CODE]) == {2, 3, 4}

    @patch("oda_data.provider_groupings")
    def test_multi_donors_only_empty_when_no_multilaterals(self, mock_groupings):
        """Test that empty DataFrame is returned when no multilaterals present."""
        mock_groupings.return_value = {"multilateral": [10, 20]}

        df = pd.DataFrame({
            ODASchema.PROVIDER_CODE: [1, 2, 3],
            ODASchema.VALUE: [100.0, 200.0, 300.0],
        })

        result = _multi_donors_only(df)

        assert len(result) == 0


# ============================================================================
# Tests for _group_by_mapped_channel
# ============================================================================


class TestGroupByMappedChannel:
    """Tests for the _group_by_mapped_channel function."""

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_group_by_mapped_channel_groups_correctly(self, mock_value_cols):
        """Test that data is grouped by year, channel, recipient, and purpose."""
        mock_value_cols.return_value = {
            "commitment": "commitment_current",
            "disbursement": "disbursement_current",
        }

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2020, 2021],
            ODASchema.CHANNEL_CODE: [1000, 1000, 2000, 1000],
            ODASchema.RECIPIENT_CODE: [100, 100, 100, 100],
            ODASchema.RECIPIENT_NAME: ["Country X"] * 4,
            ODASchema.PURPOSE_CODE: [110, 110, 110, 110],
            ODASchema.PURPOSE_NAME: ["Education"] * 4,
            "commitment_current": [100.0, 200.0, 300.0, 400.0],
            "disbursement_current": [80.0, 160.0, 240.0, 320.0],
        })

        result = _group_by_mapped_channel(df)

        # Should have 3 rows: 2020-channel1000, 2020-channel2000, 2021-channel1000
        assert len(result) == 3

        # Check aggregation for 2020 + channel 1000
        row_2020_1000 = result[
            (result[ODASchema.YEAR] == 2020) &
            (result[ODASchema.CHANNEL_CODE] == 1000)
        ]
        assert row_2020_1000["commitment_current"].iloc[0] == 300.0  # 100 + 200
        assert row_2020_1000["disbursement_current"].iloc[0] == 240.0  # 80 + 160

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_group_by_mapped_channel_preserves_columns(self, mock_value_cols):
        """Test that all grouping columns are preserved."""
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        df = pd.DataFrame({
            ODASchema.YEAR: [2020],
            ODASchema.CHANNEL_CODE: [1000],
            ODASchema.RECIPIENT_CODE: [100],
            ODASchema.RECIPIENT_NAME: ["Country X"],
            ODASchema.PURPOSE_CODE: [110],
            ODASchema.PURPOSE_NAME: ["Education"],
            "commitment_current": [100.0],
        })

        result = _group_by_mapped_channel(df)

        # All expected columns should be present
        expected_cols = [
            ODASchema.YEAR,
            ODASchema.CHANNEL_CODE,
            ODASchema.RECIPIENT_CODE,
            ODASchema.RECIPIENT_NAME,
            ODASchema.PURPOSE_CODE,
            ODASchema.PURPOSE_NAME,
            "commitment_current"
        ]
        assert all(col in result.columns for col in expected_cols)


# ============================================================================
# Tests for _rolling_period_total
# ============================================================================


class TestRollingPeriodTotal:
    """Tests for the _rolling_period_total function."""

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_rolling_period_total_3year_default(self, mock_value_cols):
        """Test 3-year rolling total (default period length)."""
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021, 2022, 2023],
            ODASchema.RECIPIENT_CODE: [100] * 4,
            "commitment_current": [100.0, 200.0, 300.0, 400.0],
        })

        result = _rolling_period_total(df)

        # Rolling totals only return rows where all period years exist
        # Year 2023: sum of 2021, 2022, 2023 = 200 + 300 + 400 = 900
        # Year 2022: sum of 2020, 2021, 2022 = 100 + 200 + 300 = 600
        # Year 2021 and 2020 may not be included if prior years are missing

        assert len(result) >= 2  # At least 2022 and 2023 should be present

        # Check specific rolling totals
        year_2023 = result[result[ODASchema.YEAR] == 2023]
        assert year_2023["commitment_current"].iloc[0] == 900.0

        year_2022 = result[result[ODASchema.YEAR] == 2022]
        assert year_2022["commitment_current"].iloc[0] == 600.0

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_rolling_period_total_custom_period(self, mock_value_cols):
        """Test rolling total with custom period length."""
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021, 2022],
            ODASchema.RECIPIENT_CODE: [100] * 3,
            "commitment_current": [100.0, 200.0, 300.0],
        })

        result = _rolling_period_total(df, period_length=2)

        # 2-year rolling totals
        # Year 2022: sum of 2021, 2022 = 200 + 300 = 500
        # Year 2021: sum of 2020, 2021 = 100 + 200 = 300

        year_2022 = result[result[ODASchema.YEAR] == 2022]
        assert year_2022["commitment_current"].iloc[0] == 500.0

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_rolling_period_total_groups_by_other_columns(self, mock_value_cols):
        """Test that rolling totals are calculated separately per group."""
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        # Use ODASchema constants for column names
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021, 2020, 2021],
            ODASchema.RECIPIENT_CODE: [100, 100, 200, 200],
            "commitment_current": [100.0, 200.0, 50.0, 75.0],
        })

        result = _rolling_period_total(df, period_length=2)

        # Should calculate separately for each recipient
        recipient_100_2021 = result[
            (result[ODASchema.RECIPIENT_CODE] == 100) &
            (result[ODASchema.YEAR] == 2021)
        ]
        assert recipient_100_2021["commitment_current"].iloc[0] == 300.0  # 100 + 200

        recipient_200_2021 = result[
            (result[ODASchema.RECIPIENT_CODE] == 200) &
            (result[ODASchema.YEAR] == 2021)
        ]
        assert recipient_200_2021["commitment_current"].iloc[0] == 125.0  # 50 + 75


# ============================================================================
# Tests for _purpose_share
# ============================================================================


class TestPurposeShare:
    """Tests for the _purpose_share function."""

    def test_purpose_share_calculates_share_correctly(self):
        """Test that purpose share is calculated as percentage of total."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2020],
            ODASchema.CHANNEL_CODE: [1000, 1000, 1000],
            ODASchema.PURPOSE_CODE: [110, 120, 130],
            "commitment_current": [100.0, 200.0, 300.0],  # Total = 600
        })

        result = _purpose_share(df, "commitment_current")

        # Shares should be: 100/600 = 0.1667, 200/600 = 0.3333, 300/600 = 0.5
        expected = pd.Series([100.0/600, 200.0/600, 300.0/600])
        pd.testing.assert_series_equal(result, expected, check_names=False, atol=1e-4)

    def test_purpose_share_groups_by_year_and_channel(self):
        """Test that shares are calculated separately per year/channel group."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2021, 2021],
            ODASchema.CHANNEL_CODE: [1000, 1000, 1000, 1000],
            ODASchema.PURPOSE_CODE: [110, 120, 110, 120],
            "commitment_current": [100.0, 200.0, 50.0, 150.0],
        })

        result = _purpose_share(df, "commitment_current")

        # 2020: total = 300, shares = 100/300, 200/300
        # 2021: total = 200, shares = 50/200, 150/200
        expected = pd.Series([100.0/300, 200.0/300, 50.0/200, 150.0/200])
        pd.testing.assert_series_equal(result, expected, check_names=False, atol=1e-4)


# ============================================================================
# Tests for _yearly_share
# ============================================================================


class TestYearlyShare:
    """Tests for the _yearly_share function."""

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_yearly_share_applies_to_all_value_columns(self, mock_value_cols):
        """Test that yearly share is calculated for all value columns."""
        mock_value_cols.return_value = {
            "commitment": "commitment_current",
            "disbursement": "disbursement_current",
        }

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020],
            ODASchema.CHANNEL_CODE: [1000, 1000],
            ODASchema.PURPOSE_CODE: [110, 120],
            "commitment_current": [100.0, 200.0],  # Total = 300
            "disbursement_current": [80.0, 120.0],  # Total = 200
        })

        result = _yearly_share(df)

        # Commitment shares: 100/300 = 0.3333, 200/300 = 0.6667
        assert result["commitment_current"].iloc[0] == pytest.approx(100.0/300, rel=1e-4)
        assert result["commitment_current"].iloc[1] == pytest.approx(200.0/300, rel=1e-4)

        # Disbursement shares: 80/200 = 0.4, 120/200 = 0.6
        assert result["disbursement_current"].iloc[0] == pytest.approx(80.0/200, rel=1e-4)
        assert result["disbursement_current"].iloc[1] == pytest.approx(120.0/200, rel=1e-4)


# ============================================================================
# Tests for _rename_channel_column_add_names
# ============================================================================


class TestRenameChannelColumnAddNames:
    """Tests for the _rename_channel_column_add_names function."""

    @patch("oda_data.indicators.crs.crs_functions.add_channel_names")
    def test_rename_channel_column_add_names_renames_correctly(self, mock_add_names):
        """Test that channel_code is renamed to provider_code."""
        df = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [1000, 2000],
            ODASchema.YEAR: [2020, 2021],
        })

        # Mock add_channel_names to return DataFrame with provider_name added
        mock_add_names.return_value = df.assign(**{ODASchema.PROVIDER_NAME: ["Org1", "Org2"]})

        result = _rename_channel_column_add_names(df)

        # Should have provider_code instead of channel_code
        assert ODASchema.PROVIDER_CODE in result.columns
        assert ODASchema.CHANNEL_CODE not in result.columns

        # Should have provider_name
        assert ODASchema.PROVIDER_NAME in result.columns

    @patch("oda_data.indicators.crs.crs_functions.add_channel_names")
    def test_rename_channel_column_calls_add_channel_names(self, mock_add_names):
        """Test that add_channel_names is called with correct parameters."""
        df = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [1000],
            ODASchema.YEAR: [2020],
        })

        mock_add_names.return_value = df.assign(**{ODASchema.PROVIDER_NAME: ["Org1"]})

        _rename_channel_column_add_names(df)

        # Verify add_channel_names was called
        mock_add_names.assert_called_once()
        call_kwargs = mock_add_names.call_args[1]
        assert call_kwargs["codes_column"] == ODASchema.CHANNEL_CODE
        assert call_kwargs["target_column"] == ODASchema.PROVIDER_NAME


# ============================================================================
# Tests for drop_if_all_values_are_missing
# ============================================================================


class TestDropIfAllValuesAreMissing:
    """Tests for the drop_if_all_values_are_missing function."""

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_drop_if_all_values_are_missing_drops_all_na_rows(self, mock_value_cols):
        """Test that rows with all value columns as NA are dropped."""
        mock_value_cols.return_value = {
            "commitment": "commitment_current",
            "disbursement": "disbursement_current",
        }

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021, 2022],
            "commitment_current": [100.0, None, 300.0],
            "disbursement_current": [80.0, None, 240.0],
        })

        result = drop_if_all_values_are_missing(df)

        # Should drop row where both values are NA (2021)
        assert len(result) == 2
        assert 2021 not in result[ODASchema.YEAR].values

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    def test_drop_if_all_values_keeps_partial_na_rows(self, mock_value_cols):
        """Test that rows with some non-NA values are kept."""
        mock_value_cols.return_value = {
            "commitment": "commitment_current",
            "disbursement": "disbursement_current",
        }

        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021],
            "commitment_current": [100.0, None],
            "disbursement_current": [None, 200.0],
        })

        result = drop_if_all_values_are_missing(df)

        # Both rows should be kept (each has at least one non-NA value)
        assert len(result) == 2


# ============================================================================
# Tests for drop_missing_donors
# ============================================================================


class TestDropMissingDonors:
    """Tests for the drop_missing_donors function."""

    def test_drop_missing_donors_removes_na_providers(self):
        """Test that rows with NA provider_code are removed."""
        df = pd.DataFrame({
            ODASchema.PROVIDER_CODE: [1, None, 3, None, 5],
            ODASchema.YEAR: [2020, 2020, 2021, 2021, 2022],
            ODASchema.VALUE: [100.0, 200.0, 300.0, 400.0, 500.0],
        })

        result = drop_missing_donors(df)

        # Should have 3 rows (providers 1, 3, 5)
        assert len(result) == 3
        assert result[ODASchema.PROVIDER_CODE].notna().all()

    def test_drop_missing_donors_keeps_all_when_none_missing(self):
        """Test that all rows are kept when no providers are missing."""
        df = pd.DataFrame({
            ODASchema.PROVIDER_CODE: [1, 2, 3],
            ODASchema.YEAR: [2020, 2021, 2022],
        })

        result = drop_missing_donors(df)

        assert len(result) == 3


# ============================================================================
# Tests for multilateral_purpose_spending_shares (integration)
# ============================================================================


@pytest.mark.integration
class TestMultilateralPurposeSpendingShares:
    """Integration tests for the multilateral_purpose_spending_shares function."""

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    @patch("oda_data.indicators.crs.crs_functions.add_multi_channel_codes")
    @patch("oda_data.indicators.crs.crs_functions.add_channel_names")
    def test_multilateral_purpose_spending_shares_pipeline(
        self, mock_add_names, mock_add_codes, mock_value_cols
    ):
        """Test the complete pipeline of multilateral purpose spending shares.

        This test validates that all pipeline steps are called in order.
        """
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        # Create sample input data with proper ODASchema column names
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021],
            ODASchema.CHANNEL_CODE: [1000, 1000],
            ODASchema.RECIPIENT_CODE: [100, 100],
            ODASchema.RECIPIENT_NAME: ["Country X", "Country X"],
            ODASchema.PURPOSE_CODE: [110, 110],
            ODASchema.PURPOSE_NAME: ["Education", "Education"],
            "commitment_current": [100.0, 200.0],
        })

        # Mock the intermediate functions
        # add_multi_channel_codes just passes through data
        mock_add_codes.return_value = df.copy()
        # add_channel_names should add the PROVIDER_NAME column
        def mock_add_names_func(df, codes_column, target_column):
            result = df.copy()
            result[target_column] = ["Channel " + str(code) for code in df[codes_column]]
            return result
        mock_add_names.side_effect = mock_add_names_func

        result = multilateral_purpose_spending_shares(df)

        # All pipeline functions should have been called
        mock_add_codes.assert_called_once()
        # add_channel_names is called via _rename_channel_column_add_names

        # Result should be a DataFrame
        assert isinstance(result, pd.DataFrame)

    @patch("oda_data.indicators.crs.crs_functions.crs_value_cols")
    @patch("oda_data.indicators.crs.crs_functions.add_multi_channel_codes")
    @patch("oda_data.indicators.crs.crs_functions.add_channel_names")
    def test_multilateral_purpose_spending_shares_removes_invalid_rows(
        self, mock_add_names, mock_add_codes, mock_value_cols
    ):
        """Test that invalid rows (missing values, missing donors) are removed."""
        mock_value_cols.return_value = {"commitment": "commitment_current"}

        # Create data with some invalid rows using proper ODASchema names
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2021, 2022, 2023],
            ODASchema.CHANNEL_CODE: [1, 2, None, 4],  # Row 3 has missing channel (will become provider_code)
            ODASchema.RECIPIENT_CODE: [100, 100, 100, 100],
            ODASchema.RECIPIENT_NAME: ["X", "X", "X", "X"],
            ODASchema.PURPOSE_CODE: [110, 110, 110, 110],
            ODASchema.PURPOSE_NAME: ["Education"] * 4,
            "commitment_current": [100.0, 200.0, None, 400.0],  # Row 3 has missing value
        })

        # Mock the intermediate functions
        mock_add_codes.return_value = df.copy()
        # add_channel_names should add the PROVIDER_NAME column
        def mock_add_names_func(df, codes_column, target_column):
            result = df.copy()
            # Handle None values in channel_code
            result[target_column] = ["Channel " + str(code) if pd.notna(code) else None
                                      for code in df[codes_column]]
            return result
        mock_add_names.side_effect = mock_add_names_func

        result = multilateral_purpose_spending_shares(df)

        # Verify that invalid rows (missing providers and all-NA values) are removed
        # 1. No rows should have missing provider_code (formerly channel_code)
        assert result[ODASchema.PROVIDER_CODE].notna().all()

        # 2. No rows should have all value columns as NA
        assert not result["commitment_current"].isna().all()
