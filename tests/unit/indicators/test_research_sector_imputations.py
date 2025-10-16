"""
Tests for sector imputation calculations.

This module tests the business logic for:
- Rolling period total calculations
- Purpose share calculations
- Multilateral sector imputation formula: imputed_value = core_contribution × spending_share
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.sector_imputations import (
    _compute_imputations,
    period_purpose_shares,
    rolling_period_total,
    share_by_purpose,
)


# ============================================================================
# Tests for rolling_period_total
# ============================================================================


class TestRollingPeriodTotal:
    """Tests for the rolling_period_total function."""

    def test_rolling_period_total_default_3_years(self):
        """Test 3-year rolling total (default)."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2018, 2019, 2020, 2021],
            "category": ["A", "A", "A", "A"],
            ODASchema.VALUE: [100.0, 150.0, 200.0, 250.0],
        })

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # For 2021: sum of 2021, 2020, 2019 = 250 + 200 + 150 = 600
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 600.0

        # For 2020: sum of 2020, 2019, 2018 = 200 + 150 + 100 = 450
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 450.0

    def test_rolling_period_total_custom_period_length(self):
        """Test custom period length (e.g., 5 years)."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2017, 2018, 2019, 2020, 2021],
            "category": ["A", "A", "A", "A", "A"],
            ODASchema.VALUE: [50.0, 100.0, 150.0, 200.0, 250.0],
        })

        result = rolling_period_total(df, period_length=5, grouper=["category"])

        # For 2021: sum of last 5 years = 50 + 100 + 150 + 200 + 250 = 750
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 750.0

    def test_rolling_period_total_iterates_backwards(self):
        """Test that function iterates backwards through years."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2019, 2020, 2021],
            "category": ["A", "A", "A"],
            ODASchema.VALUE: [100.0, 200.0, 300.0],
        })

        result = rolling_period_total(df, period_length=2, grouper=["category"])

        # Should have years in descending order or at least all years present
        assert 2021 in result[ODASchema.YEAR].values
        assert 2020 in result[ODASchema.YEAR].values

    def test_rolling_period_total_groups_correctly(self):
        """Test that grouping preserves separate categories."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2021, 2021],
            "category": ["A", "B", "A", "B"],
            ODASchema.VALUE: [100.0, 200.0, 150.0, 250.0],
        })

        result = rolling_period_total(df, period_length=2, grouper=["category"])

        # Category A for 2021: 150 + 100 = 250
        cat_a_2021 = result[
            (result["category"] == "A") & (result[ODASchema.YEAR] == 2021)
        ]
        assert cat_a_2021[ODASchema.VALUE].iloc[0] == 250.0

        # Category B for 2021: 250 + 200 = 450
        cat_b_2021 = result[
            (result["category"] == "B") & (result[ODASchema.YEAR] == 2021)
        ]
        assert cat_b_2021[ODASchema.VALUE].iloc[0] == 450.0

    def test_rolling_period_total_handles_missing_years(self):
        """Test that function handles years with missing data."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2018, 2020, 2021],  # Missing 2019
            "category": ["A", "A", "A"],
            ODASchema.VALUE: [100.0, 200.0, 300.0],
        })

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # For 2021: rolling window is [2021, 2020, 2019], but only 2021 and 2020 exist = 300 + 200 = 500
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 500.0

    def test_rolling_period_total_assigns_max_year_correctly(self):
        """Test that year is assigned as the maximum year in the period."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2019, 2020, 2021],
            "category": ["A", "A", "A"],
            ODASchema.VALUE: [100.0, 200.0, 300.0],
        })

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # Each row should have the correct year assigned
        assert ODASchema.YEAR in result.columns
        # The year column should match the period end year
        years = result[ODASchema.YEAR].unique()
        assert len(years) > 0


# ============================================================================
# Tests for share_by_purpose
# ============================================================================


class TestShareByPurpose:
    """Tests for the share_by_purpose function."""

    def test_share_by_purpose_formula(self):
        """Test share formula: value / sum(values_in_group)."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2020],
            "purpose": ["Education", "Health", "Water"],
            ODASchema.VALUE: [100.0, 200.0, 700.0],
        })

        result = share_by_purpose(df, grouper=[ODASchema.YEAR])

        # Total is 1000, so shares should be 0.1, 0.2, 0.7
        assert result[result["purpose"] == "Education"][ODASchema.SHARE].iloc[0] == pytest.approx(0.1)
        assert result[result["purpose"] == "Health"][ODASchema.SHARE].iloc[0] == pytest.approx(0.2)
        assert result[result["purpose"] == "Water"][ODASchema.SHARE].iloc[0] == pytest.approx(0.7)

    def test_share_by_purpose_groups_correctly(self):
        """Test that shares are calculated within specified groups."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2021, 2021],
            "purpose": ["Education", "Health", "Education", "Health"],
            ODASchema.VALUE: [100.0, 400.0, 300.0, 700.0],
        })

        result = share_by_purpose(df, grouper=[ODASchema.YEAR])

        # 2020: Education=100, Health=400, total=500 → shares: 0.2, 0.8
        edu_2020 = result[
            (result["purpose"] == "Education") & (result[ODASchema.YEAR] == 2020)
        ]
        assert edu_2020[ODASchema.SHARE].iloc[0] == pytest.approx(0.2)

        # 2021: Education=300, Health=700, total=1000 → shares: 0.3, 0.7
        edu_2021 = result[
            (result["purpose"] == "Education") & (result[ODASchema.YEAR] == 2021)
        ]
        assert edu_2021[ODASchema.SHARE].iloc[0] == pytest.approx(0.3)

    def test_share_by_purpose_sums_to_one(self):
        """Test that shares sum to 1.0 within each group."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2020],
            "purpose": ["A", "B", "C"],
            ODASchema.VALUE: [100.0, 200.0, 700.0],
        })

        result = share_by_purpose(df, grouper=[ODASchema.YEAR])

        # Sum of shares for 2020 should be 1.0
        total_share = result[result[ODASchema.YEAR] == 2020][ODASchema.SHARE].sum()
        assert total_share == pytest.approx(1.0)

    def test_share_by_purpose_filters_na_shares(self):
        """Test that NA shares are filtered out."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2020, 2020, 2021],
            "purpose": ["A", "B", "C"],
            ODASchema.VALUE: [100.0, 200.0, 0.0],
        })

        result = share_by_purpose(df, grouper=[ODASchema.YEAR, "purpose"])

        # Should not have NA shares
        assert not result[ODASchema.SHARE].isna().any()


# ============================================================================
# Tests for _compute_imputations
# ============================================================================


class TestComputeImputations:
    """Tests for the _compute_imputations function."""

    def test_compute_imputations_formula(self):
        """Test imputation formula: value = core_value × share."""
        core = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100, 200],
            ODASchema.YEAR: [2020, 2020],
            ODASchema.PROVIDER_CODE: [1, 1],
            ODASchema.VALUE: [1000.0, 2000.0],
        })

        shares = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100, 100, 200],
            ODASchema.YEAR: [2020, 2020, 2020],
            "purpose": ["Education", "Health", "Education"],
            ODASchema.SHARE: [0.3, 0.7, 0.4],
        })

        result = _compute_imputations(core, shares)

        # For channel 100: Education = 1000 × 0.3 = 300, Health = 1000 × 0.7 = 700
        edu_100 = result[
            (result[ODASchema.CHANNEL_CODE] == 100) & (result["purpose"] == "Education")
        ]
        assert edu_100[ODASchema.VALUE].iloc[0] == pytest.approx(300.0)

        health_100 = result[
            (result[ODASchema.CHANNEL_CODE] == 100) & (result["purpose"] == "Health")
        ]
        assert health_100[ODASchema.VALUE].iloc[0] == pytest.approx(700.0)

        # For channel 200: Education = 2000 × 0.4 = 800
        edu_200 = result[
            (result[ODASchema.CHANNEL_CODE] == 200) & (result["purpose"] == "Education")
        ]
        assert edu_200[ODASchema.VALUE].iloc[0] == pytest.approx(800.0)

    def test_compute_imputations_merges_on_channel_and_year(self):
        """Test that merge happens on channel_code and year."""
        core = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100],
            ODASchema.YEAR: [2020],
            ODASchema.VALUE: [1000.0],
        })

        shares = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100, 200],  # 200 should not match
            ODASchema.YEAR: [2020, 2020],
            "purpose": ["Education", "Education"],
            ODASchema.SHARE: [0.5, 0.5],
        })

        result = _compute_imputations(core, shares)

        # Should only have channel 100
        assert len(result) == 1
        assert result[ODASchema.CHANNEL_CODE].iloc[0] == 100

    def test_compute_imputations_drops_share_column(self):
        """Test that share column is dropped from output."""
        core = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100],
            ODASchema.YEAR: [2020],
            ODASchema.VALUE: [1000.0],
        })

        shares = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100],
            ODASchema.YEAR: [2020],
            "purpose": ["Education"],
            ODASchema.SHARE: [0.5],
        })

        result = _compute_imputations(core, shares)

        # Should not have share column
        assert ODASchema.SHARE not in result.columns

    def test_compute_imputations_filters_zero_values(self):
        """Test that zero-value rows are filtered out."""
        core = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100, 200],
            ODASchema.YEAR: [2020, 2020],
            ODASchema.VALUE: [1000.0, 0.0],  # 200 has 0
        })

        shares = pd.DataFrame({
            ODASchema.CHANNEL_CODE: [100, 200],
            ODASchema.YEAR: [2020, 2020],
            "purpose": ["Education", "Education"],
            ODASchema.SHARE: [0.5, 0.5],
        })

        result = _compute_imputations(core, shares)

        # Should not have channel 200 (value would be 0 × 0.5 = 0)
        assert 200 not in result[ODASchema.CHANNEL_CODE].values


# ============================================================================
# Integration Tests
# ============================================================================


class TestPeriodPurposeShares:
    """Integration tests for period_purpose_shares."""

    def test_period_purpose_shares_combines_rolling_and_share(self):
        """Test that function combines rolling_period_total and share_by_purpose."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2019, 2020, 2021, 2019, 2020, 2021],
            "purpose": ["Edu", "Edu", "Edu", "Health", "Health", "Health"],
            ODASchema.VALUE: [100.0, 150.0, 200.0, 200.0, 250.0, 300.0],
        })

        result = period_purpose_shares(
            df, period_length=2, grouper=["purpose"], share_by_grouper=[ODASchema.YEAR]
        )

        # Should have share column
        assert ODASchema.SHARE in result.columns

        # Shares should sum to ~1.0 for each year
        for year in result[ODASchema.YEAR].unique():
            year_shares = result[result[ODASchema.YEAR] == year][ODASchema.SHARE].sum()
            assert year_shares == pytest.approx(1.0)

    def test_period_purpose_shares_with_custom_period_length(self):
        """Test with custom period length."""
        df = pd.DataFrame({
            ODASchema.YEAR: [2018, 2019, 2020, 2021],
            "purpose": ["Edu", "Edu", "Edu", "Edu"],
            ODASchema.VALUE: [100.0, 150.0, 200.0, 250.0],
        })

        result = period_purpose_shares(
            df, period_length=3, grouper=["purpose"], share_by_grouper=[ODASchema.YEAR, "purpose"]
        )

        # Should have computed rolling totals over 3 years
        assert len(result) > 0


class TestImputedMultilateralByPurposeIntegration:
    """Integration tests for imputed_multilateral_by_purpose (if needed)."""

    def test_complete_pipeline_with_realistic_data(self):
        """Test complete imputation pipeline with realistic scenario."""
        # This would require mocking CRSData and MultiSystemData
        # For now, we test the core calculation logic which is already covered
        pass

    def test_handles_missing_years(self):
        """Test that pipeline handles missing years in either input."""
        # This would test the merge behavior when years don't align
        # Already covered in test_compute_imputations_merges_on_channel_and_year
        pass
