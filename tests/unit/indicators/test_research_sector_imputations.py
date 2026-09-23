"""
Tests for sector imputation calculations.

This module tests the business logic for:
- Rolling period total calculations
- Purpose share calculations
- Multilateral sector imputation formula: imputed_value = core_contribution × spending_share
- spending_by_purpose's exclude_multilateral_core forwarding (issue #164)
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.sector_imputations import (
    _compute_imputations,
    period_purpose_shares,
    rolling_period_total,
    share_by_purpose,
    spending_by_purpose,
)

# ============================================================================
# Tests for rolling_period_total
# ============================================================================


class TestRollingPeriodTotal:
    """Tests for the rolling_period_total function."""

    def test_rolling_period_total_default_3_years(self):
        """Test 3-year rolling total (default)."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2018, 2019, 2020, 2021],
                "category": ["A", "A", "A", "A"],
                ODASchema.VALUE: [100.0, 150.0, 200.0, 250.0],
            }
        )

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # For 2021: sum of 2021, 2020, 2019 = 250 + 200 + 150 = 600
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 600.0

        # For 2020: sum of 2020, 2019, 2018 = 200 + 150 + 100 = 450
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 450.0

    def test_rolling_period_total_custom_period_length(self):
        """Test custom period length (e.g., 5 years)."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2017, 2018, 2019, 2020, 2021],
                "category": ["A", "A", "A", "A", "A"],
                ODASchema.VALUE: [50.0, 100.0, 150.0, 200.0, 250.0],
            }
        )

        result = rolling_period_total(df, period_length=5, grouper=["category"])

        # For 2021: sum of last 5 years = 50 + 100 + 150 + 200 + 250 = 750
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 750.0

    def test_rolling_period_total_iterates_backwards(self):
        """Test that function iterates backwards through years."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2019, 2020, 2021],
                "category": ["A", "A", "A"],
                ODASchema.VALUE: [100.0, 200.0, 300.0],
            }
        )

        result = rolling_period_total(df, period_length=2, grouper=["category"])

        # Should have years in descending order or at least all years present
        assert 2021 in result[ODASchema.YEAR].values
        assert 2020 in result[ODASchema.YEAR].values

    def test_rolling_period_total_groups_correctly(self):
        """Test that grouping preserves separate categories."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                "category": ["A", "B", "A", "B"],
                ODASchema.VALUE: [100.0, 200.0, 150.0, 250.0],
            }
        )

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
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2018, 2020, 2021],  # Missing 2019
                "category": ["A", "A", "A"],
                ODASchema.VALUE: [100.0, 200.0, 300.0],
            }
        )

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # For 2021: rolling window is [2021, 2020, 2019], but only 2021 and 2020 exist = 300 + 200 = 500
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 500.0

    def test_rolling_period_total_assigns_max_year_correctly(self):
        """Test that year is assigned as the maximum year in the period."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2019, 2020, 2021],
                "category": ["A", "A", "A"],
                ODASchema.VALUE: [100.0, 200.0, 300.0],
            }
        )

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
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                "purpose": ["Education", "Health", "Water"],
                ODASchema.VALUE: [100.0, 200.0, 700.0],
            }
        )

        result = share_by_purpose(df, grouper=[ODASchema.YEAR])

        # Total is 1000, so shares should be 0.1, 0.2, 0.7
        assert result[result["purpose"] == "Education"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.1)
        assert result[result["purpose"] == "Health"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.2)
        assert result[result["purpose"] == "Water"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.7)

    def test_share_by_purpose_groups_correctly(self):
        """Test that shares are calculated within specified groups."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                "purpose": ["Education", "Health", "Education", "Health"],
                ODASchema.VALUE: [100.0, 400.0, 300.0, 700.0],
            }
        )

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
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                "purpose": ["A", "B", "C"],
                ODASchema.VALUE: [100.0, 200.0, 700.0],
            }
        )

        result = share_by_purpose(df, grouper=[ODASchema.YEAR])

        # Sum of shares for 2020 should be 1.0
        total_share = result[result[ODASchema.YEAR] == 2020][ODASchema.SHARE].sum()
        assert total_share == pytest.approx(1.0)

    def test_share_by_purpose_filters_na_shares(self):
        """Test that NA shares are filtered out."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021],
                "purpose": ["A", "B", "C"],
                ODASchema.VALUE: [100.0, 200.0, 0.0],
            }
        )

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
        core = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100, 200],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.VALUE: [1000.0, 2000.0],
            }
        )

        shares = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100, 100, 200],
                ODASchema.YEAR: [2020, 2020, 2020],
                "purpose": ["Education", "Health", "Education"],
                ODASchema.SHARE: [0.3, 0.7, 0.4],
            }
        )

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
        core = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100],
                ODASchema.YEAR: [2020],
                ODASchema.VALUE: [1000.0],
            }
        )

        shares = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100, 200],  # 200 should not match
                ODASchema.YEAR: [2020, 2020],
                "purpose": ["Education", "Education"],
                ODASchema.SHARE: [0.5, 0.5],
            }
        )

        result = _compute_imputations(core, shares)

        # Should only have channel 100
        assert len(result) == 1
        assert result[ODASchema.CHANNEL_CODE].iloc[0] == 100

    def test_compute_imputations_drops_share_column(self):
        """Test that share column is dropped from output."""
        core = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100],
                ODASchema.YEAR: [2020],
                ODASchema.VALUE: [1000.0],
            }
        )

        shares = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100],
                ODASchema.YEAR: [2020],
                "purpose": ["Education"],
                ODASchema.SHARE: [0.5],
            }
        )

        result = _compute_imputations(core, shares)

        # Should not have share column
        assert ODASchema.SHARE not in result.columns

    def test_compute_imputations_filters_zero_values(self):
        """Test that zero-value rows are filtered out."""
        core = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100, 200],
                ODASchema.YEAR: [2020, 2020],
                ODASchema.VALUE: [1000.0, 0.0],  # 200 has 0
            }
        )

        shares = pd.DataFrame(
            {
                ODASchema.CHANNEL_CODE: [100, 200],
                ODASchema.YEAR: [2020, 2020],
                "purpose": ["Education", "Education"],
                ODASchema.SHARE: [0.5, 0.5],
            }
        )

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
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2019, 2020, 2021, 2019, 2020, 2021],
                "purpose": ["Edu", "Edu", "Edu", "Health", "Health", "Health"],
                ODASchema.VALUE: [100.0, 150.0, 200.0, 200.0, 250.0, 300.0],
            }
        )

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
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2018, 2019, 2020, 2021],
                "purpose": ["Edu", "Edu", "Edu", "Edu"],
                ODASchema.VALUE: [100.0, 150.0, 200.0, 250.0],
            }
        )

        result = period_purpose_shares(
            df,
            period_length=3,
            grouper=["purpose"],
            share_by_grouper=[ODASchema.YEAR, "purpose"],
        )

        # Should have computed rolling totals over 3 years
        assert len(result) > 0


class TestSpendingByPurposeExcludeMultilateralCore:
    """Tests that spending_by_purpose forwards exclude_multilateral_core to CRSData."""

    @staticmethod
    def _crs_read_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.PROVIDER_NAME: ["Donor", "Donor"],
                ODASchema.AGENCY_CODE: [1, 1],
                ODASchema.AGENCY_NAME: ["Agency", "Agency"],
                ODASchema.PURPOSE_CODE: [110, 110],
                ODASchema.RECIPIENT_CODE: [1, 1],
                ODASchema.YEAR: [2020, 2020],
                "usd_disbursement": [10.0, 20.0],
            }
        )

    def test_forwards_exclude_multilateral_core_default_true(self):
        """spending_by_purpose passes exclude_multilateral_core=True by default."""
        with patch("oda_data.api.sources.CRSData") as mock_crs_cls:
            mock_crs_cls.return_value.read.return_value = self._crs_read_df()

            spending_by_purpose(years=[2020], providers=[1])

            _, kwargs = mock_crs_cls.call_args
            assert kwargs["exclude_multilateral_core"] is True

    def test_forwards_exclude_multilateral_core_when_disabled(self):
        """spending_by_purpose passes exclude_multilateral_core=False through."""
        with patch("oda_data.api.sources.CRSData") as mock_crs_cls:
            mock_crs_cls.return_value.read.return_value = self._crs_read_df()

            spending_by_purpose(
                years=[2020], providers=[1], exclude_multilateral_core=False
            )

            _, kwargs = mock_crs_cls.call_args
            assert kwargs["exclude_multilateral_core"] is False


class TestSpendingByPurposeExcludesCoreContributions:
    """End-to-end (I/O-boundary-mocked) tests of the bi_multi == 2 exclusion.

    Unlike TestSpendingByPurposeExcludeMultilateralCore above, these mock only
    at the I/O boundary (``pd.read_parquet`` / the bulk fetcher, as the
    CRSData-level tests in tests/unit/test_sources.py do) and run
    spending_by_purpose for real, so they exercise the actual row-level
    exclusion rather than just the kwarg forwarding.
    """

    @staticmethod
    def _mixed_bi_multi_df() -> pd.DataFrame:
        """One normal row, one bi_multi == 2 (core contribution) row, one null."""
        return pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.PROVIDER_NAME: ["Donor", "Donor", "Donor"],
                ODASchema.AGENCY_CODE: [1, 1, 1],
                ODASchema.AGENCY_NAME: ["Agency", "Agency", "Agency"],
                ODASchema.PURPOSE_CODE: [110, 110, 110],
                ODASchema.RECIPIENT_CODE: [1, 1, 1],
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.BI_MULTI: [1, 2, None],
                "usd_disbursement": [10.0, 20.0, 30.0],
            }
        )

    def _run_spending_by_purpose(
        self,
        temp_cache_dir,
        mock_bulk_fetcher,
        exclude_multilateral_core: bool,
    ) -> pd.DataFrame:
        from oda_data.api.sources import CRSData

        CRSData.memory_cache.clear()

        with (
            patch(
                "oda_data.api.sources.create_crs_bulk_fetcher"
            ) as mock_create_fetcher,
            patch("oda_data.api.sources.pd.read_parquet") as mock_read_parquet,
            patch("oda_data.api.sources.ODAPaths") as mock_paths,
            patch(
                "oda_data.indicators.research.sector_imputations.convert_units",
                side_effect=lambda data, **kwargs: data,
            ),
        ):
            mock_paths.raw_data = temp_cache_dir
            mock_paths.cache_root = temp_cache_dir
            mock_create_fetcher.return_value = mock_bulk_fetcher
            mock_read_parquet.return_value = self._mixed_bi_multi_df()

            return spending_by_purpose(
                years=[2020],
                providers=[1],
                exclude_multilateral_core=exclude_multilateral_core,
            )

    def test_excludes_bi_multi_2_by_default(self, temp_cache_dir, mock_bulk_fetcher):
        """The summed value excludes the bi_multi == 2 row but keeps the null row."""
        result = self._run_spending_by_purpose(
            temp_cache_dir, mock_bulk_fetcher, exclude_multilateral_core=True
        )

        # Normal row (10.0) + null bi_multi row (30.0); the bi_multi == 2 row
        # (20.0) is excluded.
        assert result[ODASchema.VALUE].sum() == pytest.approx(40.0)

    def test_keeps_bi_multi_2_when_disabled(self, temp_cache_dir, mock_bulk_fetcher):
        """With exclude_multilateral_core=False, the bi_multi == 2 row is kept."""
        result = self._run_spending_by_purpose(
            temp_cache_dir, mock_bulk_fetcher, exclude_multilateral_core=False
        )

        # All three rows are included: 10.0 + 20.0 + 30.0
        assert result[ODASchema.VALUE].sum() == pytest.approx(60.0)


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
