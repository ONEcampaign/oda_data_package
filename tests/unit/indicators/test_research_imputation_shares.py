"""
Tests for oda_data.indicators.research.imputation_shares.

This module tests:
- The window-padding fix (single-year requests no longer raise; the earliest
  requested years no longer silently vanish from a multi-year request).
- The vectorised rolling-window total against known values (replacing the old
  deep-copy/concat loop).
- flow_types filtering and the oda_only/shares_based_on_oda_only deprecation.
- The imputed/stale_share precedence and reported share_years.
- spending_by_purpose's exclude_multilateral_core forwarding and the
  bi_multi == 2 core-contribution exclusion (issue #164), migrated here from
  test_research_sector_imputations.py along with spending_by_purpose itself.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.imputation_shares import (
    FLOW_TYPE_CATEGORIES,
    multilateral_spending_shares_by_channel_and_purpose_smoothed,
    pad_years_for_window,
    period_purpose_shares,
    resolve_channel_windows,
    rolling_period_total,
    rolling_window_total,
    share_by_purpose,
    spending_by_purpose,
)

# ============================================================================
# Tests for rolling_period_total / rolling_window_total
# ============================================================================


class TestRollingPeriodTotal:
    """Tests for the rolling_period_total function (vectorised rolling total)."""

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
        """A year missing from the input counts as zero within a window."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2018, 2020, 2021],  # Missing 2019
                "category": ["A", "A", "A"],
                ODASchema.VALUE: [100.0, 200.0, 300.0],
            }
        )

        result = rolling_period_total(df, period_length=3, grouper=["category"])

        # For 2021: window is [2019, 2020, 2021]; 2019 counts as 0 = 300+200+0
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 500.0

    def test_rolling_period_total_no_year_bleed_across_groups(self):
        """A rolling window for one group must not pick up another group's years.

        Regression test for a bug in a naive "roll the whole wide frame at
        once" implementation, where rolling across a flattened (group, year)
        column index bleeds one group's trailing years into the next group's
        leading window.
        """
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2018, 2019, 2020, 2021, 2019, 2020, 2021],
                "channel": ["A", "A", "A", "A", "B", "B", "B"],
                ODASchema.VALUE: [10.0, 20.0, 30.0, 40.0, 5.0, 6.0, 7.0],
            }
        )

        result = rolling_period_total(df, period_length=2, grouper=["channel"])

        b_2019 = result[(result["channel"] == "B") & (result[ODASchema.YEAR] == 2019)]
        # If 2019 is present for B at all, it must not include A's money.
        if not b_2019.empty:
            assert b_2019[ODASchema.VALUE].iloc[0] == pytest.approx(5.0)


class TestRollingWindowTotalDefectRegression:
    """Regression tests for plan defect 1: single-year requests used to raise
    a KeyError, and a multi-year request's earliest years used to vanish.
    """

    def test_single_year_input_does_not_raise(self):
        """A single year of input, with too little history for a full
        window, returns an empty frame rather than raising."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                "channel": ["A"],
                ODASchema.VALUE: [100.0],
            }
        )

        result = rolling_window_total(df, period_length=3, grouper=["channel"])

        assert result.empty
        assert list(result.columns) == ["channel", ODASchema.YEAR, ODASchema.VALUE]

    def test_sufficient_history_covers_every_year_including_earliest(self):
        """Given enough padding before the earliest requested year, a
        multi-year window covers every one of those years, not just the
        last."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: list(range(2018, 2023)),
                "channel": ["A"] * 5,
                ODASchema.VALUE: [100.0, 110.0, 120.0, 130.0, 140.0],
            }
        )

        result = rolling_window_total(df, period_length=3, grouper=["channel"])

        # Years 2018-2019 can never have a complete 3-year window (no data
        # further back); 2020, 2021, 2022 can, and must all be present.
        assert sorted(result[ODASchema.YEAR].tolist()) == [2020, 2021, 2022]
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == pytest.approx(330.0)


class TestPadYearsForWindow:
    """Tests for pad_years_for_window."""

    def test_none_years_returns_none(self):
        assert pad_years_for_window(None) == (None, None)

    def test_pads_by_period_length_and_max_share_age(self):
        read_years, requested_years = pad_years_for_window(
            2022, period_length=3, max_share_age=5
        )

        # pad = period_length - 1 + max_share_age = 2 + 5 = 7
        assert requested_years == [2022]
        assert min(read_years) == 2022 - 7
        assert max(read_years) == 2022

    def test_pads_from_the_earliest_requested_year(self):
        read_years, requested_years = pad_years_for_window(
            [2020, 2021, 2022], period_length=3, max_share_age=5
        )

        assert requested_years == [2020, 2021, 2022]
        assert min(read_years) == 2020 - 7
        assert max(read_years) == 2022


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

        assert result[result["purpose"] == "Education"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.1)
        assert result[result["purpose"] == "Health"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.2)
        assert result[result["purpose"] == "Water"][ODASchema.SHARE].iloc[
            0
        ] == pytest.approx(0.7)

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

        total_share = result[result[ODASchema.YEAR] == 2020][ODASchema.SHARE].sum()
        assert total_share == pytest.approx(1.0)


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

        assert ODASchema.SHARE in result.columns

        for year in result[ODASchema.YEAR].unique():
            year_shares = result[result[ODASchema.YEAR] == year][ODASchema.SHARE].sum()
            assert year_shares == pytest.approx(1.0)


# ============================================================================
# Tests for resolve_channel_windows (imputed / stale_share precedence)
# ============================================================================


class TestResolveChannelWindows:
    """Tests for the imputed/stale_share window-selection logic."""

    def test_own_window_is_imputed(self):
        windows = pd.DataFrame(
            {
                "channel": ["A"],
                ODASchema.YEAR: [2022],
                ODASchema.VALUE: [100.0],
            }
        )

        result = resolve_channel_windows(
            windows, requested_years=[2022], max_share_age=5, grouper=["channel"]
        )

        assert result["allocation_status"].iloc[0] == "imputed"
        assert result["source_year"].iloc[0] == 2022

    def test_falls_back_to_latest_window_within_max_share_age(self):
        windows = pd.DataFrame(
            {
                "channel": ["A"],
                ODASchema.YEAR: [2015],
                ODASchema.VALUE: [100.0],
            }
        )

        result = resolve_channel_windows(
            windows, requested_years=[2018, 2020], max_share_age=5, grouper=["channel"]
        )

        assert set(result["allocation_status"]) == {"stale_share"}
        assert (result["source_year"] == 2015).all()

    def test_outside_max_share_age_has_no_row(self):
        windows = pd.DataFrame(
            {
                "channel": ["A"],
                ODASchema.YEAR: [2015],
                ODASchema.VALUE: [100.0],
            }
        )

        result = resolve_channel_windows(
            windows, requested_years=[2021], max_share_age=5, grouper=["channel"]
        )

        # 2021 - 2015 = 6 > max_share_age of 5
        assert result.empty

    def test_non_positive_denominator_is_not_a_share(self):
        """A window whose total is zero or negative is never used, as a
        source for its own year or as a stale fallback for a later one."""
        windows = pd.DataFrame(
            {
                "channel": ["A", "A"],
                ODASchema.YEAR: [2018, 2020],
                ODASchema.VALUE: [-50.0, 0.0],
            }
        )

        result = resolve_channel_windows(
            windows,
            requested_years=[2018, 2020, 2021],
            max_share_age=5,
            grouper=["channel"],
        )

        assert result.empty


# ============================================================================
# Tests for multilateral_spending_shares_by_channel_and_purpose_smoothed
# ============================================================================


def _crs_rows(
    channel_provider,
    channel_agency,
    purpose_code,
    recipient_code,
    years,
    value,
    category=10,
):
    return [
        {
            ODASchema.PROVIDER_CODE: channel_provider,
            ODASchema.PROVIDER_NAME: "Provider",
            ODASchema.AGENCY_CODE: channel_agency,
            ODASchema.AGENCY_NAME: "Agency",
            ODASchema.PURPOSE_CODE: purpose_code,
            ODASchema.RECIPIENT_CODE: recipient_code,
            ODASchema.YEAR: y,
            ODASchema.CATEGORY: category,
            ODASchema.BI_MULTI: 1,
            "usd_disbursement": value,
        }
        for y in years
    ]


class TestMultilateralSpendingSharesByChannelAndPurposeSmoothed:
    """Tests using the multilateral_channel_crosswalk's real (provider_code,
    agency_code) -> channel_code mapping for provider 104 (Nordic
    Development Fund), agency 1 -> channel_code 47128, via crs= fixtures
    (no network)."""

    def _fixture(self, years, value=100.0, purpose_code=110, recipient_code=1):
        return pd.DataFrame(
            _crs_rows(104, 1.0, purpose_code, recipient_code, years, value)
        )

    def test_single_year_request_returns_a_share(self):
        crs = self._fixture(years=range(2010, 2023))

        result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2022, crs=crs
        )

        assert len(result) == 1
        assert result[ODASchema.YEAR].iloc[0] == 2022
        assert result["allocation_status"].iloc[0] == "imputed"

    def test_multi_year_request_returns_every_requested_year(self):
        crs = self._fixture(years=range(2010, 2023))

        result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=[2020, 2021, 2022], crs=crs
        )

        assert sorted(result[ODASchema.YEAR].unique().tolist()) == [2020, 2021, 2022]

    def test_known_shares_two_purposes(self):
        crs = pd.concat(
            [
                self._fixture(years=range(2018, 2023), value=100.0, purpose_code=110),
                self._fixture(years=range(2018, 2023), value=300.0, purpose_code=120),
            ],
            ignore_index=True,
        )

        result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2022, crs=crs
        )

        share_110 = result[result[ODASchema.PURPOSE_CODE] == 110][ODASchema.SHARE].iloc[
            0
        ]
        share_120 = result[result[ODASchema.PURPOSE_CODE] == 120][ODASchema.SHARE].iloc[
            0
        ]
        assert share_110 == pytest.approx(0.25)
        assert share_120 == pytest.approx(0.75)

    def test_lapsed_reporter_gets_stale_share_with_years_reported(self):
        # Reports 2010-2015, then goes silent.
        crs = self._fixture(years=range(2010, 2016))

        result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2019, crs=crs, max_share_age=5
        )

        assert len(result) == 1
        assert result["allocation_status"].iloc[0] == "stale_share"
        assert result["share_years"].iloc[0] == "2013-2015"

    def test_beyond_max_share_age_has_no_row(self):
        crs = self._fixture(years=range(2010, 2016))

        result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2021, crs=crs, max_share_age=5
        )

        assert result.empty

    def test_flow_types_filters_to_selected_categories(self):
        crs = pd.concat(
            [
                self._fixture(years=range(2018, 2023), value=100.0, purpose_code=110),
                pd.DataFrame(
                    _crs_rows(104, 1.0, 999, 1, range(2018, 2023), 500.0, category=21)
                ),
            ],
            ignore_index=True,
        )

        oda_only = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2022, crs=crs, flow_types=("ODA",)
        )
        assert set(oda_only[ODASchema.PURPOSE_CODE]) == {110}

        oda_oof = multilateral_spending_shares_by_channel_and_purpose_smoothed(
            years=2022, crs=crs, flow_types=("ODA", "OOF")
        )
        assert set(oda_oof[ODASchema.PURPOSE_CODE]) == {110, 999}

    def test_unknown_flow_type_raises(self):
        crs = self._fixture(years=range(2018, 2023))

        with pytest.raises(ValueError, match="Unknown flow_types"):
            multilateral_spending_shares_by_channel_and_purpose_smoothed(
                years=2022, crs=crs, flow_types=("NOT_A_FLOW_TYPE",)
            )

    def test_oda_only_true_deprecation_warns_and_maps_to_oda(self):
        crs = pd.concat(
            [
                self._fixture(years=range(2018, 2023), value=100.0, purpose_code=110),
                pd.DataFrame(
                    _crs_rows(104, 1.0, 999, 1, range(2018, 2023), 500.0, category=60)
                ),
            ],
            ignore_index=True,
        )

        with pytest.warns(DeprecationWarning, match="oda_only"):
            result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
                years=2022, crs=crs, oda_only=True
            )

        # oda_only=True now maps to flow_types=("ODA",) -- PSI (category 60)
        # is excluded, unlike the old oda_only=True branch.
        assert set(result[ODASchema.PURPOSE_CODE]) == {110}

    def test_oda_only_false_deprecation_warns_and_maps_to_oda_oof(self):
        crs = pd.concat(
            [
                self._fixture(years=range(2018, 2023), value=100.0, purpose_code=110),
                pd.DataFrame(
                    _crs_rows(104, 1.0, 999, 1, range(2018, 2023), 500.0, category=21)
                ),
                pd.DataFrame(
                    _crs_rows(104, 1.0, 998, 1, range(2018, 2023), 500.0, category=50)
                ),
            ],
            ignore_index=True,
        )

        with pytest.warns(DeprecationWarning, match="oda_only"):
            result = multilateral_spending_shares_by_channel_and_purpose_smoothed(
                years=2022, crs=crs, oda_only=False
            )

        # oda_only=False now maps to flow_types=("ODA", "OOF") -- category 50
        # is excluded, unlike the old oda_only=False branch (no filter).
        assert set(result[ODASchema.PURPOSE_CODE]) == {110, 999}


# ============================================================================
# Tests for spending_by_purpose (moved from test_research_sector_imputations.py)
# ============================================================================


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
                ODASchema.CATEGORY: [10, 10, 10],
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
                "oda_data.indicators.research.imputation_shares.convert_units",
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


class TestSpendingByPurposeFlowTypes:
    """Tests for flow_types filtering and the oda_only deprecation on
    spending_by_purpose directly."""

    @staticmethod
    def _crs_df() -> pd.DataFrame:
        return pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.PROVIDER_NAME: ["Donor"] * 3,
                ODASchema.AGENCY_CODE: [1, 1, 1],
                ODASchema.AGENCY_NAME: ["Agency"] * 3,
                ODASchema.PURPOSE_CODE: [110, 120, 130],
                ODASchema.RECIPIENT_CODE: [1, 1, 1],
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.CATEGORY: [10, 21, 60],
                ODASchema.BI_MULTI: [1, 1, 1],
                "usd_disbursement": [10.0, 20.0, 30.0],
            }
        )

    def test_default_flow_types_is_oda_only(self):
        result = spending_by_purpose(crs=self._crs_df())
        assert result[ODASchema.VALUE].sum() == pytest.approx(10.0)

    def test_flow_types_oda_oof(self):
        result = spending_by_purpose(crs=self._crs_df(), flow_types=("ODA", "OOF"))
        assert result[ODASchema.VALUE].sum() == pytest.approx(30.0)

    def test_empty_flow_types_applies_no_filter(self):
        result = spending_by_purpose(crs=self._crs_df(), flow_types=())
        assert result[ODASchema.VALUE].sum() == pytest.approx(60.0)

    def test_oda_only_true_warns(self):
        with pytest.warns(DeprecationWarning, match="oda_only"):
            spending_by_purpose(crs=self._crs_df(), oda_only=True)

    def test_supplied_frame_excludes_core_contributions(self):
        crs = self._crs_df().assign(**{ODASchema.BI_MULTI: [2, 1, None]})
        result = spending_by_purpose(crs=crs, flow_types=())
        # The bi_multi == 2 row (10.0) is dropped; the null row is kept.
        assert result[ODASchema.VALUE].sum() == pytest.approx(50.0)

    def test_supplied_frame_keeps_core_contributions_when_disabled(self):
        crs = self._crs_df().assign(**{ODASchema.BI_MULTI: [2, 1, 1]})
        result = spending_by_purpose(
            crs=crs, flow_types=(), exclude_multilateral_core=False
        )
        assert result[ODASchema.VALUE].sum() == pytest.approx(60.0)

    def test_supplied_frame_without_bi_multi_is_refused(self):
        crs = self._crs_df().drop(columns=[ODASchema.BI_MULTI])
        with pytest.raises(ValueError, match="bi_multi"):
            spending_by_purpose(crs=crs)
        result = spending_by_purpose(crs=crs, exclude_multilateral_core=False)
        assert result[ODASchema.VALUE].sum() == pytest.approx(10.0)

    def test_supplied_frame_is_filtered_by_years_and_providers(self):
        crs = self._crs_df().assign(
            **{ODASchema.YEAR: [2020, 2021, 2020], ODASchema.PROVIDER_CODE: [1, 1, 2]}
        )
        result = spending_by_purpose(crs=crs, flow_types=(), years=2020, providers=1)
        assert result[ODASchema.VALUE].sum() == pytest.approx(10.0)

    def test_oda_only_keeps_its_positional_slot(self):
        with pytest.warns(DeprecationWarning, match="oda_only"):
            result = spending_by_purpose(
                None, None, "gross_disbursement", True, crs=self._crs_df()
            )
        assert result[ODASchema.VALUE].sum() == pytest.approx(10.0)

    def test_flow_type_categories_match_crs_codebook(self):
        assert FLOW_TYPE_CATEGORIES == {"ODA": 10, "OOF": 21, "PSI": 60}
