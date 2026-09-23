"""
Tests for oda_data.indicators.research.sector_imputations.

This module tests the allocation half of the pipeline (S5): outer-joining
core Multisystem contributions against CRS-derived shares
(imputation_shares.py, tested in test_research_imputation_shares.py), the
proxy fallback (channel_share_proxies.csv), unallocated rows, money
conservation and provenance.

Fixtures use real (provider_code, agency_code) -> channel_code crosswalk
entries so `add_multilateral_channel_codes` resolves them without mocking:
- 104 / 1 -> 47128 (Nordic Development Fund)
- 905 / 1 -> 44002 (IDA)
905/1 -> 44002 is also the proxy target for two rows in
channel_share_proxies.csv (44007 IDA-MDRI, 44003 IDA-HIPC), so 44007's own
CRS presence is exercised via a patched crosswalk instead (real IDA-MDRI has
none, by design -- that's why it needs a proxy).
"""

import warnings
from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.sector_imputations import (
    ImputationConservationError,
    _check_conservation,
    imputed_multilateral_by_purpose,
)


class NoWarnings:
    """Context manager asserting no warning of any kind is emitted."""

    def __enter__(self):
        self._cm = warnings.catch_warnings(record=True)
        self._records = self._cm.__enter__()
        warnings.simplefilter("always")
        return self

    def __exit__(self, *exc_info):
        self._cm.__exit__(*exc_info)
        assert not self._records, [str(w.message) for w in self._records]


# ============================================================================
# Fixture helpers
# ============================================================================


def _crs_rows(
    provider, agency, purpose_code, recipient_code, years, value, category=10
):
    return [
        {
            ODASchema.PROVIDER_CODE: provider,
            ODASchema.PROVIDER_NAME: "Provider",
            ODASchema.AGENCY_CODE: agency,
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


def _empty_crs() -> pd.DataFrame:
    """An empty CRS fixture with the columns `spending_by_purpose` needs,
    for scenarios where no channel in the multisystem fixture has any CRS
    presence at all (a bare `pd.DataFrame()` has no `category` column to
    filter on)."""
    return pd.DataFrame(
        columns=[
            ODASchema.PROVIDER_CODE,
            ODASchema.PROVIDER_NAME,
            ODASchema.AGENCY_CODE,
            ODASchema.AGENCY_NAME,
            ODASchema.PURPOSE_CODE,
            ODASchema.RECIPIENT_CODE,
            ODASchema.YEAR,
            ODASchema.CATEGORY,
            ODASchema.BI_MULTI,
            "usd_disbursement",
        ]
    )


def _multisystem_rows(donor_channel_year_amount):
    """donor_channel_year_amount: list of (donor_code, channel_code, year, amount)."""
    donors, channels, years, amounts = zip(*donor_channel_year_amount, strict=True)
    n = len(donor_channel_year_amount)
    return pd.DataFrame(
        {
            ODASchema.PROVIDER_CODE: list(donors),
            ODASchema.CHANNEL_CODE: list(channels),
            ODASchema.YEAR: list(years),
            "amount": list(amounts),
            "flow_type": ["Disbursements"] * n,
            "amount_type": ["Current prices"] * n,
        }
    )


# ============================================================================
# Tests for imputed_multilateral_by_purpose -- allocation-status paths
# ============================================================================


class TestImputedMultilateralByPurposeAllocationPaths:
    def test_own_share_is_imputed_and_splits_by_purpose(self):
        crs = pd.concat(
            [
                pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 300.0)),
                pd.DataFrame(_crs_rows(104, 1.0, 120, 1, range(2018, 2023), 100.0)),
            ],
            ignore_index=True,
        )
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert set(result["allocation_status"]) == {"imputed"}
        assert result[ODASchema.VALUE].sum() == pytest.approx(1000.0)
        purpose_110 = result.loc[result[ODASchema.PURPOSE_CODE] == 110, ODASchema.VALUE]
        assert purpose_110.iloc[0] == pytest.approx(750.0)  # 300/400 * 1000
        assert (result["share_channel_code"] == 47128).all()
        assert (result[ODASchema.CHANNEL_CODE] == 47128).all()

    def test_lapsed_reporter_is_stale_share(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2010, 2016), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2019, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2019], crs=crs, multisystem=multisystem, max_share_age=5
        )

        assert result["allocation_status"].iloc[0] == "stale_share"
        assert result["share_years"].iloc[0] == "2013-2015"
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(1000.0)

    def test_own_stale_share_beats_proxy(self):
        """44007 has a parent_fund proxy row (-> 44002) in
        channel_share_proxies.csv, but if it *also* has its own (stale)
        CRS-reported shares, those win -- a proxy is the last resort, not a
        preference."""
        with patch(
            "oda_data.clean_data.channels.get_multilateral_channel_crosswalk"
        ) as mock_crosswalk:
            mock_crosswalk.return_value = pd.DataFrame(
                [
                    {
                        "provider_code": 104,
                        "agency_code": 1.0,
                        "provider_name": "P",
                        "agency_name": "A",
                        "channel_code": 44007,
                        "status": "mapped",
                        "reason": None,
                        "reviewed": True,
                    }
                ]
            )
            crs = pd.DataFrame(_crs_rows(104, 1.0, 999, 1, range(2010, 2016), 100.0))
            multisystem = _multisystem_rows([(1, 44007, 2019, 1000.0)])

            result = imputed_multilateral_by_purpose(
                years=[2019], crs=crs, multisystem=multisystem, max_share_age=5
            )

        assert result["allocation_status"].iloc[0] == "stale_share"
        assert result[ODASchema.PURPOSE_CODE].iloc[0] == 999
        assert result["share_channel_code"].iloc[0] == 44007

    def test_parent_fund_proxy_borrows_shares_and_reports_proxy_channel(self):
        # 905/1 -> 44002 (IDA) reports CRS; 44007 (IDA-MDRI) has no CRS
        # presence of its own and proxies to 44002 (channel_share_proxies.csv).
        crs = pd.DataFrame(_crs_rows(905, 1.0, 16010, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 44007, 2022, 7000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert result["allocation_status"].iloc[0] == "proxy"
        assert result[ODASchema.CHANNEL_CODE].iloc[0] == 44007  # never the proxy
        assert result["share_channel_code"].iloc[0] == 44002
        assert result[ODASchema.PURPOSE_CODE].iloc[0] == 16010
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(7000.0)

    def test_fixed_purpose_proxy_allocates_whole_amount_to_one_purpose(self):
        # 41310 (UN peacekeeping) is a fixed_purpose row -> purpose 15230,
        # recipient null, no share_channel_code (channel_share_proxies.csv).
        multisystem = _multisystem_rows([(2, 41310, 2022, 500.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=_empty_crs(), multisystem=multisystem
        )

        assert len(result) == 1
        row = result.iloc[0]
        assert row["allocation_status"] == "proxy"
        assert row[ODASchema.CHANNEL_CODE] == 41310
        assert row[ODASchema.PURPOSE_CODE] == 15230
        assert pd.isna(row[ODASchema.RECIPIENT_CODE])
        assert pd.isna(row["share_channel_code"])
        assert row[ODASchema.VALUE] == pytest.approx(500.0)

    def test_no_share_and_no_proxy_is_unallocated_with_full_amount(self):
        # Channel with no crosswalk-mapped CRS presence and no proxy row.
        multisystem = _multisystem_rows([(3, 909090, 2022, 250.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=_empty_crs(), multisystem=multisystem
        )

        assert len(result) == 1
        row = result.iloc[0]
        assert row["allocation_status"] == "unallocated"
        assert pd.isna(row[ODASchema.RECIPIENT_CODE])
        assert pd.isna(row[ODASchema.PURPOSE_CODE])
        assert pd.isna(row["share_channel_code"])
        assert pd.isna(row["share_years"])
        assert row[ODASchema.VALUE] == pytest.approx(250.0)

    def test_use_proxy_shares_false_skips_proxy_and_leaves_unallocated(self):
        crs = pd.DataFrame(_crs_rows(905, 1.0, 16010, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 44007, 2022, 7000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022],
            crs=crs,
            multisystem=multisystem,
            use_proxy_shares=False,
        )

        assert result["allocation_status"].iloc[0] == "unallocated"
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(7000.0)


# ============================================================================
# Conservation
# ============================================================================


class TestConservation:
    def test_result_conserves_money_across_mixed_statuses(self):
        crs = pd.concat(
            [
                pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0)),
                pd.DataFrame(_crs_rows(905, 1.0, 16010, 1, range(2018, 2023), 100.0)),
            ],
            ignore_index=True,
        )
        multisystem = _multisystem_rows(
            [
                (1, 47128, 2022, 1000.0),  # imputed
                (1, 44007, 2022, 7000.0),  # proxy (parent_fund)
                (2, 41310, 2022, 500.0),  # proxy (fixed_purpose)
                (3, 909090, 2022, 250.0),  # unallocated
            ]
        )

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        totals = result.groupby(
            [ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.CHANNEL_CODE]
        )[ODASchema.VALUE].sum()
        assert totals[(2022, 1, 47128)] == pytest.approx(1000.0)
        assert totals[(2022, 1, 44007)] == pytest.approx(7000.0)
        assert totals[(2022, 2, 41310)] == pytest.approx(500.0)
        assert totals[(2022, 3, 909090)] == pytest.approx(250.0)

    def test_oof_only_channel_under_default_flow_types_is_imputed_not_dropped(self):
        """Regression: before D11, a channel reporting only OOF (category 21)
        to CRS -- the IBRD/EBRD/IFC/IDB Invest pattern -- got an empty
        ODA-only share pool and its core money silently vanished from the
        old inner-join output. The default flow_types=("ODA","OOF") and the
        outer join together must not drop it."""
        crs = pd.DataFrame(
            _crs_rows(104, 1.0, 210, 1, range(2018, 2023), 500.0, category=21)
        )
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert result[ODASchema.VALUE].sum() == pytest.approx(1000.0)
        assert result["allocation_status"].iloc[0] == "imputed"

    def test_empty_share_pool_under_explicit_oda_only_is_unallocated_not_dropped(self):
        """The same OOF-only channel, with flow_types explicitly restricted
        to ODA: no share pool exists, but the money must still appear as
        `unallocated`, never silently disappear."""
        crs = pd.DataFrame(
            _crs_rows(104, 1.0, 210, 1, range(2018, 2023), 500.0, category=21)
        )
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem, flow_types=("ODA",)
        )

        assert len(result) == 1
        assert result["allocation_status"].iloc[0] == "unallocated"
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(1000.0)

    def test_check_conservation_passes_within_tolerance(self):
        core = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.CHANNEL_CODE: [1],
                ODASchema.VALUE: [1000.0],
            }
        )
        result = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.CHANNEL_CODE: [1],
                ODASchema.VALUE: [999.9999995],
            }
        )
        _check_conservation(result, core, stage="test")  # must not raise

    def test_check_conservation_raises_on_dropped_money(self):
        core = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.CHANNEL_CODE: [1],
                ODASchema.VALUE: [1000.0],
            }
        )
        result = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.CHANNEL_CODE: [1],
                ODASchema.VALUE: [900.0],
            }
        )
        with pytest.raises(ImputationConservationError, match="Worst offenders"):
            _check_conservation(result, core, stage="test")

    def test_check_conservation_raises_on_missing_key_entirely(self):
        """A core key with zero rows in result (the classic silent-drop bug)
        must raise, not pass because there was nothing to sum."""
        core = pd.DataFrame(
            {
                ODASchema.YEAR: [2022],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.CHANNEL_CODE: [1],
                ODASchema.VALUE: [1000.0],
            }
        )
        result = pd.DataFrame(
            columns=[
                ODASchema.YEAR,
                ODASchema.PROVIDER_CODE,
                ODASchema.CHANNEL_CODE,
                ODASchema.VALUE,
            ]
        )
        with pytest.raises(ImputationConservationError):
            _check_conservation(result, core, stage="test")

    def test_check_conservation_empty_core_is_a_noop(self):
        core = pd.DataFrame(
            columns=[
                ODASchema.YEAR,
                ODASchema.PROVIDER_CODE,
                ODASchema.CHANNEL_CODE,
                ODASchema.VALUE,
            ]
        )
        result = core.copy()
        _check_conservation(result, core, stage="test")  # must not raise


# ============================================================================
# Zero-value rows: a wider read window must not inflate the row count
# ============================================================================


class TestZeroValueRows:
    """Regression tests: `pad_years_for_window` reads further back for a
    range request that starts earlier than a single-year request does,
    which can surface a (channel, purpose, recipient) window whose net CRS
    spending is exactly zero for a given requested year -- a channel had
    that purpose at some point in its longer history, just not in the
    3-year window ending at that particular year. Left in, that becomes a
    share of exactly 0 and an output row of exactly 0. These must never be
    emitted: a single-year call and the matching year of a multi-year range
    call must return the same rows, and a negative (a real CRS reversal)
    must survive."""

    @staticmethod
    def _master_crs() -> pd.DataFrame:
        return pd.concat(
            [
                pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2005, 2023), 300.0)),
                # Only visible to a read window starting before 2013: the
                # range call (years=range(2018, 2023)) pads back to 2011
                # (pad = period_length - 1 + max_share_age = 7, from 2018);
                # the single-year call (years=2020) only pads back to 2013.
                pd.DataFrame(_crs_rows(104, 1.0, 999, 1, [2011], 900.0)),
            ],
            ignore_index=True,
        )

    def _patched_crs_source(self):
        """Patch CRSData so `.read()` returns only the master fixture's rows
        within the `years` the caller constructed it with -- standing in for
        a real bulk read, which does filter server-side by year."""
        master = self._master_crs()

        class _FilteredCRSData:
            def __init__(self, **kwargs):
                self._years = kwargs.get("years")

            def read(self, **kwargs):
                years = self._years
                if years is None:
                    return master
                wanted = {years} if isinstance(years, int) else set(years)
                return master.loc[master[ODASchema.YEAR].isin(wanted)].reset_index(
                    drop=True
                )

        return patch("oda_data.api.sources.CRSData", side_effect=_FilteredCRSData)

    def test_single_year_matches_slice_of_range_call(self):
        # A pre-fetched `multisystem=` is used as-is, not filtered by
        # `years` (like a real caller, each fixture reflects exactly what a
        # single-year vs a 2018-2022 Multisystem read would return), so
        # `core_years` -- and therefore how far back the CRS share read
        # pads -- differs between the two calls the same way it would in
        # production.
        multisystem_single = _multisystem_rows([(1, 47128, 2020, 1000.0)])
        multisystem_range = _multisystem_rows(
            [(1, 47128, y, 1000.0) for y in range(2018, 2023)]
        )

        with self._patched_crs_source():
            single_year = imputed_multilateral_by_purpose(
                years=2020, multisystem=multisystem_single
            )
        with self._patched_crs_source():
            range_call = imputed_multilateral_by_purpose(
                years=range(2018, 2023), multisystem=multisystem_range
            )

        range_slice = range_call.loc[range_call[ODASchema.YEAR] == 2020]

        key_cols = [
            ODASchema.PROVIDER_CODE,
            ODASchema.CHANNEL_CODE,
            ODASchema.RECIPIENT_CODE,
            ODASchema.PURPOSE_CODE,
            ODASchema.VALUE,
        ]
        single_keys = single_year[key_cols].sort_values(key_cols).reset_index(drop=True)
        slice_keys = range_slice[key_cols].sort_values(key_cols).reset_index(drop=True)
        pd.testing.assert_frame_equal(single_keys, slice_keys, check_dtype=False)
        # Purpose 999's only spending (2011) sits outside every window ending
        # in 2020, so it never earns a share here -- one row, purpose 110.
        assert len(single_year) == 1

    def test_zero_value_rows_are_absent(self):
        multisystem_range = _multisystem_rows(
            [(1, 47128, y, 1000.0) for y in range(2018, 2023)]
        )

        with self._patched_crs_source():
            range_call = imputed_multilateral_by_purpose(
                years=range(2018, 2023), multisystem=multisystem_range
            )

        assert not (range_call[ODASchema.VALUE] == 0.0).any()
        assert 999 not in set(
            range_call.loc[range_call[ODASchema.YEAR] == 2020, ODASchema.PURPOSE_CODE]
        )

    def test_negative_windowed_share_survives_as_a_negative_row(self):
        """A CRS reversal netting a purpose's window to a negative total is
        a real value, not noise to be dropped alongside the zero rows."""
        crs = pd.concat(
            [
                pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2021), 400.0)),
                pd.DataFrame(_crs_rows(104, 1.0, 999, 1, range(2018, 2021), -100.0)),
            ],
            ignore_index=True,
        )
        multisystem = _multisystem_rows([(1, 47128, 2020, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2020], crs=crs, multisystem=multisystem
        )

        negative = result.loc[result[ODASchema.PURPOSE_CODE] == 999]
        assert len(negative) == 1
        assert negative[ODASchema.VALUE].iloc[0] < 0
        assert result[ODASchema.VALUE].sum() == pytest.approx(1000.0)


# ============================================================================
# Output contract: dtypes, columns, empty input
# ============================================================================


class TestOutputContract:
    def test_nullable_int64_dtypes(self):
        multisystem = _multisystem_rows([(3, 909090, 2022, 250.0)])
        result = imputed_multilateral_by_purpose(
            years=[2022], crs=_empty_crs(), multisystem=multisystem
        )

        assert str(result[ODASchema.RECIPIENT_CODE].dtype) == "Int64"
        assert str(result[ODASchema.PURPOSE_CODE].dtype) == "Int64"
        assert str(result["share_channel_code"].dtype) == "Int64"

    def test_channel_code_is_always_the_core_contribution_channel(self):
        crs = pd.DataFrame(_crs_rows(905, 1.0, 16010, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 44007, 2022, 7000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert (result[ODASchema.CHANNEL_CODE] == 44007).all()
        assert (result["share_channel_code"] == 44002).all()

    def test_share_years_format(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert result["share_years"].iloc[0] == "2020-2022"

    def test_output_columns(self):
        multisystem = _multisystem_rows([(3, 909090, 2022, 250.0)])
        result = imputed_multilateral_by_purpose(
            years=[2022], crs=_empty_crs(), multisystem=multisystem
        )

        assert list(result.columns) == [
            ODASchema.YEAR,
            ODASchema.PROVIDER_CODE,
            ODASchema.CHANNEL_CODE,
            ODASchema.RECIPIENT_CODE,
            ODASchema.PURPOSE_CODE,
            ODASchema.VALUE,
            ODASchema.CURRENCY,
            ODASchema.PRICES,
            "allocation_status",
            "share_channel_code",
            "share_years",
        ]

    def test_empty_core_returns_empty_frame_with_correct_dtypes(self):
        multisystem = pd.DataFrame(
            columns=[
                ODASchema.PROVIDER_CODE,
                ODASchema.CHANNEL_CODE,
                ODASchema.YEAR,
                "amount",
            ]
        )
        result = imputed_multilateral_by_purpose(
            years=[2022], crs=_empty_crs(), multisystem=multisystem
        )

        assert result.empty
        assert str(result[ODASchema.RECIPIENT_CODE].dtype) == "Int64"
        assert "provenance" in result.attrs


# ============================================================================
# Provenance and deprecation
# ============================================================================


class TestProvenanceAndDeprecation:
    def test_provenance_contents(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )
        provenance = result.attrs["provenance"]

        assert set(provenance) == {
            "crs_release",
            "multisystem_release",
            "crosswalk_vintage",
            "proxy_table_vintage",
            "crs_channel_mapping_vintage",
            "package_version",
            "parameters",
        }
        assert "sha256_16" in provenance["crosswalk_vintage"]
        assert provenance["parameters"]["flow_types"] == ("ODA", "OOF")
        assert provenance["parameters"]["years"] == [2022]

    def test_supplied_frames_are_recorded_as_supplied(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        result = imputed_multilateral_by_purpose(
            years=[2022], crs=crs, multisystem=multisystem
        )

        assert result.attrs["provenance"]["crs_release"] == "supplied"
        assert result.attrs["provenance"]["multisystem_release"] == "supplied"

    def test_crs_release_is_looked_up_after_the_crs_read(self, monkeypatch):
        from oda_data import cache
        from oda_data.indicators.research import sector_imputations

        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])
        calls = []
        real_shares = sector_imputations._multilateral_spending_shares

        def shares(*args, **kwargs):
            calls.append("shares")
            return real_shares(*args, **{**kwargs, "crs": crs})

        monkeypatch.setattr(sector_imputations, "_multilateral_spending_shares", shares)
        monkeypatch.setattr(cache, "release_info", calls.append)

        imputed_multilateral_by_purpose(years=[2022], multisystem=multisystem)

        assert calls.index("CRSData") > calls.index("shares")

    def test_shares_based_on_oda_only_keeps_its_positional_slot(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        with pytest.warns(DeprecationWarning, match="oda_only"):
            imputed_multilateral_by_purpose(
                [2022],
                None,
                None,
                "gross_disbursement",
                "USD",
                None,
                True,
                crs=crs,
                multisystem=multisystem,
            )

    def test_default_call_emits_no_deprecation_warning(self):
        crs = pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0))
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        with NoWarnings():
            imputed_multilateral_by_purpose(
                years=[2022], crs=crs, multisystem=multisystem
            )

    def test_deprecated_shares_based_on_oda_only_true_still_warns_and_applies(self):
        crs = pd.concat(
            [
                pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 100.0)),
                pd.DataFrame(
                    _crs_rows(104, 1.0, 999, 1, range(2018, 2023), 500.0, category=21)
                ),
            ],
            ignore_index=True,
        )
        multisystem = _multisystem_rows([(1, 47128, 2022, 1000.0)])

        with pytest.warns(DeprecationWarning, match="oda_only"):
            result = imputed_multilateral_by_purpose(
                years=[2022],
                crs=crs,
                multisystem=multisystem,
                shares_based_on_oda_only=True,
            )

        # oda_only=True -> flow_types=("ODA",): the OOF row (category 21,
        # purpose 999) must not appear in the share pool.
        assert set(result[ODASchema.PURPOSE_CODE]) == {110}


# ============================================================================
# core_multilateral_contributions_by_provider
# ============================================================================


class TestCoreMultilateralContributionsByProvider:
    def test_uses_multisystem_fixture_without_network(self):
        from oda_data.indicators.research.sector_imputations import (
            core_multilateral_contributions_by_provider,
        )

        multisystem = _multisystem_rows(
            [(1, 47128, 2022, 1000.0), (1, 47128, 2021, 500.0)]
        )

        result = core_multilateral_contributions_by_provider(multisystem=multisystem)

        assert result[ODASchema.VALUE].sum() == pytest.approx(1500.0)
        assert set(result[ODASchema.YEAR]) == {2021, 2022}

    def test_supplied_multisystem_is_filtered_by_scope(self):
        from oda_data.indicators.research.sector_imputations import (
            core_multilateral_contributions_by_provider,
        )

        multisystem = _multisystem_rows(
            [
                (1, 47128, 2022, 1000.0),
                (1, 47128, 2021, 500.0),
                (2, 47128, 2022, 300.0),
                (1, 41301, 2022, 200.0),
            ]
        )

        result = core_multilateral_contributions_by_provider(
            years=2022, providers=1, channels=47128, multisystem=multisystem
        )

        assert result[ODASchema.VALUE].sum() == pytest.approx(1000.0)
