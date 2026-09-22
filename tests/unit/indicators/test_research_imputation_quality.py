"""
Tests for oda_data.indicators.research.imputation_quality.

Builds real `imputed_multilateral_by_purpose` results via crs=/multisystem=
fixtures (no network) and runs `imputation_quality_report` over them.
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.imputation_quality import imputation_quality_report
from oda_data.indicators.research.sector_imputations import (
    imputed_multilateral_by_purpose,
)


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
            "usd_disbursement": value,
        }
        for y in years
    ]


def _multisystem_rows(donor_channel_year_amount):
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


@pytest.fixture
def mixed_result() -> pd.DataFrame:
    """A result covering imputed (with a concentrated purpose mix) and
    unallocated rows."""
    crs = pd.concat(
        [
            pd.DataFrame(_crs_rows(104, 1.0, 110, 1, range(2018, 2023), 900.0)),
            pd.DataFrame(_crs_rows(104, 1.0, 120, 1, range(2018, 2023), 100.0)),
        ],
        ignore_index=True,
    )
    multisystem = _multisystem_rows(
        [
            (1, 47128, 2022, 1000.0),
            (2, 47128, 2022, 500.0),
            (3, 909090, 2022, 250.0),  # no crosswalk presence, no proxy
        ]
    )
    return imputed_multilateral_by_purpose(
        years=[2022], crs=crs, multisystem=multisystem
    )


class TestByChannelYear:
    def test_one_row_per_channel_year_with_core_total(self, mixed_result):
        report = imputation_quality_report(mixed_result)
        by_channel_year = report["by_channel_year"]

        row_47128 = by_channel_year.loc[
            by_channel_year[ODASchema.CHANNEL_CODE] == 47128
        ].iloc[0]
        assert row_47128["core_value"] == pytest.approx(1500.0)
        assert row_47128["allocation_statuses"] == ["imputed"]
        assert row_47128["staleness_years"] == 0

        row_909090 = by_channel_year.loc[
            by_channel_year[ODASchema.CHANNEL_CODE] == 909090
        ].iloc[0]
        assert row_909090["core_value"] == pytest.approx(250.0)
        assert row_909090["allocation_statuses"] == ["unallocated"]
        assert pd.isna(row_909090["staleness_years"])


class TestTotalsByStatus:
    def test_totals_sum_to_grand_total_and_shares_sum_to_one(self, mixed_result):
        totals = imputation_quality_report(mixed_result)["totals_by_status"]

        assert totals["value"].sum() == pytest.approx(
            mixed_result[ODASchema.VALUE].sum()
        )
        assert totals["share_of_total"].sum() == pytest.approx(1.0)
        imputed_row = totals.loc[totals["allocation_status"] == "imputed"].iloc[0]
        assert imputed_row["value"] == pytest.approx(1500.0)


class TestDuplicatesNegativesCrosswalkMisses:
    def test_no_duplicates_in_a_clean_result(self, mixed_result):
        assert imputation_quality_report(mixed_result)["duplicates"].empty

    def test_duplicated_rows_are_flagged(self, mixed_result):
        doubled = pd.concat([mixed_result, mixed_result.iloc[[0]]], ignore_index=True)
        duplicates = imputation_quality_report(doubled)["duplicates"]
        assert len(duplicates) == 2

    def test_no_negatives_in_a_clean_result(self, mixed_result):
        assert imputation_quality_report(mixed_result)["negatives"].empty

    def test_negative_value_is_flagged(self, mixed_result):
        negative = mixed_result.copy()
        negative.loc[negative.index[0], ODASchema.VALUE] = -10.0
        negatives = imputation_quality_report(negative)["negatives"]
        assert len(negatives) == 1

    def test_no_crosswalk_misses_when_channel_code_always_present(self, mixed_result):
        assert imputation_quality_report(mixed_result)["crosswalk_misses"].empty


class TestSinglePurposeConcentration:
    def test_flags_a_concentrated_window_above_threshold(self, mixed_result):
        concentration = imputation_quality_report(mixed_result)[
            "single_purpose_concentration"
        ]

        row = concentration.loc[concentration["share_channel_code"] == 47128].iloc[0]
        assert row["top_purpose_code"] == 110
        assert row["top_purpose_share"] == pytest.approx(0.9)
        assert row["flagged"]

    def test_excludes_rows_with_no_share_channel_code(self, mixed_result):
        concentration = imputation_quality_report(mixed_result)[
            "single_purpose_concentration"
        ]
        # 909090 is unallocated (no share_channel_code) and must not appear.
        assert 909090 not in concentration["share_channel_code"].tolist()

    def test_threshold_is_configurable(self, mixed_result):
        strict = imputation_quality_report(mixed_result, concentration_threshold=0.95)[
            "single_purpose_concentration"
        ]
        row = strict.loc[strict["share_channel_code"] == 47128].iloc[0]
        assert not row["flagged"]


class TestProvenance:
    def test_reads_provenance_from_the_result_frame(self, mixed_result):
        report = imputation_quality_report(mixed_result)
        assert report["provenance"] == mixed_result.attrs["provenance"]

    def test_missing_attrs_yields_none(self, mixed_result):
        # Most pandas operations drop .attrs; simulate one.
        stripped = mixed_result.copy(deep=True)
        stripped.attrs = {}
        report = imputation_quality_report(stripped)
        assert report["provenance"] is None
