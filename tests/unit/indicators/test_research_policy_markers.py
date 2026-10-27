"""
Tests for policy marker filtering logic.

This module tests the business logic for filtering bilateral policy marker data:
- Marker score mapping, including desertification's principal value 3
- Modality filtering (specific list of 11 modalities)
- Special "not_screened" handling
- Rejection of unknown markers and scores
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.api.constants import EXTENDED_PROVIDER_PURPOSE_GROUPER, MEASURES
from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.policy_markers import (
    _marker_modality_filter,
    _marker_score_map,
    bilateral_policy_marker,
)

# ============================================================================
# Tests for _marker_score_map
# ============================================================================


class TestMarkerScoreMap:
    """Tests for the _marker_score_map function."""

    def test_marker_score_map_significant(self):
        """Test that 'significant' maps to [1]."""
        result = _marker_score_map("significant")

        assert result == [1]

    def test_marker_score_map_principal(self):
        """Test that 'principal' maps to [2, 3], since desertification also uses 3."""
        result = _marker_score_map("principal")

        assert result == [2, 3]

    def test_marker_score_map_not_targeted(self):
        """Test that 'not_targeted' maps to [0]."""
        result = _marker_score_map("not_targeted")

        assert result == [0]

    def test_marker_score_map_total_targeted(self):
        """Test that 'total_targeted' maps to [1, 2, 3]."""
        result = _marker_score_map("total_targeted")

        assert result == [1, 2, 3]


# ============================================================================
# Tests for _marker_modality_filter
# ============================================================================


class TestMarkerModalityFilter:
    """Tests for the _marker_modality_filter function."""

    def test_marker_modality_filter_returns_correct_modalities(self):
        """Test that function returns correct list of 11 modality codes."""
        result = _marker_modality_filter()

        # Should return a list with a tuple containing flow_modality filter
        assert isinstance(result, list)
        assert len(result) == 1

        # Extract the filter tuple
        filter_tuple = result[0]
        assert filter_tuple[0] == ODASchema.FLOW_MODALITY
        assert filter_tuple[1] == "in"

        # Check the modalities list
        modalities = filter_tuple[2]
        expected_modalities = [
            "A02",
            "B01",
            "B03",
            "B031",
            "B032",
            "B033",
            "B04",
            "C01",
            "D01",
            "D02",
            "E01",
        ]

        assert set(modalities) == set(expected_modalities)
        assert len(modalities) == 11


# ============================================================================
# Helpers for bilateral_policy_marker tests
# ============================================================================

_GROUPER = [
    c
    for c in EXTENDED_PROVIDER_PURPOSE_GROUPER
    if c not in [ODASchema.CURRENCY, ODASchema.PRICES]
]
_MEASURE = MEASURES["CRS"]["gross_disbursement"]["column"]


@pytest.fixture
def crs_with_markers():
    """CRS rows with a blank, a 0, a 1 and a principal score per marker."""
    base = {
        col: ["A02"] * 4 if col == ODASchema.FLOW_MODALITY else [1] * 4
        for col in _GROUPER
    }
    return pd.DataFrame(
        base
        | {
            ODASchema.FLOW_CODE: [11] * 4,
            "gender": [pd.NA, 0, 1, 2],
            "environment": [2, 1, pd.NA, 0],
            "desertification": [pd.NA, 0, 1, 3],
            _MEASURE: [100.0, 200.0, 300.0, 400.0],
        }
    )


def _fake_read(frame: pd.DataFrame):
    """Mimic CRSData.read: apply 'in' filters, then return only requested columns."""

    def read(columns, additional_filters, **_):
        out = frame
        for col, _op, values in additional_filters:
            out = out.loc[out[col].isin(values)]
        return out[columns]

    return read


def _run(frame: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Call bilateral_policy_marker against a fake CRS read, skipping conversion."""
    with (
        patch("oda_data.api.sources.CRSData") as crs_cls,
        patch(
            "oda_data.indicators.research.policy_markers.convert_units",
            side_effect=lambda d, **_: d,
        ),
    ):
        crs_cls.return_value.read.side_effect = _fake_read(frame)
        return bilateral_policy_marker(years=2023, **kwargs)


# ============================================================================
# Integration Tests for bilateral_policy_marker
# ============================================================================


class TestBilateralPolicyMarkerFiltering:
    """Integration tests for bilateral_policy_marker filtering logic."""

    @pytest.mark.parametrize(("oda_only", "expected"), [(True, 600.0), (False, 1000.0)])
    def test_oda_only_drops_non_oda_flows(self, crs_with_markers, oda_only, expected):
        """Test that oda_only keeps flow codes 11, 13, 19 and 60 and drops OOF (14)."""
        frame = crs_with_markers.assign(**{ODASchema.FLOW_CODE: [11, 13, 60, 14]})

        result = _run(
            frame, marker="gender", marker_score="total_allocable", oda_only=oda_only
        )

        assert result["value"].sum() == expected

    def test_not_screened_keeps_only_blank_marker_rows(self, crs_with_markers):
        """Test that 'not_screened' returns only rows whose marker is blank."""
        result = _run(crs_with_markers, marker="gender", marker_score="not_screened")

        assert result["value"].sum() == 100.0

    @pytest.mark.parametrize("marker", ["gender", "environment", "desertification"])
    def test_scores_sum_to_total_allocable(self, crs_with_markers, marker):
        """Test that the four exclusive scores add up to total_allocable (#170)."""
        parts = sum(
            _run(crs_with_markers, marker=marker, marker_score=score)["value"].sum()
            for score in ["principal", "significant", "not_targeted", "not_screened"]
        )
        total = _run(crs_with_markers, marker=marker, marker_score="total_allocable")

        assert parts == total["value"].sum()

    @pytest.mark.parametrize(
        ("marker", "marker_score"),
        [("gender", "principle"), ("gender", "Principal"), ("gendr", "principal")],
    )
    def test_unknown_marker_or_score_raises(
        self, crs_with_markers, marker, marker_score
    ):
        """Test that a misspelt marker or score raises instead of returning all aid."""
        with pytest.raises(ValueError):
            _run(crs_with_markers, marker=marker, marker_score=marker_score)
