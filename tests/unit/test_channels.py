"""Tests for channel code resolution.

`add_multilateral_channel_codes` joins CRS (provider_code, agency_code)
pairs onto the reviewed multilateral channel crosswalk. This module tests
that join directly (float/null agency-code normalisation, the raise and
unallocated paths for unmapped pairs, excluded-row handling) plus the
long-standing `channel_to_code` / `add_channel_names` helpers and the
deprecated `add_multi_channel_codes` alias.
"""

import logging

import pandas as pd
import pytest

from oda_data.clean_data.channels import (
    UnmappedChannelError,
    add_channel_names,
    add_multi_channel_codes,
    add_multilateral_channel_codes,
    channel_to_code,
    clean_string,
    get_multilateral_channel_crosswalk,
)
from oda_data.clean_data.schema import ODASchema
from oda_data.config import ODAPaths

_OD_LOGGER = logging.getLogger("oda_data")

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def crosswalk() -> pd.DataFrame:
    """A small reviewed crosswalk exercising float agency codes, a null
    agency code, and an excluded row."""
    return pd.DataFrame(
        {
            "provider_code": [807, 807, 918, 104],
            "agency_code": [1.0, None, 5.0, 1.0],
            "provider_name": ["UNEP", "UNEP", "EU Institutions", "NDF"],
            "agency_name": ["Assessed contributions", None, "MFA", "NDF"],
            "channel_code": [41116, 41116, None, 47128],
            "status": ["mapped", "mapped", "excluded", "mapped"],
            "reason": [
                None,
                None,
                "borrowing-funded, no core-contribution channel",
                None,
            ],
            "method": ["exact", "exact", "manual", "exact"],
            "reviewed": [True, True, True, True],
        }
    )


@pytest.fixture
def crs_rows() -> pd.DataFrame:
    """CRS-shaped rows: a mapped pair reported with an int agency code, the
    same channel reported under a null agency code, an excluded pair, and
    an unmapped pair carrying money."""
    return pd.DataFrame(
        {
            ODASchema.PROVIDER_CODE: [807, 807, 918, 999],
            ODASchema.AGENCY_CODE: [1, None, 5, 2],
            ODASchema.PROVIDER_NAME: ["UNEP", "UNEP", "EU Institutions", "Unknown Org"],
            ODASchema.AGENCY_NAME: [
                "Assessed contributions",
                None,
                "MFA",
                "Some Fund",
            ],
            ODASchema.VALUE: [100.0, 50.0, 30.0, 200.0],
        }
    )


# ============================================================================
# Tests for clean_string / channel_to_code / add_channel_names
# (backed by crs_channel_mapping.csv, unrelated to the crosswalk join)
# ============================================================================


class TestCleanString:
    """Tests for the clean_string function."""

    def test_clean_string_converts_to_lowercase(self):
        assert clean_string("WORLD BANK") == "world bank"

    def test_clean_string_removes_punctuation(self):
        result = clean_string("Food & Agriculture Organization!")
        assert result == "food agriculture organization"

    def test_clean_string_normalizes_whitespace(self):
        result = clean_string("World    Bank   Group")
        assert result == "world bank group"

    def test_clean_string_strips_leading_trailing_spaces(self):
        assert clean_string("  World Bank  ") == "world bank"

    def test_clean_string_with_series(self):
        series = pd.Series(["WORLD BANK", "Food & Ag"])
        result = clean_string(series)
        assert isinstance(result, pd.Series)
        assert result.iloc[0] == "world bank"
        assert result.iloc[1] == "food ag"

    def test_clean_string_with_single_string(self):
        result = clean_string("WORLD BANK")
        assert isinstance(result, str)
        assert result == "world bank"


class TestChannelToCode:
    """Tests for the channel_to_code function."""

    def test_channel_to_code_default_channel_name(self, monkeypatch):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_crs_official_mapping",
            lambda: pd.DataFrame(
                {
                    "channel_code": [44000, 41301],
                    "channel_name": ["World Bank", "Food and Agriculture Organization"],
                    "en_acronym": ["WB", "FAO"],
                    "fr_acronym": ["BM", "FAO"],
                }
            ),
        )

        result = channel_to_code(map_to="channel_name")

        assert result["world bank"] == 44000

    def test_channel_to_code_by_en_acronym(self, monkeypatch):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_crs_official_mapping",
            lambda: pd.DataFrame(
                {
                    "channel_code": [44000, 41301],
                    "channel_name": ["World Bank", "Food and Agriculture Organization"],
                    "en_acronym": ["WB", "FAO"],
                    "fr_acronym": ["BM", "FAO"],
                }
            ),
        )

        result = channel_to_code(map_to="en_acronym")

        assert result["WB"] == 44000
        assert result["FAO"] == 41301

    def test_channel_to_code_invalid_map_to_raises_error(self):
        with pytest.raises(ValueError, match="map_to must be one of"):
            channel_to_code(map_to="invalid")


class TestAddChannelNames:
    """Tests for the add_channel_names function."""

    def test_add_channel_names_maps_codes_to_names(self, monkeypatch):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.channel_to_code",
            lambda map_to="channel_name": {"world bank": 44000, "fao": 41301},
        )

        df = pd.DataFrame({"channel_code": [44000, 41301]})

        result = add_channel_names(
            df, codes_column="channel_code", target_column="channel_name"
        )

        assert result["channel_name"].iloc[0] == "world bank"
        assert result["channel_name"].iloc[1] == "fao"


class TestGetMultilateralChannelCrosswalk:
    """Tests for get_multilateral_channel_crosswalk against the real,
    committed crosswalk file. Skipped while that file has not landed yet
    (owned by a parallel slice)."""

    def test_real_crosswalk_has_expected_columns(self):
        path = ODAPaths.cleaning / "multilateral_channel_crosswalk.csv"
        if not path.exists():
            pytest.skip("multilateral_channel_crosswalk.csv not yet present")

        result = get_multilateral_channel_crosswalk()

        expected = {
            "provider_code",
            "agency_code",
            "provider_name",
            "agency_name",
            "channel_code",
            "status",
            "reason",
            "method",
            "reviewed",
        }
        assert expected.issubset(result.columns)
        assert result["status"].isin(["mapped", "excluded"]).all()


# ============================================================================
# Tests for add_multilateral_channel_codes (the exact join)
# ============================================================================


class TestAddMultilateralChannelCodes:
    """Tests for the add_multilateral_channel_codes exact join."""

    def test_unreviewed_rows_are_treated_as_unmapped(self, crs_rows, crosswalk):
        unreviewed = crosswalk.assign(reviewed=False)
        with pytest.raises(UnmappedChannelError):
            add_multilateral_channel_codes(crs_rows, crosswalk=unreviewed)

    def test_crosswalk_without_reviewed_column_is_refused(self, crs_rows, crosswalk):
        with pytest.raises(ValueError, match="reviewed"):
            add_multilateral_channel_codes(
                crs_rows, crosswalk=crosswalk.drop(columns=["reviewed"])
            )

    def test_joins_correct_codes_for_mapped_pairs(self, crs_rows, crosswalk):
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )

        mapped = result.set_index([ODASchema.PROVIDER_CODE, ODASchema.AGENCY_CODE])
        assert mapped.loc[(807, 1), ODASchema.CHANNEL_CODE] == 41116

    def test_normalises_float_agency_codes(self, crs_rows, crosswalk):
        # crosswalk's agency_code for the 918/5 pair is stored as 5.0 (float);
        # crs_rows reports it as an int. Both must resolve to the same row.
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )
        # 918/5 is excluded, so it should be dropped rather than unmapped.
        remaining_pairs = set(
            zip(
                result[ODASchema.PROVIDER_CODE],
                result[ODASchema.AGENCY_CODE],
                strict=True,
            )
        )
        assert (918, 5) not in remaining_pairs

    def test_normalises_null_agency_on_both_sides(self, crs_rows, crosswalk):
        # crs_rows has a null agency_code for provider 807; the crosswalk
        # carries its own null-agency row for the same provider. They must
        # match each other, not fail to join.
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )
        null_agency_row = result.loc[
            (result[ODASchema.PROVIDER_CODE] == 807)
            & result[ODASchema.AGENCY_CODE].isna()
        ]
        assert len(null_agency_row) == 1
        assert null_agency_row[ODASchema.CHANNEL_CODE].iloc[0] == 41116

    def test_channel_code_dtype_is_nullable_int(self, crs_rows, crosswalk):
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )
        assert result[ODASchema.CHANNEL_CODE].dtype == "int32[pyarrow]"

    def test_excludes_rows_with_excluded_status(self, crs_rows, crosswalk):
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )
        remaining_pairs = set(
            zip(
                result[ODASchema.PROVIDER_CODE],
                result[ODASchema.AGENCY_CODE],
                strict=True,
            )
        )
        assert (918, 5) not in remaining_pairs
        # Only the excluded row should have been dropped: 4 in, 1 excluded.
        assert len(result) == 3

    def test_logs_excluded_count_and_value_at_info(self, crs_rows, crosswalk, caplog):
        # The "oda_data" logger has propagate=False (see oda_data/logger.py),
        # so caplog's root handler never sees its records; attach directly.
        _OD_LOGGER.addHandler(caplog.handler)
        try:
            with caplog.at_level(logging.INFO):
                add_multilateral_channel_codes(
                    crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
                )
        finally:
            _OD_LOGGER.removeHandler(caplog.handler)

        assert "Dropped 1 excluded" in caplog.text
        assert "30.00" in caplog.text
        assert "borrowing-funded" in caplog.text

    def test_raise_path_names_pairs_and_money(self, crs_rows, crosswalk):
        with pytest.raises(UnmappedChannelError) as exc_info:
            add_multilateral_channel_codes(
                crs_rows, on_unmapped="raise", crosswalk=crosswalk
            )

        message = str(exc_info.value)
        assert "999" in message
        assert "Unknown Org" in message
        assert "Some Fund" in message
        assert "200.00" in message

    def test_raise_path_ignores_unmapped_pairs_with_zero_value(self, crosswalk):
        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_CODE: [999],
                ODASchema.AGENCY_CODE: [2],
                ODASchema.PROVIDER_NAME: ["Unknown Org"],
                ODASchema.AGENCY_NAME: ["Some Fund"],
                ODASchema.VALUE: [0.0],
            }
        )

        result = add_multilateral_channel_codes(
            df, on_unmapped="raise", crosswalk=crosswalk
        )

        assert result[ODASchema.CHANNEL_CODE].isna().all()

    def test_unallocated_path_keeps_rows_with_null_channel_code(
        self, crs_rows, crosswalk
    ):
        result = add_multilateral_channel_codes(
            crs_rows, on_unmapped="unallocated", crosswalk=crosswalk
        )

        unmapped_row = result.loc[result[ODASchema.PROVIDER_CODE] == 999]
        assert len(unmapped_row) == 1
        assert unmapped_row[ODASchema.CHANNEL_CODE].isna().all()
        # The row itself, and its money, are preserved rather than dropped.
        assert unmapped_row[ODASchema.VALUE].iloc[0] == 200.0

    def test_default_on_unmapped_is_raise(self, crs_rows, crosswalk):
        with pytest.raises(UnmappedChannelError):
            add_multilateral_channel_codes(crs_rows, crosswalk=crosswalk)


# ============================================================================
# Tests for the deprecated add_multi_channel_codes alias
# ============================================================================


class TestAddMultiChannelCodesDeprecatedAlias:
    """Tests for the add_multi_channel_codes deprecated alias."""

    def test_emits_deprecation_warning(self, crs_rows, crosswalk, monkeypatch):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_multilateral_channel_crosswalk",
            lambda: crosswalk,
        )
        with pytest.warns(DeprecationWarning, match="add_multilateral_channel_codes"):
            add_multi_channel_codes(crs_rows, on_unmapped="unallocated")

    def test_deprecation_warning_text_mentions_raising_behaviour(
        self, crs_rows, crosswalk, monkeypatch
    ):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_multilateral_channel_crosswalk",
            lambda: crosswalk,
        )
        with pytest.warns(DeprecationWarning, match="RAISES UnmappedChannelError"):
            add_multi_channel_codes(crs_rows, on_unmapped="unallocated")

    def test_alias_forwards_to_new_join(self, crs_rows, crosswalk, monkeypatch):
        # Use the real crosswalk lookup path (no crosswalk= override) by
        # monkeypatching the loader, to confirm the alias truly delegates.
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_multilateral_channel_crosswalk",
            lambda: crosswalk,
        )

        with pytest.warns(DeprecationWarning):
            result = add_multi_channel_codes(crs_rows, on_unmapped="unallocated")

        mapped = result.set_index([ODASchema.PROVIDER_CODE, ODASchema.AGENCY_CODE])
        assert mapped.loc[(807, 1), ODASchema.CHANNEL_CODE] == 41116

    def test_alias_raises_on_unmapped_by_default(
        self, crs_rows, monkeypatch, crosswalk
    ):
        monkeypatch.setattr(
            "oda_data.clean_data.channels.get_multilateral_channel_crosswalk",
            lambda: crosswalk,
        )

        with pytest.warns(DeprecationWarning), pytest.raises(UnmappedChannelError):
            add_multi_channel_codes(crs_rows)
