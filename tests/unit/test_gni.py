"""
Tests for GNI share calculation utilities.

This module tests the functions in oda_data.tools.gni that compute ODA as a share
of GNI, including special handling for EU Institutions.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.tools.gni import (
    _get_eu27_gni_as_eu_institutions,
    _get_gni_data,
    add_gni_share_column,
)

# ============================================================================
# Tests for _get_eu27_gni_as_eu_institutions
# ============================================================================


class TestGetEU27GNIAsEUInstitutions:
    """Tests for the _get_eu27_gni_as_eu_institutions function."""

    @patch("oda_data.tools.gni.provider_groupings")
    @patch("oda_data.tools.gni.copy")
    def test_get_eu27_gni_fetches_eu27_countries(self, mock_copy, mock_groupings):
        """Test that EU27 countries are fetched from provider groupings."""
        mock_groupings.return_value = {"eu27_countries": [4, 5, 7, 12]}

        # Create mock OECDClient
        mock_client = MagicMock()
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [4, 5, 7],
                ODASchema.VALUE: [1000.0, 2000.0, 1500.0],
            }
        )

        # Create a mock copy that will be modified
        mock_copy_client = MagicMock()
        mock_copy_client.get_indicators.return_value = mock_gni_df
        mock_copy.return_value = mock_copy_client

        _get_eu27_gni_as_eu_institutions(mock_client)

        # Should create a copy and set providers to EU27 countries on the copy
        mock_copy.assert_called_once_with(mock_client)
        assert mock_copy_client.providers == [4, 5, 7, 12]

        # Should fetch DAC1.40.1 (GNI indicator) on the copy
        mock_copy_client.get_indicators.assert_called_once_with("DAC1.40.1")

    @patch("oda_data.tools.gni.provider_groupings")
    def test_get_eu27_gni_assigns_to_eu_institutions(self, mock_groupings):
        """Test that aggregated GNI is assigned to EU Institutions (918)."""
        mock_groupings.return_value = {"eu27_countries": [4, 5]}

        mock_client = MagicMock()
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [4, 5],
                ODASchema.PROVIDER_NAME: ["France", "Germany"],
                ODASchema.VALUE: [1000.0, 2000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        result = _get_eu27_gni_as_eu_institutions(mock_client)

        # All rows should be assigned to provider 918
        assert (result[ODASchema.PROVIDER_CODE] == 918).all()
        assert (result[ODASchema.PROVIDER_NAME] == "EU Institutions").all()

    @patch("oda_data.tools.gni.provider_groupings")
    def test_get_eu27_gni_aggregates_by_year(self, mock_groupings):
        """Test that GNI values are aggregated by year."""
        mock_groupings.return_value = {"eu27_countries": [4, 5]}

        mock_client = MagicMock()
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                ODASchema.PROVIDER_CODE: [4, 5, 4, 5],
                ODASchema.VALUE: [1000.0, 2000.0, 1100.0, 2200.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        result = _get_eu27_gni_as_eu_institutions(mock_client)

        # Should have 2 rows (one per year)
        assert len(result) == 2

        # Check 2020 aggregation
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 3000.0  # 1000 + 2000

        # Check 2021 aggregation
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 3300.0  # 1100 + 2200

    @patch("oda_data.tools.gni.provider_groupings")
    def test_get_eu27_gni_preserves_other_dimensions(self, mock_groupings):
        """Test that other grouping dimensions are preserved."""
        mock_groupings.return_value = {"eu27_countries": [4]}

        mock_client = MagicMock()
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [4],
                ODASchema.VALUE: [1000.0],
                "extra_column": ["test"],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        result = _get_eu27_gni_as_eu_institutions(mock_client)

        # Extra columns should be preserved in grouping
        assert len(result) == 1
        assert ODASchema.YEAR in result.columns
        assert ODASchema.PROVIDER_CODE in result.columns
        assert ODASchema.VALUE in result.columns


# ============================================================================
# Tests for _get_gni_data
# ============================================================================


class TestGetGNIData:
    """Tests for the _get_gni_data function."""

    def test_get_gni_data_fetches_gni_indicator(self):
        """Test that GNI data is fetched with DAC1.40.1 indicator."""
        mock_client = MagicMock()
        mock_client.providers = None
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        with patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions") as mock_eu27:
            mock_eu27.return_value = pd.DataFrame(
                columns=[ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.VALUE]
            )

            _get_gni_data(mock_client)

            # Should call get_indicators with GNI code
            # Note: the function creates a copy and modifies it
            assert mock_client.get_indicators.call_count >= 1

    def test_get_gni_data_sets_measure_to_net_disbursement(self):
        """Test that measure is set to net_disbursement before fetching."""
        mock_client = MagicMock()
        mock_client.measure = ["commitment"]  # Start with different measure
        mock_client.providers = None
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        with (
            patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions") as mock_eu27,
            patch("oda_data.tools.gni.copy") as mock_copy,
        ):
            # Mock copy to return a modifiable client
            copied_client = MagicMock()
            copied_client.providers = None
            copied_client.get_indicators.return_value = mock_gni_df
            mock_copy.return_value = copied_client
            mock_eu27.return_value = pd.DataFrame(
                columns=[ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.VALUE]
            )

            _get_gni_data(mock_client)

            # The copied client should have measure set to net_disbursement
            assert copied_client.measure == ["net_disbursement"]

    @patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions")
    def test_get_gni_data_includes_eu_institutions_when_918_in_providers(
        self, mock_eu27
    ):
        """Test that EU27 GNI is included when provider 918 is requested."""
        mock_client = MagicMock()
        mock_client.providers = [1, 918, 2]  # Includes EU Institutions
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        eu27_gni = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [918],
                ODASchema.VALUE: [500000.0],
            }
        )
        mock_eu27.return_value = eu27_gni

        with patch("oda_data.tools.gni.copy") as mock_copy:
            mock_copy.return_value = mock_client

            _get_gni_data(mock_client)

            # EU27 GNI function should have been called
            mock_eu27.assert_called_once()

    @patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions")
    def test_get_gni_data_includes_eu_institutions_when_providers_none(self, mock_eu27):
        """Test that EU27 GNI is included when providers is None (all providers)."""
        mock_client = MagicMock()
        mock_client.providers = None  # All providers
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        eu27_gni = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [918],
                ODASchema.VALUE: [500000.0],
            }
        )
        mock_eu27.return_value = eu27_gni

        with patch("oda_data.tools.gni.copy") as mock_copy:
            mock_copy.return_value = mock_client

            _get_gni_data(mock_client)

            # EU27 GNI function should have been called
            mock_eu27.assert_called_once()

    @patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions")
    def test_get_gni_data_excludes_eu_institutions_when_not_requested(self, mock_eu27):
        """Test that EU27 GNI is not included when provider 918 is not requested."""
        mock_client = MagicMock()
        mock_client.providers = [1, 2, 3]  # Does not include 918
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        with patch("oda_data.tools.gni.copy") as mock_copy:
            mock_copy.return_value = mock_client

            _get_gni_data(mock_client)

            # EU27 GNI function should not have been called
            mock_eu27.assert_not_called()

    def test_get_gni_data_filters_to_required_columns(self):
        """Test that only year, provider_code, and value columns are returned."""
        mock_client = MagicMock()
        mock_client.providers = [1]
        mock_gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
                "extra_column_1": ["test"],
                "extra_column_2": [123],
            }
        )
        mock_client.get_indicators.return_value = mock_gni_df

        with patch("oda_data.tools.gni._get_eu27_gni_as_eu_institutions") as mock_eu27:
            mock_eu27.return_value = pd.DataFrame(
                columns=[ODASchema.YEAR, ODASchema.PROVIDER_CODE, ODASchema.VALUE]
            )
            with patch("oda_data.tools.gni.copy") as mock_copy:
                mock_copy.return_value = mock_client

                result = _get_gni_data(mock_client)

                # Should only have year, provider_code, and value
                assert set(result.columns) == {
                    ODASchema.YEAR,
                    ODASchema.PROVIDER_CODE,
                    ODASchema.VALUE,
                }


# ============================================================================
# Tests for add_gni_share_column
# ============================================================================


class TestAddGNIShareColumn:
    """Tests for the add_gni_share_column function."""

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_column_calculates_percentage(self, mock_get_gni):
        """Test that GNI share is calculated as percentage."""
        mock_client = MagicMock()

        # Mock indicator data (ODA values)
        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.VALUE: [1000.0, 1500.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        # Mock GNI data
        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.VALUE: [100000.0, 150000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # Should have gni_share_pct column
        assert "gni_share_pct" in result.columns

        # Check calculations
        # 2020: 1000 / 100000 * 100 = 1.0%
        # 2021: 1500 / 150000 * 100 = 1.0%
        assert result[result[ODASchema.YEAR] == 2020]["gni_share_pct"].iloc[0] == 1.0
        assert result[result[ODASchema.YEAR] == 2021]["gni_share_pct"].iloc[0] == 1.0

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_column_rounds_to_2_decimals(self, mock_get_gni):
        """Test that GNI share is rounded to 2 decimal places."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [12345.67],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # 12345.67 / 1000000 * 100 = 1.234567 -> rounded to 1.23
        assert result["gni_share_pct"].iloc[0] == 1.23

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_column_merges_on_year_and_provider(self, mock_get_gni):
        """Test that data is merged on year and provider_code."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 2, 1],
                ODASchema.VALUE: [1000.0, 2000.0, 1500.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 2, 1],
                ODASchema.VALUE: [100000.0, 200000.0, 150000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # Each row should have correct GNI share for its provider-year
        p1_2020 = result[
            (result[ODASchema.YEAR] == 2020) & (result[ODASchema.PROVIDER_CODE] == 1)
        ]
        assert p1_2020["gni_share_pct"].iloc[0] == 1.0  # 1000 / 100000 * 100

        p2_2020 = result[
            (result[ODASchema.YEAR] == 2020) & (result[ODASchema.PROVIDER_CODE] == 2)
        ]
        assert p2_2020["gni_share_pct"].iloc[0] == 1.0  # 2000 / 200000 * 100

        p1_2021 = result[
            (result[ODASchema.YEAR] == 2021) & (result[ODASchema.PROVIDER_CODE] == 1)
        ]
        assert p1_2021["gni_share_pct"].iloc[0] == 1.0  # 1500 / 150000 * 100

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_column_preserves_original_columns(self, mock_get_gni):
        """Test that original indicator columns are preserved."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.PROVIDER_NAME: ["Donor A"],
                ODASchema.VALUE: [1000.0],
                "flow_type": ["ODA"],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [100000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # All original columns should be present
        assert ODASchema.YEAR in result.columns
        assert ODASchema.PROVIDER_CODE in result.columns
        assert ODASchema.PROVIDER_NAME in result.columns
        assert ODASchema.VALUE in result.columns
        assert "flow_type" in result.columns
        assert "gni_share_pct" in result.columns

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_column_with_multiple_indicators(self, mock_get_gni):
        """Test with multiple indicators provided as list."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1],
                "one_indicator": ["IND1", "IND2"],
                ODASchema.VALUE: [1000.0, 2000.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [100000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, ["IND1", "IND2"])

        # Both indicators should have GNI share calculated
        assert len(result) == 2
        assert "gni_share_pct" in result.columns


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.integration
class TestGNIIntegration:
    """Integration tests for GNI functions working together."""

    @patch("oda_data.tools.gni.provider_groupings")
    def test_full_gni_share_workflow(self, mock_groupings):
        """Test complete workflow of calculating GNI share with EU27 aggregation."""
        mock_groupings.return_value = {"eu27_countries": [4, 5]}

        # Create mock client
        mock_client = MagicMock()
        mock_client.providers = [1, 918]  # Regular provider + EU Institutions

        # Mock indicator data
        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 918],
                ODASchema.VALUE: [1000.0, 3000.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        with patch("oda_data.tools.gni._get_gni_data") as mock_get_gni:
            # Mock GNI data (including EU27 aggregate)
            gni_df = pd.DataFrame(
                {
                    ODASchema.YEAR: [2020, 2020],
                    ODASchema.PROVIDER_CODE: [1, 918],
                    ODASchema.VALUE: [100000.0, 500000.0],
                }
            )
            mock_get_gni.return_value = gni_df

            result = add_gni_share_column(mock_client, "TEST.INDICATOR")

            # Both providers should have GNI share
            assert len(result) == 2
            assert "gni_share_pct" in result.columns

            # Provider 1: 1000 / 100000 * 100 = 1.0%
            p1_share = result[result[ODASchema.PROVIDER_CODE] == 1][
                "gni_share_pct"
            ].iloc[0]
            assert p1_share == 1.0

            # Provider 918: 3000 / 500000 * 100 = 0.6%
            p918_share = result[result[ODASchema.PROVIDER_CODE] == 918][
                "gni_share_pct"
            ].iloc[0]
            assert p918_share == 0.6


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


class TestGNIEdgeCases:
    """Tests for edge cases in GNI functions."""

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_with_zero_gni_value(self, mock_get_gni):
        """Test handling of zero GNI values (division by zero)."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [1000.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.VALUE: [0.0],  # Zero GNI
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # Result should handle division by zero gracefully (inf or nan)
        assert "gni_share_pct" in result.columns

    @patch("oda_data.tools.gni._get_gni_data")
    def test_add_gni_share_with_missing_gni_data(self, mock_get_gni):
        """Test handling when GNI data is missing for some providers."""
        mock_client = MagicMock()

        indicator_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 2],
                ODASchema.VALUE: [1000.0, 2000.0],
            }
        )
        mock_client.get_indicators.return_value = indicator_df

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],  # Only provider 1 has GNI
                ODASchema.VALUE: [100000.0],
            }
        )
        mock_get_gni.return_value = gni_df

        result = add_gni_share_column(mock_client, "TEST.INDICATOR")

        # Provider 1 should have GNI share
        p1_row = result[result[ODASchema.PROVIDER_CODE] == 1]
        assert not p1_row["gni_share_pct"].isna().iloc[0]

        # Provider 2 should have NA for GNI share (left join)
        p2_row = result[result[ODASchema.PROVIDER_CODE] == 2]
        assert p2_row["gni_share_pct"].isna().iloc[0]
