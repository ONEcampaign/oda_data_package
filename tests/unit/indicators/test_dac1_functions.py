"""
Tests for DAC1 indicator custom processing functions.

This module tests the custom functions in oda_data.indicators.dac1.dac1_functions
that transform and filter DAC1 data according to specific indicator definitions.
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.dac1.dac1_functions import (
    official_oda,
    official_oda_gni,
    one_core_oda,
)

# ============================================================================
# Tests for official_oda
# ============================================================================


class TestOfficialODA:
    """Tests for the official_oda function."""

    def test_official_oda_filters_correctly_pre_2018(self):
        """Test that official_oda filters pre-2018 data correctly (aidtype 1010)."""
        df = pd.DataFrame(
            {
                "year": [2015, 2016, 2017],
                "provider_code": [1, 1, 1],
                "aidtype_code": [1010, 1010, 1010],
                "value": [1000.0, 1100.0, 1200.0],
            }
        )

        result = official_oda(df)

        assert len(result) == 3
        assert (result["aidtype_code"] == 1010).all()
        assert (result["year"] < 2018).all()

    def test_official_oda_filters_correctly_post_2018(self):
        """Test that official_oda filters post-2018 data correctly (aidtype 11010)."""
        df = pd.DataFrame(
            {
                "year": [2018, 2019, 2020],
                "provider_code": [1, 1, 1],
                "aidtype_code": [11010, 11010, 11010],
                "value": [1500.0, 1600.0, 1700.0],
            }
        )

        result = official_oda(df)

        assert len(result) == 3
        assert (result["aidtype_code"] == 11010).all()
        assert (result["year"] >= 2018).all()

    def test_official_oda_filters_correctly_mixed_years(self):
        """Test that official_oda handles mixed years with both code types."""
        df = pd.DataFrame(
            {
                "year": [2016, 2017, 2018, 2019, 2020],
                "provider_code": [1, 1, 1, 1, 1],
                "aidtype_code": [1010, 1010, 11010, 11010, 11010],
                "value": [1000.0, 1100.0, 1500.0, 1600.0, 1700.0],
            }
        )

        result = official_oda(df)

        assert len(result) == 5
        # Pre-2018 should have 1010
        pre_2018 = result[result["year"] < 2018]
        assert (pre_2018["aidtype_code"] == 1010).all()

        # Post-2018 should have 11010
        post_2018 = result[result["year"] >= 2018]
        assert (post_2018["aidtype_code"] == 11010).all()

    def test_official_oda_excludes_non_oda_codes(self):
        """Test that official_oda excludes rows with non-ODA aid type codes."""
        df = pd.DataFrame(
            {
                "year": [2019, 2019, 2019, 2019],
                "provider_code": [1, 1, 1, 1],
                "aidtype_code": [11010, 1020, 1030, 1040],  # Only 11010 is ODA for 2019
                "value": [1000.0, 2000.0, 3000.0, 4000.0],
            }
        )

        result = official_oda(df)

        # Should only include the 11010 row
        assert len(result) == 1
        assert result.iloc[0]["aidtype_code"] == 11010

    def test_official_oda_preserves_all_columns(self):
        """Test that official_oda preserves all DataFrame columns."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "provider_code": [1, 2],
                "provider_name": ["Donor A", "Donor B"],
                "aidtype_code": [11010, 11010],
                "aidtype_name": ["ODA GE", "ODA GE"],
                "value": [1000.0, 2000.0],
                "currency": ["USD", "USD"],
            }
        )

        result = official_oda(df)

        # All columns should be preserved
        assert list(result.columns) == list(df.columns)
        assert len(result) == 2


# ============================================================================
# Tests for official_oda_gni
# ============================================================================


class TestOfficialODAGNI:
    """Tests for the official_oda_gni function."""

    def test_official_oda_gni_calculates_ratio_correctly(self, sample_gni_df):
        """Test that official_oda_gni calculates ODA/GNI ratio correctly."""
        # Create ODA data
        oda_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.AIDTYPE_CODE: [11010, 11010],
                ODASchema.VALUE: [1000.0, 1200.0],
            }
        )

        # Create GNI data
        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2021],
                ODASchema.PROVIDER_CODE: [1, 1],
                "aidtype_code": [1, 1],  # GNI code
                ODASchema.VALUE: [100_000.0, 120_000.0],
            }
        )

        # Combine and process
        combined = pd.concat([oda_df, gni_df], ignore_index=True)
        result = official_oda_gni(combined)

        # Check ratio calculation
        # 1000 / 100,000 = 0.01
        # 1200 / 120,000 = 0.01
        assert len(result) == 2
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(0.01, rel=1e-5)
        assert result[ODASchema.VALUE].iloc[1] == pytest.approx(0.01, rel=1e-5)

    def test_official_oda_gni_merges_data_correctly(self):
        """Test that official_oda_gni merges ODA and GNI data on year and provider."""
        oda_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 2],
                ODASchema.AIDTYPE_CODE: [11010, 11010],
                ODASchema.VALUE: [1000.0, 2000.0],
            }
        )

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 2],
                "aidtype_code": [1, 1],
                ODASchema.VALUE: [100_000.0, 200_000.0],
            }
        )

        combined = pd.concat([oda_df, gni_df], ignore_index=True)
        result = official_oda_gni(combined)

        assert len(result) == 2
        # Provider 1: 1000 / 100,000 = 0.01
        # Provider 2: 2000 / 200,000 = 0.01
        provider_1 = result[result[ODASchema.PROVIDER_CODE] == 1]
        provider_2 = result[result[ODASchema.PROVIDER_CODE] == 2]

        assert provider_1[ODASchema.VALUE].iloc[0] == pytest.approx(0.01)
        assert provider_2[ODASchema.VALUE].iloc[0] == pytest.approx(0.01)

    def test_official_oda_gni_sets_correct_aidtype_code(self):
        """Test that official_oda_gni sets the correct aid type code and name."""
        oda_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.AIDTYPE_CODE: [11010],
                ODASchema.AIDTYPE_NAME: ["ODA GE"],
                ODASchema.VALUE: [1000.0],
            }
        )

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                "aidtype_code": [1],
                ODASchema.VALUE: [100_000.0],
            }
        )

        combined = pd.concat([oda_df, gni_df], ignore_index=True)
        result = official_oda_gni(combined)

        # Should set special ODA/GNI code
        assert result[ODASchema.AIDTYPE_CODE].iloc[0] == 1010110101
        assert "as a share of GNI" in result[ODASchema.AIDTYPE_NAME].iloc[0]

    def test_official_oda_gni_removes_gni_value_column(self):
        """Test that temporary gni_value column is removed from result."""
        oda_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.AIDTYPE_CODE: [11010],
                ODASchema.VALUE: [1000.0],
            }
        )

        gni_df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                "aidtype_code": [1],
                ODASchema.VALUE: [100_000.0],
            }
        )

        combined = pd.concat([oda_df, gni_df], ignore_index=True)
        result = official_oda_gni(combined)

        # gni_value should not be in result
        assert "gni_value" not in result.columns


# ============================================================================
# Tests for one_core_oda
# ============================================================================


class TestOneCoreODA:
    """Tests for the one_core_oda function."""

    def test_one_core_oda_excludes_in_donor_spending(self):
        """Test that one_core_oda excludes in-donor spending from total ODA."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.AIDTYPE_CODE: [11010, 1020, 1030],  # ODA + in-donor codes
                ODASchema.AIDTYPE_NAME: ["ODA GE", "In-donor 1", "In-donor 2"],
                ODASchema.VALUE: [10000.0, 500.0, 300.0],
            }
        )

        result = one_core_oda(df)

        # Should have combined result: 10000 - 500 - 300 = 9200
        assert len(result) == 1
        assert result[ODASchema.VALUE].iloc[0] == 9200.0

    def test_one_core_oda_handles_ge_oda_codes(self):
        """Test that one_core_oda correctly uses GE ODA definition (11010)."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.AIDTYPE_CODE: [11010, 1020],  # GE ODA present
                ODASchema.AIDTYPE_NAME: ["ODA GE", "In-donor"],
                ODASchema.VALUE: [10000.0, 500.0],
            }
        )

        result = one_core_oda(df)

        # Should detect GE ODA and use official_oda()
        assert len(result) == 1
        assert result[ODASchema.AIDTYPE_CODE].iloc[0] == 91010  # Core ODA code

    def test_one_core_oda_handles_legacy_codes(self):
        """Test that one_core_oda handles legacy ODA codes (1010) correctly."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2017, 2017],  # Pre-GE era
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.AIDTYPE_CODE: [1010, 1020],  # Legacy ODA code
                ODASchema.AIDTYPE_NAME: ["ODA", "In-donor"],
                ODASchema.VALUE: [10000.0, 500.0],
            }
        )

        result = one_core_oda(df)

        # Should handle legacy codes
        assert len(result) == 1
        assert result[ODASchema.AIDTYPE_CODE].iloc[0] == 91010

    def test_one_core_oda_sets_correct_code_and_name(self):
        """Test that one_core_oda sets the Core ODA code and name."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.AIDTYPE_CODE: [11010],
                ODASchema.AIDTYPE_NAME: ["ODA GE"],
                ODASchema.VALUE: [10000.0],
            }
        )

        result = one_core_oda(df)

        assert result[ODASchema.AIDTYPE_CODE].iloc[0] == 91010
        assert result[ODASchema.AIDTYPE_NAME].iloc[0] == "Core ODA"

    def test_one_core_oda_groups_by_all_dimensions(self):
        """Test that one_core_oda properly groups data by all non-value columns."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020, 2021, 2021],
                ODASchema.PROVIDER_CODE: [1, 1, 1, 1, 1],
                ODASchema.AIDTYPE_CODE: [11010, 1020, 1030, 11010, 1020],
                ODASchema.AIDTYPE_NAME: [
                    "ODA",
                    "In-donor 1",
                    "In-donor 2",
                    "ODA",
                    "In-donor",
                ],
                ODASchema.VALUE: [10000.0, 500.0, 300.0, 11000.0, 600.0],
                "flow_type_code": [10, 10, 10, 10, 10],  # Additional dimension
            }
        )

        result = one_core_oda(df)

        # Should have 2 rows (one per year)
        assert len(result) == 2

        # 2020: 10000 - 500 - 300 = 9200
        result_2020 = result[result[ODASchema.YEAR] == 2020]
        assert result_2020[ODASchema.VALUE].iloc[0] == 9200.0

        # 2021: 11000 - 600 = 10400
        result_2021 = result[result[ODASchema.YEAR] == 2021]
        assert result_2021[ODASchema.VALUE].iloc[0] == 10400.0

    def test_one_core_oda_with_multiple_providers(self):
        """Test that one_core_oda calculates Core ODA separately for each provider."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 2, 2],
                ODASchema.AIDTYPE_CODE: [11010, 1020, 11010, 1020],
                ODASchema.AIDTYPE_NAME: ["ODA", "In-donor", "ODA", "In-donor"],
                ODASchema.VALUE: [10000.0, 500.0, 20000.0, 1000.0],
            }
        )

        result = one_core_oda(df)

        # Should have 2 rows (one per provider)
        assert len(result) == 2

        provider_1 = result[result[ODASchema.PROVIDER_CODE] == 1]
        provider_2 = result[result[ODASchema.PROVIDER_CODE] == 2]

        # Provider 1: 10000 - 500 = 9500
        assert provider_1[ODASchema.VALUE].iloc[0] == 9500.0

        # Provider 2: 20000 - 1000 = 19000
        assert provider_2[ODASchema.VALUE].iloc[0] == 19000.0


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.integration
class TestDAC1FunctionsIntegration:
    """Integration tests for DAC1 functions working together."""

    def test_official_oda_and_gni_integration(self):
        """Test that official_oda output can be used with official_oda_gni."""
        # Create complete dataset with ODA and GNI
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.AIDTYPE_CODE: [11010, 1020, 1],  # ODA, in-donor, GNI
                ODASchema.VALUE: [10000.0, 500.0, 1_000_000.0],
            }
        )

        # Calculate ODA/GNI ratio
        result = official_oda_gni(df)

        # Should get ODA/GNI ratio: 10000 / 1,000,000 = 0.01
        assert len(result) == 1
        assert result[ODASchema.VALUE].iloc[0] == pytest.approx(0.01)
