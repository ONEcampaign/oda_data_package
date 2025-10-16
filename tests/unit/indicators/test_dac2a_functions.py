"""
Tests for DAC2A indicator custom processing functions.

This module tests the custom functions in oda_data.indicators.dac2a.dac2a_functions
that transform and filter DAC2A bilateral flow data according to specific indicator
definitions.
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.dac2a.dac2a_functions import total_bilateral_plus_imputed

# ============================================================================
# Tests for total_bilateral_plus_imputed
# ============================================================================


class TestTotalBilateralPlusImputed:
    """Tests for the total_bilateral_plus_imputed function."""

    def test_total_bilateral_plus_imputed_groups_by_dimensions(self):
        """Test that data is grouped by all dimensions except aidtype."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.PROVIDER_NAME: ["Donor A", "Donor A", "Donor A"],
                ODASchema.RECIPIENT_CODE: [100, 100, 100],
                ODASchema.RECIPIENT_NAME: ["Country X", "Country X", "Country X"],
                "aidtype_code": [1010, 1020, 1030],  # Different aid types
                "aid_type": ["ODA", "Other", "Mixed"],
                ODASchema.VALUE: [100.0, 200.0, 300.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should have 1 row (grouped by year, provider, recipient)
        assert len(result) == 1

        # Value should be sum of all aid types
        assert result[ODASchema.VALUE].iloc[0] == 600.0

    def test_total_bilateral_plus_imputed_sets_correct_aidtype(self):
        """Test that aidtype_code is set to bilateral plus imputed code."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.RECIPIENT_CODE: [100],
                "aidtype_code": [1010],
                "aid_type": ["ODA"],
                ODASchema.VALUE: [100.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should set special code for bilateral plus imputed
        assert result["aidtype_code"].iloc[0] == 206106
        assert result["aid_type"].iloc[0] == "Bilateral plus imputed multilateral"

    def test_total_bilateral_plus_imputed_preserves_column_order(self):
        """Test that original column order is preserved."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020],
                ODASchema.PROVIDER_CODE: [1],
                ODASchema.RECIPIENT_CODE: [100],
                "aidtype_code": [1010],
                "aid_type": ["ODA"],
                ODASchema.VALUE: [100.0],
                "extra_column": ["test"],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Column order should match input
        assert list(result.columns) == list(df.columns)

    def test_total_bilateral_plus_imputed_multiple_years(self):
        """Test grouping across multiple years."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2021, 2021],
                ODASchema.PROVIDER_CODE: [1, 1, 1, 1],
                ODASchema.RECIPIENT_CODE: [100, 100, 100, 100],
                "aidtype_code": [1010, 1020, 1010, 1020],
                "aid_type": ["ODA", "Other", "ODA", "Other"],
                ODASchema.VALUE: [100.0, 200.0, 150.0, 250.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should have 2 rows (one per year)
        assert len(result) == 2

        # Check 2020 total
        year_2020 = result[result[ODASchema.YEAR] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 300.0  # 100 + 200

        # Check 2021 total
        year_2021 = result[result[ODASchema.YEAR] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 400.0  # 150 + 250

    def test_total_bilateral_plus_imputed_multiple_providers(self):
        """Test that grouping is done separately for each provider."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 2, 2],
                ODASchema.PROVIDER_NAME: ["Donor A", "Donor A", "Donor B", "Donor B"],
                ODASchema.RECIPIENT_CODE: [100, 100, 100, 100],
                ODASchema.RECIPIENT_NAME: ["Country X"] * 4,
                "aidtype_code": [1010, 1020, 1010, 1020],
                "aid_type": ["ODA", "Other", "ODA", "Other"],
                ODASchema.VALUE: [100.0, 200.0, 50.0, 75.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should have 2 rows (one per provider)
        assert len(result) == 2

        # Check provider 1
        provider_1 = result[result[ODASchema.PROVIDER_CODE] == 1]
        assert provider_1[ODASchema.VALUE].iloc[0] == 300.0  # 100 + 200

        # Check provider 2
        provider_2 = result[result[ODASchema.PROVIDER_CODE] == 2]
        assert provider_2[ODASchema.VALUE].iloc[0] == 125.0  # 50 + 75

    def test_total_bilateral_plus_imputed_multiple_recipients(self):
        """Test that grouping is done separately for each recipient."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 1, 1],
                ODASchema.PROVIDER_NAME: ["Donor A"] * 4,
                ODASchema.RECIPIENT_CODE: [100, 100, 200, 200],
                ODASchema.RECIPIENT_NAME: [
                    "Country X",
                    "Country X",
                    "Country Y",
                    "Country Y",
                ],
                "aidtype_code": [1010, 1020, 1010, 1020],
                "aid_type": ["ODA", "Other", "ODA", "Other"],
                ODASchema.VALUE: [100.0, 200.0, 50.0, 75.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should have 2 rows (one per recipient)
        assert len(result) == 2

        # Check recipient 100
        recipient_100 = result[result[ODASchema.RECIPIENT_CODE] == 100]
        assert recipient_100[ODASchema.VALUE].iloc[0] == 300.0  # 100 + 200

        # Check recipient 200
        recipient_200 = result[result[ODASchema.RECIPIENT_CODE] == 200]
        assert recipient_200[ODASchema.VALUE].iloc[0] == 125.0  # 50 + 75

    def test_total_bilateral_plus_imputed_preserves_all_grouping_columns(self):
        """Test that all non-aggregated columns are preserved in result."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1],
                ODASchema.PROVIDER_NAME: ["Donor A", "Donor A"],
                ODASchema.RECIPIENT_CODE: [100, 100],
                ODASchema.RECIPIENT_NAME: ["Country X", "Country X"],
                "flow_type": ["ODA", "ODA"],
                "aidtype_code": [1010, 1020],
                "aid_type": ["Type1", "Type2"],
                ODASchema.VALUE: [100.0, 200.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # All grouping dimensions should be preserved
        assert ODASchema.YEAR in result.columns
        assert ODASchema.PROVIDER_CODE in result.columns
        assert ODASchema.PROVIDER_NAME in result.columns
        assert ODASchema.RECIPIENT_CODE in result.columns
        assert ODASchema.RECIPIENT_NAME in result.columns
        assert "flow_type" in result.columns

    def test_total_bilateral_plus_imputed_with_na_values(self):
        """Test handling of NA values in the data."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020],
                ODASchema.PROVIDER_CODE: [1, 1, 1],
                ODASchema.RECIPIENT_CODE: [100, 100, 100],
                "aidtype_code": [1010, 1020, 1030],
                "aid_type": ["ODA", "Other", "Mixed"],
                ODASchema.VALUE: [100.0, None, 300.0],  # One NA value
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Sum should ignore NA values (100 + 300 = 400)
        assert result[ODASchema.VALUE].iloc[0] == 400.0

    def test_total_bilateral_plus_imputed_complex_grouping(self):
        """Test complex multi-dimensional grouping."""
        df = pd.DataFrame(
            {
                ODASchema.YEAR: [2020, 2020, 2020, 2021, 2021, 2021],
                ODASchema.PROVIDER_CODE: [1, 1, 2, 1, 1, 2],
                ODASchema.PROVIDER_NAME: ["A", "A", "B", "A", "A", "B"],
                ODASchema.RECIPIENT_CODE: [100, 100, 100, 200, 200, 200],
                ODASchema.RECIPIENT_NAME: ["X", "X", "X", "Y", "Y", "Y"],
                "aidtype_code": [1010, 1020, 1010, 1010, 1020, 1010],
                "aid_type": ["T1", "T2", "T1", "T1", "T2", "T1"],
                ODASchema.VALUE: [100.0, 200.0, 50.0, 150.0, 250.0, 75.0],
            }
        )

        result = total_bilateral_plus_imputed(df)

        # Should have 4 rows (2 years × 2 unique provider-recipient combinations)
        # 2020: Provider1-Recipient100, Provider2-Recipient100
        # 2021: Provider1-Recipient200, Provider2-Recipient200
        assert len(result) == 4

        # Verify one specific combination
        p1_r100_2020 = result[
            (result[ODASchema.YEAR] == 2020)
            & (result[ODASchema.PROVIDER_CODE] == 1)
            & (result[ODASchema.RECIPIENT_CODE] == 100)
        ]
        assert p1_r100_2020[ODASchema.VALUE].iloc[0] == 300.0  # 100 + 200


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.integration
class TestDAC2AFunctionsIntegration:
    """Integration tests for DAC2A functions working together."""

    def test_total_bilateral_plus_imputed_with_realistic_data(self, sample_dac2a_df):
        """Test with realistic DAC2A data structure.

        This test uses the fixture sample_dac2a_df from conftest.py to validate
        the function with realistic data.
        """
        # Add aidtype_code and aid_type columns (required by function)
        df = sample_dac2a_df.copy()
        df["aid_type"] = "ODA"

        result = total_bilateral_plus_imputed(df)

        # Result should aggregate by year, provider, recipient
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

        # All results should have the bilateral plus imputed code
        assert (result["aidtype_code"] == 206106).all()
        assert (result["aid_type"] == "Bilateral plus imputed multilateral").all()
