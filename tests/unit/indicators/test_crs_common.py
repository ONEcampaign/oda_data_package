"""
Tests for CRS common functions - data grouping logic.

This module tests the business logic for grouping CRS project-level data
by different dimensions (flow type, finance type, modality, purpose) based
on indicator codes.
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.crs.common import crs_value_cols, group_data_based_on_indicator


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_crs_data():
    """Sample CRS data for testing grouping logic."""
    return pd.DataFrame({
        "year": [2020, 2020, 2020, 2021, 2021],
        "donor_code": [1, 1, 2, 1, 2],
        "de_donorcode": [1, 1, 2, 1, 2],
        "donor_name": ["USA", "USA", "France", "USA", "France"],
        "recipient_code": [100, 100, 200, 100, 200],
        "de_recipientcode": [100, 100, 200, 100, 200],
        "recipient_name": ["Country A", "Country A", "Country B", "Country A", "Country B"],
        "recipient_region_code": [1, 1, 2, 1, 2],
        "recipient_region": ["Region 1", "Region 1", "Region 2", "Region 1", "Region 2"],
        "recipient_income_code": [1, 1, 2, 1, 2],
        "incomegroup_name": ["LDC", "LDC", "LMIC", "LDC", "LMIC"],
        "category": [10, 10, 20, 10, 20],  # Type of flow
        "type_of_finance": [110, 110, 420, 110, 420],  # Type of finance
        "modality": ["C", "C", "B", "C", "B"],  # Modality
        "purpose_code": [110, 120, 110, 120, 110],  # Purpose
        "usd_commitment": [100.0, 150.0, 200.0, 300.0, 400.0],
        "usd_disbursement": [80.0, 120.0, 160.0, 240.0, 320.0],
    })


# ============================================================================
# Tests for crs_value_cols
# ============================================================================


class TestCRSValueCols:
    """Tests for the crs_value_cols function."""

    def test_crs_value_cols_returns_dict(self):
        """Test that crs_value_cols returns a dictionary."""
        result = crs_value_cols()

        assert isinstance(result, dict)

    def test_crs_value_cols_contains_expected_measures(self):
        """Test that common CRS measures are included."""
        result = crs_value_cols()

        # Should include key measure types
        assert "commitment" in result
        assert "gross_disbursement" in result

    def test_crs_value_cols_values_are_column_names(self):
        """Test that values are actual column names (strings)."""
        result = crs_value_cols()

        # All values should be strings representing column names
        assert all(isinstance(v, str) for v in result.values())


# ============================================================================
# Tests for group_data_based_on_indicator
# ============================================================================


class TestGroupDataBasedOnIndicator:
    """Tests for the group_data_based_on_indicator function."""

    def test_group_data_single_dimension_flow_type(self, sample_crs_data):
        """Test grouping by flow type only (CRS.X format)."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment", "gross_disbursement"]
        )

        # Should have measure column
        assert ODASchema.MEASURE in result.columns

        # Should melt the data
        assert "value" in result.columns

        # Should preserve base dimensions
        assert "year" in result.columns
        assert "donor_code" in result.columns

    def test_group_data_two_dimensions_finance_type(self, sample_crs_data):
        """Test grouping by flow type and finance type (CRS.X.Y format)."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW.FINANCE",
            measures=["commitment"]
        )

        # Should have measure and value columns
        assert ODASchema.MEASURE in result.columns
        assert "value" in result.columns

        # Should preserve base dimensions
        assert "year" in result.columns
        assert "donor_code" in result.columns

    def test_group_data_three_dimensions_modality(self, sample_crs_data):
        """Test grouping by flow, finance, and modality (CRS.X.Y.Z format)."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW.FINANCE.MODALITY",
            measures=["commitment"]
        )

        # Should have measure and value columns
        assert ODASchema.MEASURE in result.columns
        assert "value" in result.columns

        # Should preserve base dimensions
        assert "year" in result.columns
        assert "donor_code" in result.columns

    def test_group_data_four_dimensions_purpose(self, sample_crs_data):
        """Test grouping by all four dimensions (CRS.X.Y.Z.W format)."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW.FINANCE.MODALITY.PURPOSE",
            measures=["commitment"]
        )

        # Should have measure and value columns
        assert ODASchema.MEASURE in result.columns
        assert "value" in result.columns

        # Should preserve base dimensions
        assert "year" in result.columns
        assert "donor_code" in result.columns

    def test_group_data_sums_values_correctly(self, sample_crs_data):
        """Test that values are summed correctly within groups."""
        # Create simple data where we know the expected sum
        simple_data = pd.DataFrame({
            "year": [2020, 2020],
            "donor_code": [1, 1],
            "de_donorcode": [1, 1],
            "donor_name": ["USA", "USA"],
            "recipient_code": [100, 100],
            "de_recipientcode": [100, 100],
            "recipient_name": ["Country A", "Country A"],
            "recipient_region_code": [1, 1],
            "recipient_region": ["Region 1", "Region 1"],
            "recipient_income_code": [1, 1],
            "incomegroup_name": ["LDC", "LDC"],
            "category": [10, 10],
            "usd_commitment": [100.0, 200.0],
            "usd_disbursement": [50.0, 75.0],
        })

        result = group_data_based_on_indicator(
            simple_data,
            indicator_code="CRS.FLOW",
            measures=["commitment"]
        )

        # Filter to commitment measure
        commitment_result = result[result[ODASchema.MEASURE] == "commitment"]

        # Should sum to 300 (100 + 200)
        assert commitment_result["value"].iloc[0] == 300.0

    def test_group_data_preserves_base_dimensions(self, sample_crs_data):
        """Test that base dimensions (year, donor, recipient) are preserved."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment"]
        )

        # Should preserve key dimensions
        assert "year" in result.columns
        assert "donor_code" in result.columns
        assert "donor_name" in result.columns
        assert "recipient_code" in result.columns
        assert "recipient_name" in result.columns

    def test_group_data_handles_multiple_measures(self, sample_crs_data):
        """Test that multiple measures are melted correctly."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment", "gross_disbursement"]
        )

        # Should have both measures in the measure column
        assert "commitment" in result[ODASchema.MEASURE].values
        assert "gross_disbursement" in result[ODASchema.MEASURE].values

        # Should have separate rows for each measure
        commitment_rows = result[result[ODASchema.MEASURE] == "commitment"]
        disbursement_rows = result[result[ODASchema.MEASURE] == "gross_disbursement"]

        assert len(commitment_rows) > 0
        assert len(disbursement_rows) > 0

    def test_group_data_handles_missing_grouping_columns(self):
        """Test that grouping works when some grouping columns are missing."""
        data = pd.DataFrame({
            "year": [2020, 2020],
            "donor_code": [1, 1],
            "de_donorcode": [1, 1],
            "donor_name": ["USA", "USA"],
            "recipient_code": [100, 100],
            "de_recipientcode": [100, 100],
            "recipient_name": ["Country A", "Country A"],
            "recipient_region_code": [1, 1],
            "recipient_region": ["Region 1", "Region 1"],
            "recipient_income_code": [1, 1],
            "incomegroup_name": ["LDC", "LDC"],
            # Missing: category, type_of_finance, modality, purpose_code
            "usd_commitment": [100.0, 200.0],
        })

        # Should still work, just won't group by missing columns
        result = group_data_based_on_indicator(
            data,
            indicator_code="CRS.FLOW.FINANCE",
            measures=["commitment"]
        )

        # Should return a result without errors
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_group_data_groups_separately_by_year(self, sample_crs_data):
        """Test that data is grouped separately for each year."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment"]
        )

        # Should have separate entries for each year
        years = result["year"].unique()
        assert 2020 in years
        assert 2021 in years

    def test_group_data_groups_separately_by_donor(self, sample_crs_data):
        """Test that data is grouped separately for each donor."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment"]
        )

        # Should have separate entries for each donor
        donors = result["donor_code"].unique()
        assert 1 in donors
        assert 2 in donors

    def test_group_data_maps_measure_names_correctly(self, sample_crs_data):
        """Test that measure column names are mapped back to measure names."""
        result = group_data_based_on_indicator(
            sample_crs_data,
            indicator_code="CRS.FLOW",
            measures=["commitment"]
        )

        # Measure column should contain the original measure name, not column name
        assert "commitment" in result[ODASchema.MEASURE].values
        assert "commitment_current" not in result[ODASchema.MEASURE].values
