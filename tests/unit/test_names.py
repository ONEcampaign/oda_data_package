"""
Tests for name utilities - adding human-readable names to code columns.

This module tests the logic for mapping code columns (e.g., recipient_code,
donor_code) to their human-readable name equivalents (e.g., recipient_name,
donor_name) using predefined mappings.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.tools.names.add import add_names_columns

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_mock_mapping():
    """Sample mapping structure as returned by get_merged_names_mapping."""
    return {
        "recipient_code": {
            "100": "Country A",
            "200": "Country B",
            "300": "Country C",
        },
        "donor_code": {
            "1": "United States",
            "2": "France",
            "3": "Germany",
        },
        "sector_code": {
            "110": "Education",
            "120": "Health",
            "140": "Water",
        },
    }


# ============================================================================
# Tests for add_names_columns
# ============================================================================


class TestAddNamesColumns:
    """Tests for the add_names_columns function."""

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_single_code_column(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test adding name column for a single code column."""
        mock_get_mapping.return_value = sample_mock_mapping

        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "recipient_code": [100, 200],
                "value": [1000.0, 2000.0],
            }
        )

        result = add_names_columns(df, "recipient_code")

        # Should have added recipient_name column
        assert "recipient_name" in result.columns

        # Values should be mapped correctly
        assert result["recipient_name"].iloc[0] == "Country A"
        assert result["recipient_name"].iloc[1] == "Country B"

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_multiple_code_columns(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test adding name columns for multiple code columns."""
        mock_get_mapping.return_value = sample_mock_mapping

        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "donor_code": [1, 2],
                "recipient_code": [100, 200],
                "value": [1000.0, 2000.0],
            }
        )

        result = add_names_columns(df, ["donor_code", "recipient_code"])

        # Should have added both name columns
        assert "donor_name" in result.columns
        assert "recipient_name" in result.columns

        # Values should be mapped correctly
        assert result["donor_name"].iloc[0] == "United States"
        assert result["donor_name"].iloc[1] == "France"
        assert result["recipient_name"].iloc[0] == "Country A"
        assert result["recipient_name"].iloc[1] == "Country B"

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_with_empty_dataframe(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test that empty DataFrame is returned unchanged."""
        mock_get_mapping.return_value = sample_mock_mapping

        df = pd.DataFrame(columns=["year", "recipient_code", "value"])

        result = add_names_columns(df, "recipient_code")

        # Should return the empty DataFrame without errors
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert result is df  # Should return same object for empty df

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_raises_error_for_nonexistent_column(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test that ValueError is raised when code column doesn't exist."""
        mock_get_mapping.return_value = sample_mock_mapping

        df = pd.DataFrame(
            {
                "year": [2020],
                "value": [1000.0],
            }
        )

        with pytest.raises(ValueError, match="does not exist in the DataFrame"):
            add_names_columns(df, "recipient_code")

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_raises_error_for_unmapped_column(self, mock_get_mapping):
        """Test that ValueError is raised when no mapping exists for column."""
        # Mapping doesn't include "sector_code"
        mock_get_mapping.return_value = {
            "recipient_code": {"100": "Country A"},
        }

        df = pd.DataFrame(
            {
                "year": [2020],
                "sector_code": [110],
                "value": [1000.0],
            }
        )

        with pytest.raises(ValueError, match="No name mapping available"):
            add_names_columns(df, "sector_code")

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_inserts_next_to_code_column(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test that name column is inserted immediately after code column."""
        mock_get_mapping.return_value = sample_mock_mapping

        df = pd.DataFrame(
            {
                "year": [2020],
                "recipient_code": [100],
                "value": [1000.0],
                "other_column": ["data"],
            }
        )

        result = add_names_columns(df, "recipient_code")

        # Get column positions
        columns = result.columns.tolist()
        recipient_code_idx = columns.index("recipient_code")
        recipient_name_idx = columns.index("recipient_name")

        # Name column should be immediately after code column
        assert recipient_name_idx == recipient_code_idx + 1


# ============================================================================
# Integration Tests
# ============================================================================


class TestAddNamesColumnsIntegration:
    """Integration tests for add_names_columns function."""

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_complete_workflow(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test complete workflow with realistic data."""
        mock_get_mapping.return_value = sample_mock_mapping

        # Simulate realistic ODA data
        df = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "donor_code": [1, 2, 1, 2],
                "recipient_code": [100, 100, 200, 200],
                "sector_code": [110, 120, 110, 140],
                "value": [1000.0, 1500.0, 2000.0, 2500.0],
            }
        )

        result = add_names_columns(df, ["donor_code", "recipient_code", "sector_code"])

        # Verify all name columns added
        assert "donor_name" in result.columns
        assert "recipient_name" in result.columns
        assert "sector_name" in result.columns

        # Verify correct column order (name after code)
        columns = result.columns.tolist()
        assert columns.index("donor_name") == columns.index("donor_code") + 1
        assert columns.index("recipient_name") == columns.index("recipient_code") + 1
        assert columns.index("sector_name") == columns.index("sector_code") + 1

        # Verify all mappings work
        assert result["donor_name"].iloc[0] == "United States"
        assert result["recipient_name"].iloc[0] == "Country A"
        assert result["sector_name"].iloc[0] == "Education"

    @patch("oda_data.tools.names.add.get_merged_names_mapping")
    def test_add_names_columns_doesnt_duplicate_existing_names(
        self, mock_get_mapping, sample_mock_mapping
    ):
        """Test that existing name columns are not duplicated."""
        mock_get_mapping.return_value = sample_mock_mapping

        # DataFrame already has recipient_name
        df = pd.DataFrame(
            {
                "year": [2020],
                "recipient_code": [100],
                "recipient_name": ["Existing Name"],
                "value": [1000.0],
            }
        )

        result = add_names_columns(df, "recipient_code")

        # Should not duplicate the column
        assert result.columns.tolist().count("recipient_name") == 1

        # Original name should be preserved
        assert result["recipient_name"].iloc[0] == "Existing Name"
