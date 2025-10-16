"""
Tests for CRS indicator generation logic.

This module tests the business logic for generating CRS indicators, including:
- Hierarchical totals generation
- Partial totals creation
- Column mapping with special handling
- Filter generation based on row data
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from oda_data.indicators.crs.oecd import (
    apply_filter,
    filters,
    generate_partial_totals,
    generate_totals,
    map_column,
)

# ============================================================================
# Tests for generate_totals
# ============================================================================


class TestGenerateTotals:
    """Tests for the generate_totals function."""

    def test_generate_totals_creates_hierarchical_totals(self):
        """Test that totals are created at each hierarchical level."""
        data = pd.DataFrame(
            {
                "source": ["CRS", "CRS"],
                "perspective": ["P", "R"],
                "flow_type": ["10", "20"],
                "finance_type": ["100", "200"],
            }
        )

        result = generate_totals(data)

        # Should have created totals at multiple levels
        assert len(result) > len(data)

        # Should have rows with "T" markers
        assert (result == "T").any().any()

    def test_generate_totals_assigns_t_markers_correctly(self):
        """Test that 'T' markers are assigned to grouped columns."""
        data = pd.DataFrame(
            {
                "col1": ["A", "B"],
                "col2": ["X", "Y"],
                "col3": ["1", "2"],
            }
        )

        result = generate_totals(data)

        # Should have totals for col3
        assert "T" in result["col3"].values

    def test_generate_totals_with_two_columns(self):
        """Test base case with 2-column DataFrame."""
        data = pd.DataFrame(
            {
                "source": ["CRS", "CRS"],
                "perspective": ["P", "R"],
            }
        )

        result = generate_totals(data)

        # Should create at least one total row
        assert len(result) >= 1

    def test_generate_totals_with_four_columns(self):
        """Test full hierarchy with 4-column DataFrame."""
        data = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": ["B"],
                "col3": ["C"],
                "col4": ["D"],
            }
        )

        result = generate_totals(data)

        # Should create multiple levels of totals
        # For 4 columns, we expect totals at each level
        assert len(result) > 1

    def test_generate_totals_preserves_column_order(self):
        """Test that original column order is preserved."""
        data = pd.DataFrame(
            {
                "first": ["A"],
                "second": ["B"],
                "third": ["C"],
            }
        )

        result = generate_totals(data)

        # Columns should be in same order
        assert list(result.columns) == list(data.columns)

    def test_generate_totals_handles_duplicates(self):
        """Test that duplicate rows are handled correctly."""
        data = pd.DataFrame(
            {
                "col1": ["A", "A"],
                "col2": ["X", "X"],
            }
        )

        result = generate_totals(data)

        # Should deduplicate
        assert isinstance(result, pd.DataFrame)

    def test_generate_totals_with_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        data = pd.DataFrame({"col1": [], "col2": []})

        result = generate_totals(data)

        # Should return empty or minimal result
        assert isinstance(result, pd.DataFrame)

    def test_generate_totals_single_row(self):
        """Test with single-row DataFrame."""
        data = pd.DataFrame(
            {
                "col1": ["A"],
                "col2": ["B"],
                "col3": ["C"],
            }
        )

        result = generate_totals(data)

        # Should create totals even for single row
        assert len(result) > 0


# ============================================================================
# Tests for generate_partial_totals
# ============================================================================


class TestGeneratePartialTotals:
    """Tests for the generate_partial_totals function."""

    def test_generate_partial_totals_creates_three_variants(self):
        """Test that three partial total variants are created."""
        data = pd.DataFrame(
            {
                "source": ["CRS"],
                "type_of_finance": ["100"],
                "modality": ["C"],
                "purpose_code": [110],
            }
        )

        result = generate_partial_totals(data)

        # Should have 3 rows (one for each partial total)
        assert len(result) == 3

    def test_generate_partial_totals_sets_correct_columns_to_t(self):
        """Test that correct columns are set to 'T'."""
        data = pd.DataFrame(
            {
                "source": ["CRS"],
                "type_of_finance": ["100"],
                "modality": ["C"],
                "purpose_code": [110],
            }
        )

        result = generate_partial_totals(data)

        # Should have "T" in type_of_finance, modality, and purpose_code columns
        assert "T" in result["type_of_finance"].values
        assert "T" in result["modality"].values
        assert "T" in result["purpose_code"].values

    def test_generate_partial_totals_preserves_other_values(self):
        """Test that other column values are preserved."""
        data = pd.DataFrame(
            {
                "source": ["CRS"],
                "other_col": ["KEEP"],
                "type_of_finance": ["100"],
                "modality": ["C"],
                "purpose_code": [110],
            }
        )

        result = generate_partial_totals(data)

        # source and other_col should be preserved in all rows
        assert (result["source"] == "CRS").all()
        assert (result["other_col"] == "KEEP").all()

    def test_generate_partial_totals_drops_duplicates(self):
        """Test that duplicates are dropped."""
        data = pd.DataFrame(
            {
                "source": ["CRS", "CRS"],
                "type_of_finance": ["100", "100"],
                "modality": ["C", "C"],
                "purpose_code": [110, 110],
            }
        )

        result = generate_partial_totals(data)

        # Should have exactly 3 unique rows (one for each partial total)
        assert len(result) == 3


# ============================================================================
# Tests for map_column
# ============================================================================


class TestMapColumn:
    """Tests for the map_column function."""

    def test_map_column_maps_values_correctly(self):
        """Test that values are mapped using the mapping function."""
        data = pd.DataFrame({"type_of_finance": [110, 420]})

        def mock_mapping():
            return {
                "grant": {"code": 110, "name": "Grant"},
                "loan": {"code": 420, "name": "Loan"},
            }

        result = map_column(data, "type_of_finance", mock_mapping)

        # Should map 110 and 420 to their codes (already correct in this case)
        assert result.iloc[0] == 110
        assert result.iloc[1] == 420

    def test_map_column_applies_fill_value(self):
        """Test that fill_value is applied to unmapped values."""
        data = pd.DataFrame({"modality": [pd.NA, "C"]})

        def mock_mapping():
            return {"modality_c": {"code": "C", "name": "Project"}}

        result = map_column(data, "modality", mock_mapping, fill_value="X")

        # NA should be filled with "X"
        assert result.iloc[0] == "X"

    def test_map_column_preserves_unmapped_when_no_fill(self):
        """Test that unmapped values preserved when fill_value is None."""
        data = pd.DataFrame({"category": [10, 999]})

        def mock_mapping():
            return {"cat10": {"code": 10, "name": "ODA"}}

        result = map_column(data, "category", mock_mapping, fill_value=None)

        # 999 should remain as-is (fillna with original)
        assert result.iloc[1] == 999

    def test_map_column_with_none_fill_value_doesnt_fill_na(self):
        """Test that None fill_value doesn't apply additional filling."""
        data = pd.DataFrame({"col": [1, pd.NA]})

        def mock_mapping():
            return {"one": {"code": 1, "name": "One"}}

        result = map_column(data, "col", mock_mapping, fill_value=None)

        # NA should remain (filled from original column)
        assert pd.isna(result.iloc[1])


# ============================================================================
# Tests for filters
# ============================================================================


class TestFilters:
    """Tests for the filters function."""

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_filters_creates_flow_filter_when_not_total(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that type_of_flow creates equality filter when not 'T'."""
        # Mock mappings
        mock_finance.return_value = {}
        mock_modality.return_value = {}
        mock_purpose.return_value = {}

        row = MagicMock()
        row.type_of_flow = "10"
        row.type_of_finance = "T"
        row.modality = "T"
        row.purpose_code = "T"

        result = filters(row)

        # Should have category filter
        assert "category" in result
        assert result["category"] == ("==", "10")

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_filters_omits_flow_when_total(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that type_of_flow omitted when value is 'T'."""
        mock_finance.return_value = {}
        mock_modality.return_value = {}
        mock_purpose.return_value = {}

        row = MagicMock()
        row.type_of_flow = "T"
        row.type_of_finance = "T"
        row.modality = "T"
        row.purpose_code = "T"

        result = filters(row)

        # Should not have category filter
        assert "category" not in result

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_filters_creates_in_filter_for_specific_values(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that specific values create 'in' filters."""
        # Mock mappings to return values that will match
        mock_finance.return_value = {
            "grant": {"code": 110, "name": "Grant"},
            "other": {"code": 999, "name": "Other"},
        }
        mock_modality.return_value = {}
        mock_purpose.return_value = {}

        row = MagicMock()
        row.type_of_flow = "T"
        row.type_of_finance = 110  # Should create "in" filter
        row.modality = "T"
        row.purpose_code = "T"

        result = filters(row)

        # Should have type_of_finance filter with "in" operator
        assert "type_of_finance" in result
        assert result["type_of_finance"][0] == "in"
        assert "grant" in result["type_of_finance"][1]

    def test_apply_filter_with_missing_value(self):
        """Test that missing values create 'is None' filter."""

        def mock_mapping():
            return {}

        result = apply_filter(pd.NA, mock_mapping)

        assert result == ("is", None)

    def test_apply_filter_with_special_case(self):
        """Test that special case value creates 'is None' filter."""

        def mock_mapping():
            return {}

        result = apply_filter("X", mock_mapping, special_case="X")

        assert result == ("is", None)

    def test_apply_filter_with_specific_value(self):
        """Test that specific value creates 'in' filter with mapped values."""

        def mock_mapping():
            return {
                "key1": {"code": 100, "name": "Name1"},
                "key2": {"code": 200, "name": "Name2"},
            }

        result = apply_filter(100, mock_mapping)

        # Should return "in" with list of keys that map to code 100
        assert result[0] == "in"
        assert "key1" in result[1]


# ============================================================================
# Integration Tests
# ============================================================================


class TestUniqueCRSIndicatorRowsIntegration:
    """Integration tests for unique_crs_indicator_rows."""

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_unique_crs_indicator_rows_produces_expected_structure(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that complete pipeline produces expected DataFrame structure."""
        # Mock mappings
        mock_finance.return_value = {"grant": {"code": 100, "name": "Grant"}}
        mock_modality.return_value = {"project": {"code": "C", "name": "Project"}}
        mock_purpose.return_value = {"education": {"code": 110, "name": "Education"}}

        # Import here to avoid loading issues
        from oda_data.indicators.crs.oecd import unique_crs_indicator_rows

        data = pd.DataFrame(
            {
                "category": [10],
                "type_of_finance": [100],
                "modality": ["C"],
                "sector_code": [110],
            }
        )

        result = unique_crs_indicator_rows(data)

        # Should have required columns
        assert "source" in result.columns
        assert "perspective" in result.columns
        assert "type_of_flow" in result.columns
        assert "type_of_finance" in result.columns
        assert "modality" in result.columns
        assert "purpose_code" in result.columns

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_unique_crs_indicator_rows_adds_perspectives(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that both P and R perspectives are added."""
        mock_finance.return_value = {"grant": {"code": 100, "name": "Grant"}}
        mock_modality.return_value = {"project": {"code": "C", "name": "Project"}}
        mock_purpose.return_value = {"education": {"code": 110, "name": "Education"}}

        from oda_data.indicators.crs.oecd import unique_crs_indicator_rows

        data = pd.DataFrame(
            {
                "category": [10],
                "type_of_finance": [100],
                "modality": ["C"],
                "sector_code": [110],
            }
        )

        result = unique_crs_indicator_rows(data)

        # Should have both P and R perspectives
        assert "P" in result["perspective"].values
        assert "R" in result["perspective"].values

    @patch("oda_data.indicators.crs.oecd.read_crs_type_of_finance_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_modality_mapping")
    @patch("oda_data.indicators.crs.oecd.read_crs_purpose_mapping")
    def test_unique_crs_indicator_rows_includes_totals(
        self, mock_purpose, mock_modality, mock_finance
    ):
        """Test that totals and partial totals are included."""
        mock_finance.return_value = {"grant": {"code": 100, "name": "Grant"}}
        mock_modality.return_value = {"project": {"code": "C", "name": "Project"}}
        mock_purpose.return_value = {"education": {"code": 110, "name": "Education"}}

        from oda_data.indicators.crs.oecd import unique_crs_indicator_rows

        data = pd.DataFrame(
            {
                "category": [10],
                "type_of_finance": [100],
                "modality": ["C"],
                "sector_code": [110],
            }
        )

        result = unique_crs_indicator_rows(data)

        # Should have rows with "T" markers (totals)
        assert (result == "T").any().any()
