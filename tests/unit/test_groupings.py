"""
Tests for provider and recipient grouping functions.

This module tests the grouping functions in oda_data.tools.groupings that load
and process provider and recipient grouping definitions from JSON files.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from oda_data.tools.groupings import (
    _read_grouping,
    provider_groupings,
    recipient_groupings,
)

# ============================================================================
# Tests for _read_grouping (internal helper)
# ============================================================================


class TestReadGrouping:
    """Tests for the _read_grouping helper function."""

    def test_read_grouping_loads_simple_dict(self, tmp_path: Path):
        """Test loading a simple dictionary from JSON."""
        # Create test JSON file
        test_file = tmp_path / "test_grouping.json"
        data = {
            "group1": {"1": "Country A", "2": "Country B"},
            "group2": {"3": "Country C", "4": "Country D"},
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        result = _read_grouping(test_file)

        # Should convert string keys to integers
        assert result["group1"] == {1: "Country A", 2: "Country B"}
        assert result["group2"] == {3: "Country C", 4: "Country D"}

    def test_read_grouping_expands_list_references(self, tmp_path: Path):
        """Test that list values referencing other groups are expanded."""
        # Create test JSON with list references
        test_file = tmp_path / "test_grouping.json"
        data = {
            "base1": {"1": "A", "2": "B"},
            "base2": {"3": "C", "4": "D"},
            "combined": ["base1", "base2"],  # Reference to other groups
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        result = _read_grouping(test_file)

        # "combined" should be expanded to include all items from base1 and base2
        assert result["combined"] == {1: "A", 2: "B", 3: "C", 4: "D"}

    def test_read_grouping_handles_nested_list_references(self, tmp_path: Path):
        """Test that nested list references are properly expanded."""
        test_file = tmp_path / "test_grouping.json"
        data = {
            "group_a": {"1": "Item 1"},
            "group_b": {"2": "Item 2"},
            "level1": ["group_a", "group_b"],
            "level2": ["level1"],  # References level1, which itself is a list
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        result = _read_grouping(test_file)

        # level2 should expand through level1 to include items from group_a and group_b
        assert result["level2"] == {1: "Item 1", 2: "Item 2"}

    def test_read_grouping_converts_numeric_string_keys_to_int(self, tmp_path: Path):
        """Test that numeric string keys are converted to integers."""
        test_file = tmp_path / "test_grouping.json"
        data = {
            "codes": {
                "1": "One",
                "2": "Two",
                "10": "Ten",
                "100": "Hundred",
            }
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        result = _read_grouping(test_file)

        # All keys should be integers
        assert isinstance(next(iter(result["codes"].keys())), int)
        assert result["codes"][1] == "One"
        assert result["codes"][100] == "Hundred"

    def test_read_grouping_preserves_non_numeric_dict_keys(self, tmp_path: Path):
        """Test that non-numeric keys in dicts cause ValueError."""
        test_file = tmp_path / "test_grouping.json"
        data = {
            "mixed": {"abc": "Value A", "xyz": "Value B"},
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        # Non-numeric keys should cause ValueError when code tries int() conversion
        with pytest.raises(ValueError, match="invalid literal for int"):
            _read_grouping(test_file)

    def test_read_grouping_handles_empty_file(self, tmp_path: Path):
        """Test that empty JSON file is handled correctly."""
        test_file = tmp_path / "empty.json"
        with open(test_file, "w") as f:
            json.dump({}, f)

        result = _read_grouping(test_file)

        assert result == {}


# ============================================================================
# Tests for provider_groupings
# ============================================================================


class TestProviderGroupings:
    """Tests for the provider_groupings function."""

    @patch("oda_data.tools.groupings.config.ODAPaths")
    @patch("oda_data.tools.groupings._read_grouping")
    def test_provider_groupings_calls_read_grouping(self, mock_read, mock_paths):
        """Test that provider_groupings calls _read_grouping with correct path."""
        mock_paths.settings = Path("/test/settings")
        mock_read.return_value = {"test": {1: "Test"}}

        provider_groupings()

        # Should call _read_grouping with provider_groupings.json path
        expected_path = Path("/test/settings") / "provider_groupings.json"
        mock_read.assert_called_once_with(expected_path)

    @patch("oda_data.tools.groupings.config.ODAPaths")
    @patch("oda_data.tools.groupings._read_grouping")
    def test_provider_groupings_returns_dict(self, mock_read, mock_paths):
        """Test that provider_groupings returns a dictionary."""
        mock_paths.settings = Path("/test/settings")
        mock_read.return_value = {
            "dac": {1: "USA", 2: "UK"},
            "multilateral": {901: "World Bank", 918: "EU Institutions"},
        }

        result = provider_groupings()

        assert isinstance(result, dict)
        assert "dac" in result
        assert "multilateral" in result

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_provider_groupings_with_real_structure(self, mock_paths, tmp_path: Path):
        """Test with realistic provider grouping structure."""
        # Create a realistic provider groupings file
        mock_paths.settings = tmp_path

        provider_data = {
            "dac_countries": {"1": "United States", "2": "United Kingdom"},
            "multilateral": {"901": "World Bank", "918": "EU Institutions"},
            "eu27_countries": {"4": "France", "5": "Germany"},
            "all_providers": ["dac_countries", "multilateral"],
        }

        test_file = tmp_path / "provider_groupings.json"
        with open(test_file, "w") as f:
            json.dump(provider_data, f)

        result = provider_groupings()

        # Check structure
        assert "dac_countries" in result
        assert "multilateral" in result
        assert "all_providers" in result

        # Check that list reference was expanded
        expected_all = {
            1: "United States",
            2: "United Kingdom",
            901: "World Bank",
            918: "EU Institutions",
        }
        assert result["all_providers"] == expected_all


# ============================================================================
# Tests for recipient_groupings
# ============================================================================


class TestRecipientGroupings:
    """Tests for the recipient_groupings function."""

    @patch("oda_data.tools.groupings.config.ODAPaths")
    @patch("oda_data.tools.groupings._read_grouping")
    def test_recipient_groupings_calls_read_grouping(self, mock_read, mock_paths):
        """Test that recipient_groupings calls _read_grouping with correct path."""
        mock_paths.settings = Path("/test/settings")
        mock_read.return_value = {"test": {100: "Test"}}

        recipient_groupings()

        # Should call _read_grouping with recipient_groupings.json path
        expected_path = Path("/test/settings") / "recipient_groupings.json"
        mock_read.assert_called_once_with(expected_path)

    @patch("oda_data.tools.groupings.config.ODAPaths")
    @patch("oda_data.tools.groupings._read_grouping")
    def test_recipient_groupings_returns_dict(self, mock_read, mock_paths):
        """Test that recipient_groupings returns a dictionary."""
        mock_paths.settings = Path("/test/settings")
        mock_read.return_value = {
            "ldcs": {100: "Afghanistan", 200: "Bangladesh"},
            "africa": {300: "Kenya", 400: "Nigeria"},
        }

        result = recipient_groupings()

        assert isinstance(result, dict)
        assert "ldcs" in result
        assert "africa" in result

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_recipient_groupings_with_real_structure(self, mock_paths, tmp_path: Path):
        """Test with realistic recipient grouping structure."""
        # Create a realistic recipient groupings file
        mock_paths.settings = tmp_path

        recipient_data = {
            "ldcs": {"100": "Afghanistan", "200": "Bangladesh"},
            "africa": {"300": "Kenya", "400": "Nigeria"},
            "asia": {"500": "India", "600": "Pakistan"},
            "developing_countries": ["ldcs", "africa", "asia"],
        }

        test_file = tmp_path / "recipient_groupings.json"
        with open(test_file, "w") as f:
            json.dump(recipient_data, f)

        result = recipient_groupings()

        # Check structure
        assert "ldcs" in result
        assert "africa" in result
        assert "developing_countries" in result

        # Check that list reference was expanded
        expected_all = {
            100: "Afghanistan",
            200: "Bangladesh",
            300: "Kenya",
            400: "Nigeria",
            500: "India",
            600: "Pakistan",
        }
        assert result["developing_countries"] == expected_all


# ============================================================================
# Integration Tests
# ============================================================================


@pytest.mark.integration
class TestGroupingsIntegration:
    """Integration tests for groupings functions working together."""

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_provider_and_recipient_groupings_can_be_loaded_together(
        self, mock_paths, tmp_path: Path
    ):
        """Test that both provider and recipient groupings can be loaded."""
        mock_paths.settings = tmp_path

        # Create both files
        provider_data = {"dac": {"1": "USA"}}
        recipient_data = {"ldcs": {"100": "Afghanistan"}}

        with open(tmp_path / "provider_groupings.json", "w") as f:
            json.dump(provider_data, f)

        with open(tmp_path / "recipient_groupings.json", "w") as f:
            json.dump(recipient_data, f)

        # Load both
        providers = provider_groupings()
        recipients = recipient_groupings()

        # Both should be successfully loaded
        assert "dac" in providers
        assert "ldcs" in recipients

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_groupings_with_complex_nested_references(self, mock_paths, tmp_path: Path):
        """Test complex nested group references."""
        mock_paths.settings = tmp_path

        # Create complex nested structure
        data = {
            "base_a": {"1": "Item 1", "2": "Item 2"},
            "base_b": {"3": "Item 3", "4": "Item 4"},
            "base_c": {"5": "Item 5"},
            "level_1": ["base_a", "base_b"],
            "level_2": ["base_c"],
            "all": ["level_1", "level_2"],
        }

        with open(tmp_path / "provider_groupings.json", "w") as f:
            json.dump(data, f)

        result = provider_groupings()

        # "all" should include everything from base_a, base_b, and base_c
        expected = {1: "Item 1", 2: "Item 2", 3: "Item 3", 4: "Item 4", 5: "Item 5"}
        assert result["all"] == expected


# ============================================================================
# Error Handling Tests
# ============================================================================


class TestGroupingsErrorHandling:
    """Tests for error handling in groupings functions."""

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_read_grouping_with_nonexistent_file_raises_error(self, mock_paths):
        """Test that reading a non-existent file raises FileNotFoundError."""
        mock_paths.settings = Path("/nonexistent")
        nonexistent_file = Path("/nonexistent/missing.json")

        with pytest.raises(FileNotFoundError):
            _read_grouping(nonexistent_file)

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_read_grouping_with_invalid_json_raises_error(
        self, mock_paths, tmp_path: Path
    ):
        """Test that invalid JSON raises JSONDecodeError."""
        test_file = tmp_path / "invalid.json"
        with open(test_file, "w") as f:
            f.write("{invalid json content")

        with pytest.raises(json.JSONDecodeError):
            _read_grouping(test_file)

    @patch("oda_data.tools.groupings.config.ODAPaths")
    def test_read_grouping_with_circular_reference(self, mock_paths, tmp_path: Path):
        """Test handling of circular references in list expansions."""
        test_file = tmp_path / "circular.json"
        data = {
            "group_a": ["group_b"],
            "group_b": ["group_a"],  # Circular reference
        }
        with open(test_file, "w") as f:
            json.dump(data, f)

        # Circular references should cause AttributeError (list has no .items())
        with pytest.raises(AttributeError, match="'list' object has no attribute"):
            _read_grouping(test_file)
