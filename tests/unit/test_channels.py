"""
Tests for channel classification and mapping logic.

This module tests the complex multi-step channel mapping logic that:
1. Cleans channel names
2. Attempts direct matching to official CRS channel codes
3. Falls back to fuzzy matching with multiple dictionaries
4. Falls back to regex pattern matching
5. Handles unmapped channels gracefully
"""

from unittest.mock import patch

import pandas as pd
import pytest

from oda_data.clean_data.channels import (
    ADDITIONAL_PATTERNS,
    _apply_fuzzy_match,
    _clean_names_for_regex,
    _direct_match_name,
    _fuzzy_match_name,
    _generate_regex_full_lookahead,
    _generate_regex_full_word,
    _generate_regex_partial_lookahead,
    _regex_map_names_to_codes,
    _regex_match_channel_name_to_code,
    add_channel_codes,
    add_channel_names,
    add_multi_channel_codes,
    channel_to_code,
    clean_string,
    generate_channel_mapping_dictionary,
    match_names_direct_and_fuzzy,
    raw_data_to_unique_channels,
)
from oda_data.clean_data.schema import ODASchema

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_channel_mapping():
    """Sample CRS official channel mapping data."""
    return pd.DataFrame(
        {
            "channel_code": [44000, 41301, 21039, 47078],
            "channel_name": [
                "World Bank",
                "Food and Agriculture Organization",
                "IISD IIDD Institut",
                "Montreal Protocol",
            ],
            "en_acronym": ["WB", "FAO", "IISD", "MP"],
            "fr_acronym": ["BM", "FAO", "IISD", "PM"],
        }
    )


@pytest.fixture
def sample_raw_data():
    """Sample raw data with channel names."""
    return pd.DataFrame(
        {
            "channel_name": [
                "World Bank",
                "WORLD BANK",
                "world bank - ida",
                "Food & Agriculture Organization",
                "FAO",
                "Unknown Channel",
                "EU Institutions European Commission",
            ],
            "year": [2020, 2020, 2021, 2021, 2022, 2022, 2023],
            "value": [100, 200, 300, 400, 500, 600, 700],
        }
    )


# ============================================================================
# Tests for String Cleaning
# ============================================================================


class TestCleanString:
    """Tests for the clean_string function."""

    def test_clean_string_converts_to_lowercase(self):
        """Test that strings are converted to lowercase."""
        result = clean_string("WORLD BANK")
        assert result == "world bank"

    def test_clean_string_removes_punctuation(self):
        """Test that punctuation is replaced with spaces."""
        result = clean_string("Food & Agriculture Organization!")
        assert result == "food agriculture organization"

    def test_clean_string_normalizes_whitespace(self):
        """Test that multiple spaces are collapsed to single space."""
        result = clean_string("World    Bank   Group")
        assert result == "world bank group"

    def test_clean_string_strips_leading_trailing_spaces(self):
        """Test that leading and trailing spaces are removed."""
        result = clean_string("  World Bank  ")
        assert result == "world bank"

    def test_clean_string_with_series(self):
        """Test that function works with pandas Series."""
        series = pd.Series(["WORLD BANK", "Food & Ag"])
        result = clean_string(series)
        assert isinstance(result, pd.Series)
        assert result.iloc[0] == "world bank"
        assert result.iloc[1] == "food ag"

    def test_clean_string_with_single_string(self):
        """Test that function works with single string and returns string."""
        result = clean_string("WORLD BANK")
        assert isinstance(result, str)
        assert result == "world bank"


# ============================================================================
# Tests for Direct Matching
# ============================================================================


class TestChannelToCode:
    """Tests for the channel_to_code function."""

    @patch("oda_data.clean_data.channels.get_crs_official_mapping")
    def test_channel_to_code_default_channel_name(self, mock_get_mapping):
        """Test mapping by channel_name (default)."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000, 41301],
                "channel_name": ["World Bank", "Food and Agriculture Organization"],
                "en_acronym": ["WB", "FAO"],
                "fr_acronym": ["BM", "FAO"],
            }
        )

        result = channel_to_code(map_to="channel_name")

        assert isinstance(result, dict)
        assert "world bank" in result
        assert result["world bank"] == 44000

    @patch("oda_data.clean_data.channels.get_crs_official_mapping")
    def test_channel_to_code_by_en_acronym(self, mock_get_mapping):
        """Test mapping by English acronym."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000, 41301],
                "channel_name": ["World Bank", "Food and Agriculture Organization"],
                "en_acronym": ["WB", "FAO"],
                "fr_acronym": ["BM", "FAO"],
            }
        )

        result = channel_to_code(map_to="en_acronym")

        assert "WB" in result
        assert result["WB"] == 44000
        assert "FAO" in result
        assert result["FAO"] == 41301

    @patch("oda_data.clean_data.channels.get_crs_official_mapping")
    def test_channel_to_code_by_fr_acronym(self, mock_get_mapping):
        """Test mapping by French acronym."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000, 41301],
                "channel_name": ["World Bank", "Food and Agriculture Organization"],
                "en_acronym": ["WB", "FAO"],
                "fr_acronym": ["BM", "FAO"],
            }
        )

        result = channel_to_code(map_to="fr_acronym")

        assert "BM" in result
        assert result["BM"] == 44000

    def test_channel_to_code_invalid_map_to_raises_error(self):
        """Test that invalid map_to parameter raises ValueError."""
        with pytest.raises(ValueError, match="map_to must be one of"):
            channel_to_code(map_to="invalid")


class TestDirectMatchName:
    """Tests for the _direct_match_name function."""

    def test_direct_match_name_maps_correctly(self):
        """Test that direct matching assigns correct channel codes."""
        df = pd.DataFrame({"clean_channel": ["world bank", "fao", "unknown"]})
        channels_dict = {
            "world bank": 44000,
            "fao": 41301,
        }

        result = _direct_match_name(df, channels_dict)

        assert result["channel_code"].iloc[0] == 44000
        assert result["channel_code"].iloc[1] == 41301
        assert pd.isna(result["channel_code"].iloc[2])

    def test_direct_match_name_creates_int32_column(self):
        """Test that channel_code column is Int32 dtype."""
        df = pd.DataFrame({"clean_channel": ["world bank"]})
        channels_dict = {"world bank": 44000}

        result = _direct_match_name(df, channels_dict)

        assert result["channel_code"].dtype == "Int32"


# ============================================================================
# Tests for Fuzzy Matching
# ============================================================================


class TestFuzzyMatchName:
    """Tests for the _fuzzy_match_name function."""

    def test_fuzzy_match_name_exact_match(self):
        """Test that exact matches score 100 and are accepted."""
        channels_dict = {"world bank": 44000}

        result = _fuzzy_match_name("world bank", channels_dict, tolerance=90)

        assert result == 44000

    def test_fuzzy_match_name_close_match(self):
        """Test that close matches above tolerance are accepted."""
        channels_dict = {"world bank": 44000}

        result = _fuzzy_match_name("world bank group", channels_dict, tolerance=80)

        assert result == 44000

    def test_fuzzy_match_name_below_tolerance(self):
        """Test that matches below tolerance return NA."""
        channels_dict = {"world bank": 44000}

        result = _fuzzy_match_name("completely different", channels_dict, tolerance=90)

        assert pd.isna(result)

    def test_fuzzy_match_name_with_typo(self):
        """Test that small typos are handled with appropriate tolerance."""
        channels_dict = {"food and agriculture organization": 41301}

        result = _fuzzy_match_name(
            "food and agriclture organization", channels_dict, tolerance=90
        )

        # Should match despite typo
        assert result == 41301


class TestApplyFuzzyMatch:
    """Tests for the _apply_fuzzy_match function."""

    def test_apply_fuzzy_match_fills_missing_codes(self):
        """Test that fuzzy matching fills in missing channel codes."""
        df = pd.DataFrame(
            {
                "clean_channel": ["world bank group", "fao"],
                "channel_code": [pd.NA, pd.NA],
            }
        )

        mapping_dicts = [
            ({"world bank": 44000, "food and agriculture organization": 41301}, 80),
        ]

        result = _apply_fuzzy_match(df, mapping_dicts)

        assert result["channel_code"].iloc[0] == 44000
        # "fao" might not match "food and agriculture organization" at 80% tolerance

    def test_apply_fuzzy_match_preserves_existing_codes(self):
        """Test that existing codes are not overwritten."""
        df = pd.DataFrame(
            {"clean_channel": ["world bank", "fao"], "channel_code": [44000, pd.NA]}
        )

        mapping_dicts = [
            ({"world bank": 99999, "fao": 41301}, 90),  # Different code for world bank
        ]

        result = _apply_fuzzy_match(df, mapping_dicts)

        # Original code should be preserved
        assert result["channel_code"].iloc[0] == 44000
        # Missing code should be filled
        assert result["channel_code"].iloc[1] == 41301


# ============================================================================
# Tests for Regex Pattern Generation
# ============================================================================


class TestCleanNamesForRegex:
    """Tests for the _clean_names_for_regex function."""

    def test_clean_names_for_regex_removes_non_alphabetic(self):
        """Test that numbers and special characters are removed."""
        result = _clean_names_for_regex("World Bank 2023!")
        assert result == "world bank"

    def test_clean_names_for_regex_removes_common_words(self):
        """Test that common words like 'the', 'of', 'and' are removed when surrounded by spaces."""
        result = _clean_names_for_regex(
            "Food and Agriculture Organization of the United Nations"
        )
        # Common words surrounded by spaces should be removed
        assert " and " not in result
        assert " of " not in result
        assert " the " not in result
        # Important words should remain
        assert "food" in result
        assert "agriculture" in result
        assert "organization" in result

    def test_clean_names_for_regex_with_na(self):
        """Test that NA values return empty string."""
        result = _clean_names_for_regex(pd.NA)
        assert result == ""


class TestRegexGeneration:
    """Tests for regex pattern generation functions."""

    def test_generate_regex_partial_lookahead_requires_half(self):
        """Test that partial lookahead requires at least half of words."""
        words = ["world", "bank", "group", "international"]
        regex = _generate_regex_partial_lookahead(words)

        # Should require at least 3 out of 4 words (4//2 + 1 = 3)
        assert "(?=.*world)" in regex
        assert "(?=.*bank)" in regex
        assert "(?=.*group)" in regex
        # Should not require "international"
        assert "(?=.*international)" not in regex

    def test_generate_regex_full_lookahead_requires_all(self):
        """Test that full lookahead requires all words."""
        words = ["world", "bank"]
        regex = _generate_regex_full_lookahead(words)

        assert "(?=.*world)" in regex
        assert "(?=.*bank)" in regex
        assert regex.endswith(".*")

    def test_generate_regex_full_word_uses_first_word(self):
        """Test that full word regex uses word boundaries for first word."""
        words = ["world", "bank"]
        regex = _generate_regex_full_word(words)

        assert regex == r"\bworld\b"

    def test_generate_regex_full_word_with_empty_list(self):
        """Test that empty word list returns no-match pattern."""
        words = []
        regex = _generate_regex_full_word(words)

        assert regex == "no_match__"


class TestRegexMapNamesToCodes:
    """Tests for the _regex_map_names_to_codes function."""

    def test_regex_map_names_to_codes_partial_lookahead(self):
        """Test regex generation with partial lookahead."""
        df = pd.DataFrame(
            {"mapped_name": ["world bank group"], "channel_code": [44000]}
        )

        result = _regex_map_names_to_codes(
            df, names_column="mapped_name", regex_type="partial_look_ahead"
        )

        assert isinstance(result, dict)
        # Should create a regex pattern
        assert len(result) > 0

    def test_regex_map_names_to_codes_invalid_type_raises_error(self):
        """Test that invalid regex_type raises ValueError."""
        df = pd.DataFrame({"mapped_name": ["world bank"], "channel_code": [44000]})

        with pytest.raises(ValueError, match="regex_type must be one of"):
            _regex_map_names_to_codes(
                df, names_column="mapped_name", regex_type="invalid"
            )


# ============================================================================
# Tests for Regex Matching
# ============================================================================


class TestRegexMatchChannelNameToCode:
    """Tests for the _regex_match_channel_name_to_code function."""

    def test_regex_match_with_word_boundary(self):
        """Test matching with word boundary patterns."""
        regex_dict = {r"\bworld bank\b": 44000}

        result = _regex_match_channel_name_to_code("world bank", regex_dict)
        assert result == 44000

        result = _regex_match_channel_name_to_code("world bank group", regex_dict)
        assert result == 44000

    def test_regex_match_with_lookahead(self):
        """Test matching with lookahead patterns."""
        regex_dict = {"(?=.*world)(?=.*bank).*": 44000}

        result = _regex_match_channel_name_to_code("world bank", regex_dict)
        assert result == 44000

        result = _regex_match_channel_name_to_code("bank of the world", regex_dict)
        assert result == 44000

    def test_regex_match_no_match_returns_na(self):
        """Test that no match returns NA."""
        regex_dict = {r"\bworld bank\b": 44000}

        result = _regex_match_channel_name_to_code("completely different", regex_dict)
        assert pd.isna(result)

    def test_regex_match_uses_first_matching_pattern(self):
        """Test that first matching pattern is used (dict order matters)."""
        # Patterns are tried in order
        regex_dict = {
            r"\bworld\b": 11111,
            r"\bworld bank\b": 44000,
        }

        result = _regex_match_channel_name_to_code("world bank", regex_dict)
        # Should match first pattern
        assert result == 11111


class TestAdditionalPatterns:
    """Tests for manually defined ADDITIONAL_PATTERNS."""

    def test_additional_patterns_contains_key_organizations(self):
        """Test that additional patterns include key organizations."""
        assert r"\bworld bank\b" in ADDITIONAL_PATTERNS
        assert ADDITIONAL_PATTERNS[r"\bworld bank\b"] == 44000

        assert r"\beuropean development fund\b" in ADDITIONAL_PATTERNS
        assert ADDITIONAL_PATTERNS[r"\beuropean development fund\b"] == 42003


# ============================================================================
# Tests for Full Mapping Pipeline
# ============================================================================


class TestRawDataToUniqueChannels:
    """Tests for the raw_data_to_unique_channels function."""

    def test_raw_data_to_unique_channels_extracts_unique(self, sample_raw_data):
        """Test that unique channel names are extracted."""
        result = raw_data_to_unique_channels(sample_raw_data, "channel_name")

        # Should have clean_channel column
        assert "clean_channel" in result.columns
        # Should have unique entries
        assert len(result) == sample_raw_data["channel_name"].nunique()

    def test_raw_data_to_unique_channels_cleans_names(self, sample_raw_data):
        """Test that channel names are cleaned."""
        result = raw_data_to_unique_channels(sample_raw_data, "channel_name")

        # All clean names should be lowercase
        assert result["clean_channel"].str.islower().all()


@patch("oda_data.clean_data.channels.get_crs_official_mapping")
class TestMatchNamesDirectAndFuzzy:
    """Tests for the match_names_direct_and_fuzzy function."""

    def test_match_names_direct_and_fuzzy_adds_channel_code(self, mock_get_mapping):
        """Test that channel codes are added."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000],
                "channel_name": ["World Bank"],
                "en_acronym": ["WB"],
                "fr_acronym": ["BM"],
            }
        )

        channels = pd.DataFrame(
            {"channel_name": ["World Bank"], "clean_channel": ["world bank"]}
        )

        result = match_names_direct_and_fuzzy(channels)

        assert "channel_code" in result.columns
        assert "mapped_name" in result.columns

    def test_match_names_direct_and_fuzzy_uses_fuzzy_fallback(self, mock_get_mapping):
        """Test that fuzzy matching is used when direct match fails."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000],
                "channel_name": ["World Bank"],
                "en_acronym": ["WB"],
                "fr_acronym": ["BM"],
            }
        )

        channels = pd.DataFrame(
            {
                "channel_name": ["World Bank"],  # Exact match for testing
                "clean_channel": ["world bank"],
            }
        )

        result = match_names_direct_and_fuzzy(channels)

        # Should get a code via direct match
        assert result["channel_code"].notna().any()
        assert result["channel_code"].iloc[0] == 44000


class TestAddChannelNames:
    """Tests for the add_channel_names function."""

    @patch("oda_data.clean_data.channels.channel_to_code")
    def test_add_channel_names_maps_codes_to_names(self, mock_channel_to_code):
        """Test that channel codes are mapped to names."""
        mock_channel_to_code.return_value = {
            "world bank": 44000,
            "fao": 41301,
        }

        df = pd.DataFrame({"channel_code": [44000, 41301]})

        result = add_channel_names(
            df, codes_column="channel_code", target_column="channel_name"
        )

        assert "channel_name" in result.columns
        assert result["channel_name"].iloc[0] == "world bank"
        assert result["channel_name"].iloc[1] == "fao"


@patch("oda_data.clean_data.channels.get_crs_official_mapping")
class TestGenerateChannelMappingDictionary:
    """Tests for the generate_channel_mapping_dictionary function."""

    def test_generate_channel_mapping_dictionary_returns_dict(
        self, mock_get_mapping, sample_raw_data
    ):
        """Test that a dictionary is returned."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000, 41301],
                "channel_name": ["World Bank", "Food and Agriculture Organization"],
                "en_acronym": ["WB", "FAO"],
                "fr_acronym": ["BM", "FAO"],
            }
        )

        result = generate_channel_mapping_dictionary(sample_raw_data, "channel_name")

        assert isinstance(result, dict)
        assert len(result) > 0

    def test_generate_channel_mapping_dictionary_handles_unmapped(
        self, mock_get_mapping, sample_raw_data
    ):
        """Test that unmapped channels are excluded from dictionary."""
        mock_get_mapping.return_value = pd.DataFrame(
            {
                "channel_code": [44000],
                "channel_name": ["World Bank"],
                "en_acronym": ["WB"],
                "fr_acronym": ["BM"],
            }
        )

        result = generate_channel_mapping_dictionary(sample_raw_data, "channel_name")

        # Should only include channels that were successfully mapped
        assert all(isinstance(v, int | float) for v in result.values())


@patch("oda_data.clean_data.channels.generate_channel_mapping_dictionary")
class TestAddChannelCodes:
    """Tests for the add_channel_codes function."""

    def test_add_channel_codes_adds_code_column(self, mock_generate_mapping):
        """Test that channel_code column is added."""
        mock_generate_mapping.return_value = {
            "World Bank": 44000,
            "FAO": 41301,
        }

        df = pd.DataFrame({ODASchema.CHANNEL_NAME: ["World Bank", "FAO"]})

        result = add_channel_codes(df)

        assert ODASchema.CHANNEL_CODE in result.columns
        assert result[ODASchema.CHANNEL_CODE].iloc[0] == 44000
        assert result[ODASchema.CHANNEL_CODE].iloc[1] == 41301

    def test_add_channel_codes_handles_eu_institutions_special_case(
        self, mock_generate_mapping
    ):
        """Test that EU Institutions are mapped to code 42001."""
        mock_generate_mapping.return_value = {}

        df = pd.DataFrame(
            {ODASchema.CHANNEL_NAME: ["EU Institutions European Commission"]}
        )

        result = add_channel_codes(df)

        # EU Institutions should be explicitly mapped to 42001
        assert result[ODASchema.CHANNEL_CODE].iloc[0] == 42001


@patch("oda_data.clean_data.channels.add_channel_codes")
class TestAddMultiChannelCodes:
    """Tests for the add_multi_channel_codes function."""

    def test_add_multi_channel_codes_combines_provider_agency(self, mock_add_codes):
        """Test that provider and agency names are combined."""
        mock_add_codes.side_effect = lambda data, **kwargs: data.assign(
            **{ODASchema.CHANNEL_CODE: 44000}
        )

        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_NAME: ["United States"],
                ODASchema.AGENCY_NAME: ["USAID"],
            }
        )

        result = add_multi_channel_codes(df)

        # Should create combined name column
        assert "name" in result.columns
        assert "United States USAID" in result["name"].values

    def test_add_multi_channel_codes_handles_same_provider_agency(self, mock_add_codes):
        """Test that identical provider and agency names are not duplicated."""
        mock_add_codes.side_effect = lambda data, **kwargs: data.assign(
            **{ODASchema.CHANNEL_CODE: 44000}
        )

        df = pd.DataFrame(
            {
                ODASchema.PROVIDER_NAME: ["World Bank"],
                ODASchema.AGENCY_NAME: ["World Bank"],
            }
        )

        result = add_multi_channel_codes(df)

        # Should not duplicate the name
        assert result["name"].iloc[0] == "World Bank"
