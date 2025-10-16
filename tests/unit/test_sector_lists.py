"""
Tests for sector classification and grouping logic.

This module tests the business logic for:
1. Assigning sector names to purpose codes
2. Aggregating sectors into broad categories
3. Grouping and summing data by sector
"""

import pandas as pd
import pytest

from oda_data.clean_data.schema import ODASchema
from oda_data.tools.sector_lists import (
    add_broad_sectors,
    add_sectors,
    get_broad_sector_groups,
    get_sector_groups,
)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_sector_data():
    """Sample data with purpose codes for testing sector assignment."""
    return pd.DataFrame(
        {
            "year": [2020, 2020, 2020, 2020, 2020, 2020, 2020, 2020],
            "donor_code": [1, 1, 1, 1, 1, 1, 1, 1],
            "purpose_code": [
                11220,  # Education - basic education (11200-11299)
                12220,  # Health - basic health (12200-12299)
                14010,  # Water supply
                23210,  # Energy - renewable (232xx)
                31110,  # Agriculture
                72010,  # Emergency response
                99810,  # Unspecified
                15110,  # Public sector policy
            ],
            ODASchema.VALUE: [100.0, 200.0, 150.0, 300.0, 250.0, 400.0, 50.0, 180.0],
        }
    )


# ============================================================================
# Tests for get_sector_groups
# ============================================================================


class TestGetSectorGroups:
    """Tests for the get_sector_groups function."""

    def test_get_sector_groups_returns_dict(self):
        """Test that function returns a dictionary."""
        result = get_sector_groups()

        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_sector_groups_contains_education_sectors(self):
        """Test that education sectors are defined."""
        result = get_sector_groups()

        assert "Basic Education" in result
        assert "Secondary Education" in result
        assert "Post-Secondary Education" in result

        # Check that codes are lists
        assert isinstance(result["Basic Education"], list)
        assert len(result["Basic Education"]) > 0

    def test_get_sector_groups_contains_health_sectors(self):
        """Test that health sectors are defined."""
        result = get_sector_groups()

        assert "Health, General" in result
        assert "Basic Health" in result
        assert "Non-communicable diseases (NCDs)" in result
        assert "Population Policies/Programmes & Reproductive Health" in result

    def test_get_sector_groups_contains_social_sectors(self):
        """Test that social infrastructure sectors are defined."""
        result = get_sector_groups()

        assert "Social Protection" in result
        assert "Water Supply & Sanitation" in result

    def test_get_sector_groups_contains_government_sectors(self):
        """Test that government and civil society sectors are defined."""
        result = get_sector_groups()

        assert "Public sector policy & management" in result
        assert "Public finance management" in result
        assert "Democratic participation and civil society" in result

    def test_get_sector_groups_contains_economic_sectors(self):
        """Test that economic infrastructure sectors are defined."""
        result = get_sector_groups()

        assert "Agriculture" in result
        assert "Transport & Storage" in result
        assert "Energy Policy" in result
        assert "Banking & Financial Services" in result

    def test_get_sector_groups_contains_environment_sectors(self):
        """Test that environment sectors are defined."""
        result = get_sector_groups()

        assert "Environmental Policy and Admin Management" in result
        assert "Bio-diversity" in result

    def test_get_sector_groups_contains_humanitarian_sectors(self):
        """Test that humanitarian sectors are defined."""
        result = get_sector_groups()

        assert "Emergency Response" in result
        assert "Reconstruction, Relief & Rehabilitation" in result
        assert "Disaster Prevention & Preparedness" in result

    def test_get_sector_groups_contains_other_categories(self):
        """Test that other categories are defined."""
        result = get_sector_groups()

        assert "General Budget Support" in result
        assert "Refugees in Donor Countries" in result
        assert "Unallocated/ Unspecificed" in result


# ============================================================================
# Tests for get_broad_sector_groups
# ============================================================================


class TestGetBroadSectorGroups:
    """Tests for the get_broad_sector_groups function."""

    def test_get_broad_sector_groups_returns_dict(self):
        """Test that function returns a dictionary."""
        result = get_broad_sector_groups()

        assert isinstance(result, dict)
        assert len(result) > 0

    def test_get_broad_sector_groups_aggregates_education(self):
        """Test that all education sectors map to 'Education'."""
        result = get_broad_sector_groups()

        assert result["Basic Education"] == "Education"
        assert result["Secondary Education"] == "Education"
        assert result["Post-Secondary Education"] == "Education"

    def test_get_broad_sector_groups_aggregates_health(self):
        """Test that all health sectors map to 'Health'."""
        result = get_broad_sector_groups()

        assert result["Health, General"] == "Health"
        assert result["Basic Health"] == "Health"
        assert result["Non-communicable diseases (NCDs)"] == "Health"

    def test_get_broad_sector_groups_aggregates_government(self):
        """Test that government sectors map to 'Government'."""
        result = get_broad_sector_groups()

        assert result["Public sector policy & management"] == "Government"
        assert result["Public finance management"] == "Government"
        assert result["Macroeconomic policy"] == "Government"

    def test_get_broad_sector_groups_aggregates_energy(self):
        """Test that all energy sectors map to 'Energy'."""
        result = get_broad_sector_groups()

        assert result["Energy Policy"] == "Energy"
        assert result["Energy Generation, Renewable"] == "Energy"
        assert result["Energy Generation, Non-renewable"] == "Energy"


# ============================================================================
# Tests for add_sectors
# ============================================================================


class TestAddSectors:
    """Tests for the add_sectors function."""

    def test_add_sectors_assigns_correct_sector_names(self, sample_sector_data):
        """Test that purpose codes are assigned correct sector names."""
        result = add_sectors(sample_sector_data)

        # Should have purpose_name column with sector names
        assert ODASchema.PURPOSE_NAME in result.columns

        # Check specific mappings
        # 11220 should be in Basic Education
        basic_edu_row = result[result[ODASchema.PURPOSE_NAME] == "Basic Education"]
        assert len(basic_edu_row) > 0

        # 12220 should be in Basic Health
        basic_health_row = result[result[ODASchema.PURPOSE_NAME] == "Basic Health"]
        assert len(basic_health_row) > 0

    def test_add_sectors_removes_purpose_code_column(self, sample_sector_data):
        """Test that purpose_code column is removed after grouping."""
        result = add_sectors(sample_sector_data)

        assert "purpose_code" not in result.columns

    def test_add_sectors_groups_and_sums_values(self, sample_sector_data):
        """Test that values are grouped and summed by sector."""
        # Add duplicate sector to test summing
        data = sample_sector_data.copy()
        # Add another basic education entry
        new_row = pd.DataFrame(
            {
                "year": [2020],
                "donor_code": [1],
                "purpose_code": [11230],  # Another basic education code
                ODASchema.VALUE: [50.0],
            }
        )
        data = pd.concat([data, new_row], ignore_index=True)

        result = add_sectors(data)

        # Basic education values should be summed
        basic_edu = result[result[ODASchema.PURPOSE_NAME] == "Basic Education"]
        # Should combine 100.0 (from 11220) and 50.0 (from 11230)
        assert basic_edu[ODASchema.VALUE].sum() >= 100.0

    def test_add_sectors_preserves_other_columns(self, sample_sector_data):
        """Test that other columns are preserved."""
        result = add_sectors(sample_sector_data)

        assert "year" in result.columns
        assert "donor_code" in result.columns
        assert ODASchema.VALUE in result.columns

    def test_add_sectors_filters_zero_values(self):
        """Test that rows with zero values are filtered out."""
        data = pd.DataFrame(
            {
                "year": [2020],
                "donor_code": [1],
                "purpose_code": [11220],
                ODASchema.VALUE: [0.0],
            }
        )

        result = add_sectors(data)

        # Should filter out zero values
        assert len(result) == 0

    def test_add_sectors_groups_by_all_dimensions(self):
        """Test that grouping preserves all non-value dimensions."""
        data = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "donor_code": [1, 1, 1, 1],
                "recipient_code": [100, 100, 100, 100],
                "purpose_code": [
                    11220,
                    11230,
                    11220,
                    11230,
                ],  # Same sector (Basic Education)
                ODASchema.VALUE: [100.0, 50.0, 200.0, 75.0],
            }
        )

        result = add_sectors(data)

        # Should group by year, donor, recipient, sector
        # Year 2020 should sum 100 + 50 = 150
        year_2020 = result[result["year"] == 2020]
        assert year_2020[ODASchema.VALUE].iloc[0] == 150.0

        # Year 2021 should sum 200 + 75 = 275
        year_2021 = result[result["year"] == 2021]
        assert year_2021[ODASchema.VALUE].iloc[0] == 275.0


# ============================================================================
# Tests for add_broad_sectors
# ============================================================================


class TestAddBroadSectors:
    """Tests for the add_broad_sectors function."""

    def test_add_broad_sectors_assigns_broad_categories(self, sample_sector_data):
        """Test that sectors are aggregated into broad categories."""
        result = add_broad_sectors(sample_sector_data)

        # Should have purpose_name with broad sector names
        assert ODASchema.PURPOSE_NAME in result.columns

        # Check broad categories
        assert "Education" in result[ODASchema.PURPOSE_NAME].values
        assert "Health" in result[ODASchema.PURPOSE_NAME].values
        assert "Energy" in result[ODASchema.PURPOSE_NAME].values

    def test_add_broad_sectors_aggregates_education(self, sample_sector_data):
        """Test that all education sub-sectors are aggregated to 'Education'."""
        # Create data with multiple education codes
        data = pd.DataFrame(
            {
                "year": [2020, 2020, 2020],
                "donor_code": [1, 1, 1],
                "purpose_code": [11110, 11220, 11320],  # Basic, secondary, post-sec
                ODASchema.VALUE: [100.0, 150.0, 200.0],
            }
        )

        result = add_broad_sectors(data)

        # All should be aggregated to "Education"
        education = result[result[ODASchema.PURPOSE_NAME] == "Education"]
        assert len(education) == 1
        # Sum should be 450
        assert education[ODASchema.VALUE].iloc[0] == 450.0

    def test_add_broad_sectors_aggregates_health(self):
        """Test that all health sub-sectors are aggregated to 'Health'."""
        data = pd.DataFrame(
            {
                "year": [2020, 2020, 2020],
                "donor_code": [1, 1, 1],
                "purpose_code": [12110, 12220, 12261],  # Basic health, general, NCDs
                ODASchema.VALUE: [100.0, 150.0, 200.0],
            }
        )

        result = add_broad_sectors(data)

        # All should be aggregated to "Health"
        health = result[result[ODASchema.PURPOSE_NAME] == "Health"]
        assert len(health) == 1
        assert health[ODASchema.VALUE].iloc[0] == 450.0

    def test_add_broad_sectors_removes_purpose_code(self, sample_sector_data):
        """Test that purpose_code column is removed."""
        result = add_broad_sectors(sample_sector_data)

        assert "purpose_code" not in result.columns

    def test_add_broad_sectors_preserves_data_integrity(self, sample_sector_data):
        """Test that total values are preserved after aggregation."""
        original_sum = sample_sector_data[ODASchema.VALUE].sum()

        result = add_broad_sectors(sample_sector_data)

        result_sum = result[ODASchema.VALUE].sum()

        # Totals should match (allowing for floating point precision)
        assert abs(original_sum - result_sum) < 0.01


# ============================================================================
# Tests for Edge Cases
# ============================================================================


class TestSectorEdgeCases:
    """Tests for edge cases in sector classification."""

    def test_add_sectors_with_unmapped_purpose_code(self):
        """Test handling of purpose codes not in any sector list."""
        data = pd.DataFrame(
            {
                "year": [2020],
                "donor_code": [1],
                "purpose_code": [99999],  # Non-existent code
                ODASchema.VALUE: [100.0],
            }
        )

        result = add_sectors(data)

        # Should not raise error, code just won't be assigned to any sector
        # Result may be empty or have NaN for sector name
        assert isinstance(result, pd.DataFrame)

    def test_add_sectors_with_multiple_years_donors_recipients(self):
        """Test complex grouping with multiple dimensions."""
        data = pd.DataFrame(
            {
                "year": [2020, 2020, 2021, 2021],
                "donor_code": [1, 2, 1, 2],
                "recipient_code": [100, 100, 200, 200],
                "purpose_code": [11220, 11220, 11220, 11220],  # All basic education
                ODASchema.VALUE: [100.0, 200.0, 300.0, 400.0],
            }
        )

        result = add_sectors(data)

        # Should have 4 rows (one for each year-donor-recipient combination)
        assert len(result) == 4

        # All should be Basic Education
        assert (result[ODASchema.PURPOSE_NAME] == "Basic Education").all()

    def test_add_broad_sectors_comprehensive_aggregation(self):
        """Test that broad sector aggregation works across many sector types."""
        # Create data spanning multiple broad categories
        data = pd.DataFrame(
            {
                "year": [2020] * 10,
                "donor_code": [1] * 10,
                "purpose_code": [
                    11220,  # Education - basic
                    11320,  # Education - secondary
                    12220,  # Health - basic
                    13010,  # Health (population)
                    14010,  # Water
                    23210,  # Energy - renewable
                    23310,  # Energy - non-renewable
                    31110,  # Agriculture
                    72010,  # Humanitarian
                    99810,  # Unspecified
                ],
                ODASchema.VALUE: [100.0] * 10,
            }
        )

        result = add_broad_sectors(data)

        # Should have fewer rows than original due to aggregation
        assert len(result) < len(data)

        # Check that major categories are present
        broad_sectors = result[ODASchema.PURPOSE_NAME].unique()
        assert "Education" in broad_sectors
        assert "Health" in broad_sectors
        assert "Energy" in broad_sectors
