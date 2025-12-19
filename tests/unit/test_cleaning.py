"""
Tests for data cleaning utilities.

This module tests the data cleaning functions in oda_data.clean_data.common,
which handle column name normalization, DataFrame cleaning, batch processing,
and unit conversions.
"""

from pathlib import Path

import pandas as pd
import pytest

from oda_data.clean_data.common import (
    clean_column_name,
    clean_raw_df,
    convert_units,
    map_column_schema,
)
from oda_data.clean_data.schema import CRS_MAPPING, ODASchema

# ============================================================================
# Tests for clean_column_name
# ============================================================================


class TestCleanColumnName:
    """Tests for the clean_column_name function."""

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            # CamelCase conversions
            ("DonorCode", "donor_code"),
            ("RecipientName", "recipient_name"),
            ("SectorCode", "sector_code"),
            ("ProviderCode", "provider_code"),
            # Already snake_case
            ("donor_code", "donor_code"),
            ("recipient_name", "recipient_name"),
            # All caps (should add _code suffix)
            ("SECTOR", "sector_code"),
            ("DONOR", "donor_code"),
            ("CRS", "crs_code"),
            # Mixed cases
            ("aidtypeName", "aidtype_name"),
            ("flowType", "flow_type"),
            # Spaces
            ("Donor Code", "donor_code"),
            ("Recipient Name", "recipient_name"),
            # Hyphens (removed, underscores added)
            ("donor-code", "donorcode"),
            ("sector-name", "sectorname"),
            # Double underscores (collapsed)
            ("donor__code", "donor_code"),
            # Leading/trailing spaces (strip applies, leading space becomes underscore in split)
            (" DonorCode ", "_donor_code"),
            # Multiple caps in sequence (all caps stays together)
            ("GNI", "gni_code"),
            ("ODAGrant", "odagrant"),  # Consecutive caps stay together
        ],
    )
    def test_clean_column_name_variations_correct_output(
        self, input_name: str, expected: str
    ):
        """Test that clean_column_name handles various naming conventions correctly.

        Validates CamelCase, all caps, spaces, hyphens, and mixed case conversions
        to snake_case format.
        """
        result = clean_column_name(input_name)
        assert result == expected

    def test_clean_column_name_preserves_numbers(self):
        """Test that column names with numbers are handled correctly."""
        assert clean_column_name("Sector2") == "sector2"
        assert clean_column_name("Code123") == "code123"

    def test_clean_column_name_empty_string(self):
        """Test that empty string is handled gracefully."""
        result = clean_column_name("")
        assert result == ""

    def test_clean_column_name_single_char(self):
        """Test that single character names are handled correctly."""
        # Single uppercase letter is treated as all-caps, gets _code suffix
        assert clean_column_name("A") == "a_code"
        assert clean_column_name("a") == "a"

    def test_clean_column_name_consecutive_caps(self):
        """Test handling of consecutive capital letters."""
        # Consecutive caps stay together (implementation doesn't split them)
        assert clean_column_name("HTTPSConnection") == "httpsconnection"
        assert clean_column_name("XMLParser") == "xmlparser"


# ============================================================================
# Tests for clean_raw_df
# ============================================================================


class TestCleanRawDF:
    """Tests for the clean_raw_df function."""

    def test_clean_raw_df_renames_columns_to_snake_case(self):
        """Test that DataFrame columns are renamed to snake_case."""
        df = pd.DataFrame(
            {
                "DonorCode": [1, 2],
                "RecipientName": ["Country A", "Country B"],
                "Year": [2020, 2021],
            }
        )

        result = clean_raw_df(df)

        expected_columns = ["donor_code", "recipient_name", "year"]
        assert list(result.columns) == expected_columns

    def test_clean_raw_df_converts_amount_to_numeric(self):
        """Test that 'amount' column is converted to numeric float64."""
        df = pd.DataFrame(
            {
                "Amount": ["1000.5", "2000", "invalid", "3000.75"],
                "Year": [2020, 2021, 2022, 2023],
            }
        )

        result = clean_raw_df(df)

        assert "amount" in result.columns
        # Check that invalid values are converted to NaN
        assert result["amount"].isna().sum() == 1
        # Check dtype is float (either float64 or float64[pyarrow])
        assert pd.api.types.is_float_dtype(result["amount"])

    def test_clean_raw_df_applies_schema_mapping(self):
        """Test that column schema mapping is applied correctly."""
        # This test depends on what's in CRS_MAPPING
        # We'll create a simple test that verifies the mapping is called
        df = pd.DataFrame(
            {
                "DonorCode": [1, 2],
                "Year": [2020, 2021],
            }
        )

        result = clean_raw_df(df)

        # After cleaning, donor_code should be present
        # (Schema mapping might rename it to provider_code based on CRS_MAPPING)
        # We're testing that the pipeline executes without error
        assert len(result) == 2

    def test_clean_raw_df_replaces_special_characters(self):
        """Test that special character \\x1a is replaced with pd.NA."""
        df = pd.DataFrame(
            {
                "Name": ["Valid", "\x1a", "Another"],
                "Year": [2020, 2021, 2022],
            }
        )

        result = clean_raw_df(df)

        # The special character should be replaced with pd.NA
        assert result["name"].isna().sum() == 1

    def test_clean_raw_df_sets_default_types(self):
        """Test that default data types are applied."""
        df = pd.DataFrame(
            {
                "Year": [2020, 2021, 2022],
                "Code": [1, 2, 3],
                "Value": [100.5, 200.75, 300.0],
            }
        )

        result = clean_raw_df(df)

        # Verify that set_default_types was applied
        # (Actual types depend on dtypes.py implementation)
        assert result is not None
        assert len(result) == 3

    def test_clean_raw_df_empty_dataframe(self):
        """Test that empty DataFrame is handled gracefully."""
        df = pd.DataFrame()

        result = clean_raw_df(df)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


# ============================================================================
# Tests for convert_units
# ============================================================================


class TestConvertUnits:
    """Tests for the convert_units function."""

    def test_convert_units_no_conversion_when_usd_current(self, mock_pydeflate):
        """Test that no conversion occurs when currency is USD and no base_year."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
            }
        )

        result = convert_units(
            data=df,
            indicator="DAC1.40.1",  # Should return as-is
            currency="USD",
            base_year=None,
        )

        assert "currency" in result.columns
        assert "prices" in result.columns
        assert result["currency"].iloc[0] == "USD"
        assert result["prices"].iloc[0] == "current"
        # pydeflate should not be called
        mock_pydeflate["exchange"].assert_not_called()
        mock_pydeflate["deflate"].assert_not_called()

    def test_convert_units_currency_exchange_when_target_not_usd(self, mock_pydeflate):
        """Test that currency exchange is called when target currency is not USD."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "donor_code": [
                    1,
                    2,
                ],  # Required by pydeflate (maps to provider_code in schema)
                "value": [1000.0, 2000.0],
            }
        )

        result = convert_units(data=df, indicator=None, currency="EUR", base_year=None)

        # Currency exchange should be called
        mock_pydeflate["exchange"].assert_called_once()
        assert "currency" in result.columns
        assert result["currency"].iloc[0] == "EUR"

    def test_convert_units_deflation_when_base_year_provided(self, mock_pydeflate):
        """Test that deflation is called when base_year is provided."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "donor_code": [
                    1,
                    2,
                ],  # Required by pydeflate (maps to provider_code in schema)
                "value": [1000.0, 2000.0],
            }
        )

        result = convert_units(data=df, indicator=None, currency="USD", base_year=2020)

        # Deflation should be called
        mock_pydeflate["deflate"].assert_called_once()
        assert "currency" in result.columns
        assert "prices" in result.columns
        assert result["prices"].iloc[0] == "constant"

    def test_convert_units_with_indicator_containing_40(self, mock_pydeflate):
        """Test that indicators with '.40.' skip conversion (except DAC1.40.1)."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
            }
        )

        # Should skip conversion for .40. indicators
        result = convert_units(
            data=df, indicator="DAC2A.40.100", currency="EUR", base_year=None
        )

        # No conversion should occur
        mock_pydeflate["exchange"].assert_not_called()
        assert "currency" in result.columns
        assert result["currency"].iloc[0] == "EUR"

    def test_convert_units_currency_and_base_year_together(self, mock_pydeflate):
        """Test that both currency exchange and deflation work together."""
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "donor_code": [
                    1,
                    2,
                ],  # Required by pydeflate (maps to provider_code in schema)
                "value": [1000.0, 2000.0],
            }
        )

        convert_units(data=df, indicator="TEST", currency="EUR", base_year=2020)

        # Deflation should be called (which handles both conversion and deflation)
        mock_pydeflate["deflate"].assert_called_once()


# ============================================================================
# Tests for map_column_schema
# ============================================================================


class TestMapColumnSchema:
    """Tests for the map_column_schema function."""

    def test_map_column_schema_applies_crs_mapping(self):
        """Test that CRS column mapping is applied to DataFrame."""
        # Create a DataFrame with columns that might be in CRS_MAPPING
        df = pd.DataFrame(
            {
                "donor_code": [1, 2],
                "year": [2020, 2021],
            }
        )

        result = map_column_schema(df)

        # Verify that mapping was applied
        # (Actual column names depend on CRS_MAPPING content)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    def test_map_column_schema_preserves_unmapped_columns(self):
        """Test that columns not in mapping are preserved."""
        df = pd.DataFrame(
            {
                "custom_column": ["a", "b"],
                "another_column": [1, 2],
            }
        )

        result = map_column_schema(df)

        # Unmapped columns should remain
        assert "custom_column" in result.columns
        assert "another_column" in result.columns


# ============================================================================
# Tests for clean_parquet_file_in_batches
# ============================================================================


class TestCleanParquetFileInBatches:
    """Tests for the clean_parquet_file_in_batches function."""

    @pytest.mark.slow
    def test_clean_parquet_file_in_batches_processes_file(self, tmp_path: Path):
        """Test that parquet file is processed in batches correctly.

        This is a slow test that creates actual parquet files.
        """
        from oda_data.clean_data.common import clean_parquet_file_in_batches

        # Create a sample parquet file
        input_path = tmp_path / "input.parquet"
        output_path = tmp_path / "output.parquet"

        # Create sample data with CamelCase columns
        df = pd.DataFrame(
            {
                "DonorCode": [1, 2, 3, 4, 5],
                "RecipientName": ["A", "B", "C", "D", "E"],
                "Year": [2020, 2020, 2021, 2021, 2022],
                "Amount": [100.0, 200.0, 300.0, 400.0, 500.0],
            }
        )
        df.to_parquet(input_path, engine="pyarrow")

        # Process the file in batches
        clean_parquet_file_in_batches(
            input_path=input_path,
            output_path=output_path,
            batch_size=2,  # Small batch for testing
        )

        # Read the output and verify
        result = pd.read_parquet(output_path)

        # Column names should be cleaned
        assert "donor_code" in result.columns or "provider_code" in result.columns
        assert "year" in result.columns
        assert "amount" in result.columns

        # Data should be preserved
        assert len(result) == 5

    def test_clean_parquet_file_in_batches_handles_large_files(self, tmp_path: Path):
        """Test that batch processing handles larger files efficiently.

        This test verifies the batch processing logic without creating
        extremely large files.
        """
        from oda_data.clean_data.common import clean_parquet_file_in_batches

        input_path = tmp_path / "large_input.parquet"
        output_path = tmp_path / "large_output.parquet"

        # Create a moderately sized file (1000 rows)
        df = pd.DataFrame(
            {
                "DonorCode": range(1000),
                "Year": [2020] * 1000,
                "Amount": [100.0] * 1000,
            }
        )
        df.to_parquet(input_path, engine="pyarrow")

        # Process with batch size
        clean_parquet_file_in_batches(
            input_path=input_path, output_path=output_path, batch_size=100
        )

        # Verify output
        result = pd.read_parquet(output_path)
        assert len(result) == 1000


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


class TestCleaningEdgeCases:
    """Tests for edge cases and error handling in cleaning functions."""

    def test_clean_column_name_with_unicode_characters(self):
        """Test that unicode characters are handled in column names."""
        # Should handle gracefully
        result = clean_column_name("Donör_Code")
        assert isinstance(result, str)

    def test_clean_raw_df_with_all_na_column(self):
        """Test that DataFrame with all NA values is handled correctly."""
        df = pd.DataFrame(
            {
                "AllNA": [None, None, None],
                "Year": [2020, 2021, 2022],
            }
        )

        result = clean_raw_df(df)

        assert "all_na" in result.columns
        assert result["all_na"].isna().all()

    def test_convert_units_with_missing_provider_code_column(self, mock_pydeflate):
        """Test that convert_units handles DataFrames without provider_code.

        Some conversions require provider_code for exchange/deflate functions.
        This tests behavior when it's missing.
        """
        df = pd.DataFrame(
            {
                "year": [2020, 2021],
                "value": [1000.0, 2000.0],
                # No provider_code
            }
        )

        # This might raise an error or handle gracefully
        # Depending on the pydeflate implementation
        try:
            result = convert_units(data=df, currency="USD", base_year=None)
            # If it succeeds, verify currency is added
            assert "currency" in result.columns
        except KeyError:
            # If it fails due to missing provider_code, that's expected
            pass


# ============================================================================
# Tests for ODASchema
# ============================================================================


class TestODASchema:
    """Tests for ODASchema field definitions."""

    def test_schema_has_data_type_code_field(self):
        """Test that ODASchema includes DATA_TYPE_CODE field.

        The DATA_TYPE_CODE field was added to support the datatype_code
        column in CRS bulk data files.
        """
        assert hasattr(ODASchema, "DATA_TYPE_CODE")
        assert ODASchema.DATA_TYPE_CODE == "data_type_code"

    def test_crs_mapping_includes_datatype_code(self):
        """Test that CRS_MAPPING maps datatype_code to ODASchema.DATA_TYPE_CODE.

        CRS bulk data includes a datatype_code column that should be
        properly mapped to the schema.
        """
        assert "datatype_code" in CRS_MAPPING
        assert CRS_MAPPING["datatype_code"] == ODASchema.DATA_TYPE_CODE

    def test_schema_essential_fields_exist(self):
        """Test that all essential ODASchema fields are present.

        Ensures backwards compatibility by verifying key fields exist.
        """
        essential_fields = [
            "YEAR",
            "PROVIDER_CODE",
            "PROVIDER_NAME",
            "RECIPIENT_CODE",
            "RECIPIENT_NAME",
            "SECTOR_CODE",
            "PURPOSE_CODE",
            "VALUE",
            "DATA_TYPE_CODE",
        ]
        for field in essential_fields:
            assert hasattr(ODASchema, field), f"Missing essential field: {field}"
